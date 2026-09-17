#!/usr/bin/env bash
# I/O model measurement: thread-per-connection versus an event loop.
#
#   cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release -DKVSTORE_SANITIZE=OFF
#   cmake --build build-release -j
#   ./bench/run_io.sh
#
# THE QUESTION. Both models serve the same protocol from the same store. They
# differ in what an IDLE CONNECTION COSTS, and that is the only thing this script
# varies: the number of active clients is held fixed while the number of open
# connections grows. That is what a real server looks like — thousands connected,
# a handful talking at any instant.
#
# THE THREE ROWS PER CONNECTION COUNT:
#
#   threads (small pool)  the default model: 8 worker threads. One connection
#                         occupies one worker until it disconnects, so beyond 8
#                         connections the rest sit in the queue UNSERVED. This row
#                         is expected to collapse, and watching it collapse is the
#                         point — it is the C10k problem, reproduced.
#
#   threads (one each)    threads = connections, up to the server's own 1024 cap.
#                         Now every connection has a worker, so it works — and the
#                         cost shows up as MEMORY and context switches instead.
#                         Above 1024 this row cannot run at all, which is itself
#                         the answer.
#
#   loop                  ONE thread, epoll (or poll with POLLER=poll). Idle
#                         connections cost a descriptor and a few kilobytes.
#
# RSS is reported per row because it is half the story: an event loop's advantage
# at 5,000 connections is as much about memory as about CPU.
#
# Linux is where this means anything: epoll does not exist on macOS, so a Mac run
# compares thread-per-connection against poll() only. In Docker:
#
#   docker compose run --rm dev
#   cmake -S . -B build-linux-release -DCMAKE_BUILD_TYPE=Release -DKVSTORE_SANITIZE=OFF
#   cmake --build build-linux-release -j
#   SERVER=./build-linux-release/kvstore BENCH=./build-linux-release/benchmark ./bench/run_io.sh

set -euo pipefail

SERVER="${SERVER:-./build-release/kvstore}"
BENCH="${BENCH:-./build-release/benchmark}"
PORT="${PORT:-6395}"
DURATION="${DURATION:-4}"
ACTIVE="${ACTIVE:-8}"            # load-generating threads; held FIXED across every row
VALUE_SIZE="${VALUE_SIZE:-64}"
READ_RATIO="${READ_RATIO:-0.9}"  # a cache's real mix; writes are not what the I/O model changes
KEYS="${KEYS:-4096}"
POLLER="${POLLER:-auto}"
SMALL_POOL="${SMALL_POOL:-8}"    # the "thread pool sized like a normal server" row
CONN_COUNTS="${CONN_COUNTS:-8 100 1000 5000}"
MAX_THREADS=1024                 # the server's own sanity ceiling on --threads

for binary in "$SERVER" "$BENCH"; do
    if [ ! -x "$binary" ]; then
        echo "missing $binary — build the release configuration first (see the header)" >&2
        exit 1
    fi
done

io_check="$("$SERVER" --io bogus-io-check 2>&1 || true)"
if ! printf '%s' "$io_check" | grep -q "invalid --io"; then
    echo "$SERVER does not understand --io: it is out of date. Rebuild it." >&2
    exit 1
fi

# One connection per client plus a healthy margin. Without this, 5,000 sockets in
# the load generator hits the default 1024-descriptor limit and the run silently
# measures 1024 connections while claiming 5000.
NEED_FDS=$((  $(echo $CONN_COUNTS | tr ' ' '\n' | sort -n | tail -1) * 2 + 256 ))
CURRENT_FDS=$(ulimit -n)
if [ "$CURRENT_FDS" != "unlimited" ] && [ "$CURRENT_FDS" -lt "$NEED_FDS" ]; then
    if ! ulimit -n "$NEED_FDS" 2>/dev/null; then
        echo "NOTE: file-descriptor limit is $CURRENT_FDS and this run wants $NEED_FDS." >&2
        echo "      Rows above roughly $((CURRENT_FDS / 2 - 128)) connections will be short." >&2
        echo "      Raise it with: ulimit -n $NEED_FDS" >&2
    fi
fi

SERVER_PID=""
cleanup() {
    if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

wait_for_port() {
    for _ in $(seq 1 100); do
        if (exec 3<>/dev/tcp/127.0.0.1/"$PORT") 2>/dev/null; then
            exec 3<&- 3>&- 2>/dev/null || true
            return 0
        fi
        sleep 0.1
    done
    return 1
}

# Resident set size of the server, in kilobytes. This is the number that makes the
# memory half of the argument concrete: a thread costs a stack, a connection in an
# event loop costs two buffers.
server_rss_kb() {
    if [ -r "/proc/$SERVER_PID/status" ]; then
        awk '/VmRSS/ {print $2}' "/proc/$SERVER_PID/status"
    else
        ps -o rss= -p "$SERVER_PID" 2>/dev/null | tr -d ' ' || echo "?"
    fi
}

# Runs one row: start a server with the given flags, drive `conns` connections at it,
# print one line of results.
run_row() {
    local label="$1"; shift
    local conns="$1"; shift
    # remaining arguments are the server's flags

    "$SERVER" --port "$PORT" "$@" > /tmp/kvstore_io_server.log 2>&1 &
    SERVER_PID=$!
    if ! wait_for_port; then
        printf "  %-22s %8s  %10s %10s %10s %9s\n" "$label" "$conns" "did not start" "" "" ""
        sed 's/^/      /' /tmp/kvstore_io_server.log >&2 | head -3
        cleanup; SERVER_PID=""
        return
    fi

    local out
    out=$("$BENCH" --transport tcp --port "$PORT" --clients "$ACTIVE" --conns "$conns" \
                   --duration "$DURATION" --read-ratio "$READ_RATIO" --keys "$KEYS" \
                   --value-size "$VALUE_SIZE" 2>/dev/null)

    local ops p50 p99 rss
    ops=$(printf '%s' "$out" | awk '/throughput/ {print $3}')
    p50=$(printf '%s' "$out" | awk '/latency p50/ {print $4}')
    p99=$(printf '%s' "$out" | awk '/latency p99/ {print $4}')
    rss=$(server_rss_kb)

    # Nanoseconds to microseconds, so the columns stay readable across four orders
    # of magnitude of latency.
    p50=$(awk -v v="${p50:-0}" 'BEGIN {printf "%.0f", v/1000}')
    p99=$(awk -v v="${p99:-0}" 'BEGIN {printf "%.0f", v/1000}')

    printf "  %-22s %8s  %10s %9s %9s %9s\n" "$label" "$conns" "${ops:-0}" "${p50}us" "${p99}us" "${rss}KB"

    cleanup
    SERVER_PID=""
}

echo "=============================================================================="
echo " What an idle connection costs"
echo "=============================================================================="
echo " active clients=$ACTIVE (fixed)   duration=${DURATION}s   value=${VALUE_SIZE}B   read-ratio=$READ_RATIO"
echo " poller=$POLLER   fd limit=$(ulimit -n)"
echo
printf "  %-22s %8s  %10s %9s %9s %9s\n" "model" "conns" "ops/sec" "p50" "p99" "RSS"
printf "  %-22s %8s  %10s %9s %9s %9s\n" "----------------------" "--------" "----------" "---------" "---------" "---------"

for conns in $CONN_COUNTS; do
    # Row 1: a normally-sized thread pool. Collapses as soon as connections exceed it.
    run_row "threads (pool=$SMALL_POOL)" "$conns" --threads "$SMALL_POOL"

    # Row 2: a thread for every connection — the only way thread-per-connection can
    # serve them all, and only possible up to the server's --threads ceiling.
    if [ "$conns" -le "$MAX_THREADS" ]; then
        run_row "threads (pool=$conns)" "$conns" --threads "$conns"
    else
        printf "  %-22s %8s  %10s\n" "threads (pool=$conns)" "$conns" \
               "REFUSED: --threads caps at $MAX_THREADS"
    fi

    # Row 3: one thread, one poller.
    run_row "loop ($POLLER)" "$conns" --io loop --poller "$POLLER"
    echo
done

echo "=============================================================================="
echo " How to read this"
echo "------------------------------------------------------------------------------"
echo " Row 1 is the honest failure. With a pool of $SMALL_POOL, the first $SMALL_POOL"
echo " connections occupy every worker and hold them — a connection owns its thread"
echo " until it disconnects, not until it finishes a request. Everything after that"
echo " waits in the queue, so throughput does not degrade gracefully, it stops. That"
echo " is thread-per-connection's real limit: not slowness, a CEILING equal to the"
echo " thread count."
echo
echo " Row 2 buys its way out by giving every connection a thread, and pays in memory"
echo " and context switches — watch the RSS column. At 5,000 it cannot pay at all:"
echo " the server refuses more than $MAX_THREADS threads, and it is right to."
echo
echo " Row 3 holds every connection on ONE thread. Its throughput is bounded by that"
echo " one core, so on a busy multi-core box row 2 can WIN on ops/sec while losing on"
echo " memory and scale. Both facts are real, and which one matters depends on whether"
echo " your connections are mostly busy or mostly idle."
echo
echo " Run it twice, POLLER=poll and POLLER=epoll, to separate the I/O MODEL from the"
echo " READINESS MECHANISM. poll() rescans every registered descriptor on every call,"
echo " so its cost grows with total connections; epoll returns only the ready ones."
echo " The gap between those two runs at 5,000 connections is the C10k problem,"
echo " measured on your own machine."
echo "=============================================================================="
