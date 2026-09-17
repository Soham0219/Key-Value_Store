#!/usr/bin/env bash
# Replication measurement: what replication costs a client.
#
#   cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release -DKVSTORE_SANITIZE=OFF
#   cmake --build build-release -j
#   ./bench/run_repl.sh
#
# This one uses REAL PROCESSES over REAL SOCKETS — two servers and a TCP client —
# because the question it answers is "what does a client experience", and a client
# experiences a network. bench/benchmark.cpp is reused as the load generator with
# --transport tcp, so the numbers are directly comparable to the other scripts.
#
# THE THREE ROWS, and what each one isolates:
#
#   standalone        no replication at all. The durability-only baseline.
#   async, 1 follower a follower is connected and receiving. The leader answers the
#                     client as soon as the record is in its own log, so the WRITE
#                     PATH gains almost nothing: one atomic push and one byte down a
#                     pipe. On one machine this row is still slower than row 1, and
#                     honestly so — the follower is a second process competing for
#                     the same cores and the same disk, writing every record again.
#   sync, 1 follower  the leader waits for the follower's acknowledgement before
#                     answering. The client now pays a network round trip plus the
#                     follower's own log write on EVERY write. This is the number
#                     this script exists to produce.
#
# Read the p99 column, not just the mean. Sync mode's cost is a round trip, and
# round trips have a tail: the mean tells you the typical cost and the p99 tells
# you what your slowest users see.
#
# RUN THIS ON LINUX, for the same reason as run_wal.sh and run_txn.sh: the fsync
# behaviour underneath matters, and on macOS fsync() returns before the data is
# durable. In Docker:
#
#   docker compose run --rm dev
#   cmake -S . -B build-linux-release -DCMAKE_BUILD_TYPE=Release -DKVSTORE_SANITIZE=OFF
#   cmake --build build-linux-release -j
#   SERVER=./build-linux-release/kvstore BENCH=./build-linux-release/benchmark \
#       LOG_DIR=/data ./bench/run_repl.sh

set -euo pipefail

SERVER="${SERVER:-./build-release/kvstore}"
BENCH="${BENCH:-./build-release/benchmark}"
LOG_DIR="${LOG_DIR:-/tmp}"
DURATION="${DURATION:-5}"
CLIENTS="${CLIENTS:-4}"
KEYS="${KEYS:-1024}"
VALUE_SIZE="${VALUE_SIZE:-256}"
# Pure writes. Reads are never replicated and never wait for anything, so mixing
# them in would dilute the very cost this script is trying to show.
READ_RATIO="${READ_RATIO:-0.0}"
FSYNC="${FSYNC:-interval}"
FSYNC_INTERVAL_MS="${FSYNC_INTERVAL_MS:-100}"

LEADER_PORT="${LEADER_PORT:-6390}"
REPL_PORT="${REPL_PORT:-6391}"
FOLLOWER_PORT="${FOLLOWER_PORT:-6392}"

for binary in "$SERVER" "$BENCH"; do
    if [ ! -x "$binary" ]; then
        echo "missing $binary — build the release configuration first (see the header)" >&2
        exit 1
    fi
done

# The binary existing is not the same as the binary being CURRENT. A server built
# without replication support has no --role flag, so it prints a usage error and exits,
# only symptom would be "never came up". Ask it directly instead.
#
# Captured into a variable first, NOT piped straight into grep. `set -o pipefail` makes a
# pipeline fail if ANY stage fails, and this server deliberately exits non-zero on a bad
# flag — so `server | grep` would report failure even when grep found the line. That is a
# genuinely nasty interaction and it cost one confusing run to find.
role_check="$("$SERVER" --role bogus-role-check 2>&1 || true)"
if ! printf '%s' "$role_check" | grep -q "invalid --role"; then
    echo "$SERVER does not understand --role: it is out of date. Rebuild it:" >&2
    echo "  cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release -DKVSTORE_SANITIZE=OFF" >&2
    echo "  cmake --build build-release -j" >&2
    exit 1
fi

LEADER_LOG="$LOG_DIR/kvstore_repl_leader.log"
FOLLOWER_LOG="$LOG_DIR/kvstore_repl_follower.log"

# Where each server's own stderr goes. NOT /dev/null: a server that refuses to start
# prints exactly why, and throwing that away turns a one-line diagnosis ("invalid
# --role value", "address already in use", "requires --log") into a mystery.
LEADER_OUT="$LOG_DIR/kvstore_repl_leader.out"
FOLLOWER_OUT="$LOG_DIR/kvstore_repl_follower.out"

LEADER_PID=""
FOLLOWER_PID=""

# A trap, not a tidy-up at the end: this script can be interrupted with Ctrl-C in
# the middle of a run, and a leaked server would hold its port and make every
# subsequent run fail with "address already in use".
cleanup() {
    for pid in "$FOLLOWER_PID" "$LEADER_PID"; do
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
}
trap cleanup EXIT INT TERM

fresh_logs() {
    rm -f "$LEADER_LOG" "$FOLLOWER_LOG" "$FOLLOWER_LOG.replstate"
}

# Waits for a port to accept a connection. A fixed sleep would either be too short
# on a loaded machine (the benchmark reports zero ops and the row is a lie) or too
# long everywhere else.
#
# On failure it prints the server's OWN output, which is where the actual reason is.
wait_for_port() {
    local port="$1"
    local logfile="${2:-}"
    for _ in $(seq 1 100); do
        if (exec 3<>/dev/tcp/127.0.0.1/"$port") 2>/dev/null; then
            exec 3<&- 3>&- 2>/dev/null || true
            return 0
        fi
        sleep 0.1
    done
    echo "" >&2
    echo "server on port $port never came up. Its own output was:" >&2
    if [ -n "$logfile" ] && [ -s "$logfile" ]; then
        sed 's/^/    /' "$logfile" >&2
    else
        echo "    (nothing) -- the binary may be out of date. Rebuild it:" >&2
        echo "    cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release -DKVSTORE_SANITIZE=OFF" >&2
        echo "    cmake --build build-release -j" >&2
    fi
    return 1
}

# Asks a running server for its STATS line over the client protocol.
stats_of() {
    local port="$1"
    printf 'STATS\nQUIT\n' | timeout 5 nc 127.0.0.1 "$port" 2>/dev/null | head -1 || true
}

start_leader() {
    local mode="$1"   # async | sync
    "$SERVER" --port "$LEADER_PORT" --threads "$CLIENTS" \
              --log "$LEADER_LOG" --fsync "$FSYNC" --fsync-interval-ms "$FSYNC_INTERVAL_MS" \
              --role leader --repl-port "$REPL_PORT" --repl-mode "$mode" \
              > "$LEADER_OUT" 2>&1 &
    LEADER_PID=$!
    wait_for_port "$LEADER_PORT" "$LEADER_OUT"
}

start_follower() {
    "$SERVER" --port "$FOLLOWER_PORT" --threads 2 \
              --log "$FOLLOWER_LOG" --fsync "$FSYNC" --fsync-interval-ms "$FSYNC_INTERVAL_MS" \
              --role follower --peer "127.0.0.1:$REPL_PORT" \
              > "$FOLLOWER_OUT" 2>&1 &
    FOLLOWER_PID=$!
    wait_for_port "$FOLLOWER_PORT" "$FOLLOWER_OUT"
    sleep 0.5   # let the replication link come up before the load starts
}

start_standalone() {
    "$SERVER" --port "$LEADER_PORT" --threads "$CLIENTS" \
              --log "$LEADER_LOG" --fsync "$FSYNC" --fsync-interval-ms "$FSYNC_INTERVAL_MS" \
              > "$LEADER_OUT" 2>&1 &
    LEADER_PID=$!
    wait_for_port "$LEADER_PORT" "$LEADER_OUT"
}

run_load() {
    "$BENCH" --transport tcp --port "$LEADER_PORT" \
             --clients "$CLIENTS" --duration "$DURATION" \
             --read-ratio "$READ_RATIO" --keys "$KEYS" --value-size "$VALUE_SIZE"
}

echo "=============================================================="
echo " The client-visible cost of replication"
echo "=============================================================="
echo " server=$SERVER  clients=$CLIENTS  duration=${DURATION}s"
echo " value-size=$VALUE_SIZE  read-ratio=$READ_RATIO  fsync=$FSYNC/${FSYNC_INTERVAL_MS}ms"
echo

echo "--- 1. standalone (no replication) ---------------------------"
fresh_logs
start_standalone
run_load
echo "  leader STATS: $(stats_of "$LEADER_PORT")"
cleanup; LEADER_PID=""; FOLLOWER_PID=""
echo

echo "--- 2. async replication, one follower -----------------------"
fresh_logs
start_leader async
start_follower
run_load
echo "  leader   STATS: $(stats_of "$LEADER_PORT")"
echo "  follower STATS: $(stats_of "$FOLLOWER_PORT")"
cleanup; LEADER_PID=""; FOLLOWER_PID=""
echo

echo "--- 3. sync replication, one follower ------------------------"
fresh_logs
start_leader sync
start_follower
run_load
echo "  leader   STATS: $(stats_of "$LEADER_PORT")"
echo "  follower STATS: $(stats_of "$FOLLOWER_PORT")"
cleanup; LEADER_PID=""; FOLLOWER_PID=""
echo

echo "=============================================================="
echo " How to read this"
echo "--------------------------------------------------------------"
echo " Rows 1 and 2 measure what asynchronous replication costs the"
echo " client. On the WRITE PATH it should be almost nothing -- one"
echo " atomic push and one byte down a pipe. Any larger gap on this"
echo " machine is mostly the follower itself: it is a second process"
echo " on the same cores and the same disk, writing every record a"
echo " second time. On real hardware the follower is another machine"
echo " and that part of the cost disappears, which is worth saying"
echo " out loud rather than quietly claiming loopback numbers apply."
echo
echo " Row 3 is the price of surviving the loss of the whole machine:"
echo " every write now waits for a second machine to confirm it. Over"
echo " loopback that is tens of microseconds. Across a datacentre it"
echo " is a millisecond or so; across regions, tens of milliseconds —"
echo " and THAT is why cross-region synchronous replication is rare."
echo
echo " Ignore the benchmark's own \"write-ahead log: off\" header in"
echo " these rows. With --transport tcp the load generator is a pure"
echo " client and never builds a store of its own; the log that"
echo " matters is the SERVER's, and it is shown in the STATS lines."
echo
echo " In the leader's STATS: behind_bytes is history that exists on"
echo " one machine only, lag_p99_us is how late the follower runs, and"
echo " ack_timeouts counts writes where sync mode gave up waiting and"
echo " silently became async. A non-zero ack_timeouts means the"
echo " guarantee you think you have is not the one you are getting."
echo "=============================================================="
