#!/usr/bin/env bash
# Durability measurement: what the write-ahead log and fsync cost.
#
#   cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release -DKVSTORE_SANITIZE=OFF
#   cmake --build build-release -j
#   ./bench/run_wal.sh
#
# RUN THIS ON LINUX. On macOS, fsync() returns as soon as the DRIVE has accepted
# the data into its own volatile cache, so an "always" row measured there would
# show a cost that does not correspond to any real durability guarantee. The code
# calls fcntl(F_FULLFSYNC) on macOS to get the real thing, which is slower again.
# Either way the honest number comes from Linux. Use the Dockerfile:
#
#   docker compose run --rm dev
#   cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release -DKVSTORE_SANITIZE=OFF
#   cmake --build build-release -j && LOG_DIR=/data ./bench/run_wal.sh
#
# LOG_DIR matters in Docker: /data is a NAMED VOLUME living inside the Linux VM.
# Writing the log to the bind-mounted source folder instead would measure the
# macOS/Linux file-sharing layer rather than the disk.

set -euo pipefail

BENCH="${BENCH:-./build-release/benchmark}"
DURATION="${DURATION:-5}"
KEYS="${KEYS:-1024}"
SHARDS="${SHARDS:-16}"
VALUE_SIZE="${VALUE_SIZE:-256}"
LOG_DIR="${LOG_DIR:-/tmp}"
INTERVAL_MS="${INTERVAL_MS:-1000}"

if command -v nproc >/dev/null 2>&1; then
    CORES=$(nproc)
else
    CORES=$(sysctl -n hw.ncpu 2>/dev/null || echo 4)
fi
CLIENTS="${CLIENTS:-$CORES}"

if [ ! -x "$BENCH" ]; then
    echo "benchmark binary not found at $BENCH" >&2
    exit 1
fi

LOGFILE="${LOG_DIR}/kvstore_bench_$$.log"

echo "machine: $(uname -sm), ${CORES} logical cores"
echo "config : clients=${CLIENTS} duration=${DURATION}s keys=${KEYS} value=${VALUE_SIZE}B"
echo "         log=${LOGFILE} interval=${INTERVAL_MS}ms"
if [ "$(uname -s)" != "Linux" ]; then
    echo
    echo "WARNING: not running on Linux. These durability numbers do not mean what"
    echo "         they appear to mean - see the header of this script."
fi
echo

run_row() {   # $1 = label, $2 = read ratio, rest = extra benchmark args
    local label="$1" ratio="$2"
    shift 2

    rm -f "$LOGFILE"   # each row starts from an empty log, so replay time is not measured

    local out
    out=$("$BENCH" --lock-mode sharded --shards "$SHARDS" --clients "$CLIENTS" \
                   --duration "$DURATION" --read-ratio "$ratio" --keys "$KEYS" \
                   --value-size "$VALUE_SIZE" "$@")

    local ops p99 appends syncs
    ops=$(echo "$out"     | awk '/^throughput/  {print $3}')
    p99=$(echo "$out"     | awk '/^latency p99/ {print $4}')
    appends=$(echo "$out" | awk '/^log appends/ {print $4}')
    syncs=$(echo "$out"   | awk '/^log syncs/   {print $4}')
    [ -z "$appends" ] && appends="-"
    [ -z "$syncs" ] && syncs="-"

    printf "%-26s %13s %10s %13s %13s\n" "$label" "$ops" "$p99" "$appends" "$syncs"
}

echo "── write-heavy (10% GET): every mutation goes through the log ──"
printf "%-26s %13s %10s %13s %13s\n" "durability" "ops/sec" "p99 ns" "appends" "syncs"
run_row "no log at all"       0.1
run_row "log, fsync=never"    0.1 --log "$LOGFILE" --fsync never
run_row "log, fsync=interval" 0.1 --log "$LOGFILE" --fsync interval --fsync-interval-ms "$INTERVAL_MS"
run_row "log, fsync=always"   0.1 --log "$LOGFILE" --fsync always

echo
echo "── read-heavy (90% GET): only the 10% of writes pay ──"
printf "%-26s %13s %10s %13s %13s\n" "durability" "ops/sec" "p99 ns" "appends" "syncs"
run_row "no log at all"      0.9
run_row "log, fsync=always"  0.9 --log "$LOGFILE" --fsync always

echo
echo "── pure reads (100% GET): the log is not touched at all ──"
printf "%-26s %13s %10s %13s %13s\n" "durability" "ops/sec" "p99 ns" "appends" "syncs"
run_row "no log at all"      1.0
run_row "log, fsync=always"  1.0 --log "$LOGFILE" --fsync always

rm -f "$LOGFILE"

cat <<'EOF'

Reading these:

  no log -> fsync=never      The cost of LOGGING itself: serialising a command,
                             framing it, and pushing it through the log's single
                             mutex. Note that last part - one file means one lock,
                             so the write-ahead log becomes a global serialisation
                             point and gives back much of what sharding won in
                             locking. Reads are untouched, which is why the second
                             table matters.

  fsync=never -> interval    The cost of syncing occasionally. Should be small: the
                             syncs column shows a handful over the whole run.

  interval -> always         The cost of DURABILITY, and it is the big one. Every
                             write waits for the disk. The syncs column equals the
                             appends column here - one sync per write, by definition.

  the two read tables        Read the PURE-READ table first: it should show no
                             difference at all, which proves reads genuinely never
                             touch the log. Then look at the 90%-read table, which
                             will still be far slower with fsync=always - because
                             the remaining 10% of writes each wait ~100us for a
                             disk, and 10% of a very slow thing dominates 90% of a
                             very fast one. Averages are set by the slow path.

                             So "the WAL costs X%" is meaningless without the write
                             fraction attached. Quote the mix.

What you are buying with fsync=always: a client that received "+OK" can rely on that
write surviving a POWER CUT. fsync=never still survives kill -9, because the page
cache belongs to the kernel and outlives your process. Those are different failures,
and only the first one costs anything.
EOF
