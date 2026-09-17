#!/usr/bin/env bash
# Transaction measurement: what group commit buys.
#
#   cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release -DKVSTORE_SANITIZE=OFF
#   cmake --build build-release -j
#   ./bench/run_txn.sh
#
# RUN THIS ON LINUX, for the same reason as run_wal.sh: on macOS fsync() returns
# before the data is really durable, so the thing being amortised here would not be
# the thing that actually costs time. In Docker:
#
#   docker compose run --rm dev
#   cmake -S . -B build-linux-release -DCMAKE_BUILD_TYPE=Release -DKVSTORE_SANITIZE=OFF
#   cmake --build build-linux-release -j
#   BENCH=./build-linux-release/benchmark LOG_DIR=/data ./bench/run_txn.sh
#
# THE IDEA. With --fsync always, every commit waits for the disk — roughly 100-200us,
# which is thousands of times longer than the work itself. That wait does not get
# bigger when the transaction contains more operations: one BEGIN, N mutations and one
# COMMIT are written as a single contiguous group and synced ONCE. So the per-write
# cost of durability falls almost linearly with transaction size.
#
# This is GROUP COMMIT. Real databases go further and batch across SEPARATE
# transactions — whatever arrives while one fsync is in flight rides along with it.
# This implementation only amortises WITHIN a transaction, which is the honest
# boundary of what this implementation claims.

set -euo pipefail

BENCH="${BENCH:-./build-release/benchmark}"
DURATION="${DURATION:-4}"
KEYS="${KEYS:-1024}"
SHARDS="${SHARDS:-16}"
VALUE_SIZE="${VALUE_SIZE:-256}"
LOG_DIR="${LOG_DIR:-/tmp}"
READ_RATIO="${READ_RATIO:-0.0}"   # pure writes: transactions only affect the write path
SIZES="${SIZES:-1 2 4 8 16 32 64}"

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

LOGFILE="${LOG_DIR}/kvstore_txn_bench_$$.log"

echo "machine: $(uname -sm), ${CORES} logical cores"
echo "config : clients=${CLIENTS} duration=${DURATION}s keys=${KEYS} value=${VALUE_SIZE}B"
echo "         read-ratio=${READ_RATIO} fsync=always log=${LOGFILE}"
if [ "$(uname -s)" != "Linux" ]; then
    echo
    echo "WARNING: not running on Linux. fsync() does not mean durability here, so the"
    echo "         cost being amortised is not the real one."
fi
echo

run_row() {   # $1 = label, rest = extra benchmark args
    local label="$1"
    shift

    rm -f "$LOGFILE"   # start each row from an empty log so replay time is not measured

    local out
    out=$("$BENCH" --lock-mode sharded --shards "$SHARDS" --clients "$CLIENTS" \
                   --duration "$DURATION" --read-ratio "$READ_RATIO" --keys "$KEYS" \
                   --value-size "$VALUE_SIZE" --log "$LOGFILE" --fsync always "$@")

    printf "%-16s %13s %11s %13s %14s\n" "$label" \
        "$(echo "$out" | awk '/^throughput/       {print $3}')" \
        "$(echo "$out" | awk '/^latency p99/      {print $4}')" \
        "$(echo "$out" | awk '/^log syncs/        {print $4}')" \
        "$(echo "$out" | awk '/^appends per sync/ {print $4}')"
}

printf "%-16s %13s %11s %13s %14s\n" "writes per txn" "ops/sec" "p99 ns" "syncs" "appends/sync"
run_row "no transaction"
for size in $SIZES; do
    run_row "$size" --txn-size "$size"
done

rm -f "$LOGFILE"

cat <<'EOF'

Reading these:

  ops/sec           Should climb close to linearly with transaction size, because the
                    per-transaction cost is dominated by ONE disk wait no matter how
                    many writes it covers.

  syncs             Should stay roughly FLAT across every row. That is the whole
                    point: the same number of disk waits is doing more and more work.

  appends/sync      N + 2 for a transaction of N writes — the mutations plus the BEGIN
                    and COMMIT markers. This column is the amortisation factor, stated
                    directly.

  "1" vs "none"     Barely differs, and it should not. A one-write transaction still
                    costs one fsync; it just writes three records instead of one. The
                    BEGIN/COMMIT markers are cheap because bytes are cheap and disk
                    WAITS are not.

  p99               Can move EITHER way, and it is worth understanding why rather
                    than assuming. Batching concentrates cost: the one operation that
                    triggers the commit absorbs the whole disk wait for the group,
                    while the other N-1 are nearly free. That alone would push the
                    tail up. But fewer total disk waits also means far less queueing
                    on the log's single mutex, which pushes it down - and in practice
                    that second effect usually wins, so p99 improves along with
                    throughput. If it ever goes the other way on your machine, the
                    explanation is that contention was not the bottleneck there.

What this does NOT do: batch across separate transactions. A real database collects
every commit that arrives while an fsync is in flight and covers them all with the
next one, so even single-write transactions amortise under load. That is the obvious
next step, and naming it is better than pretending it is already here.
EOF
