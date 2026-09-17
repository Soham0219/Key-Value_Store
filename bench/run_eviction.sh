#!/usr/bin/env bash
# Eviction measurements: what exact LRU costs, and where thrashing begins.
#
#   cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release -DKVSTORE_SANITIZE=OFF
#   cmake --build build-release -j
#   ./bench/run_eviction.sh
#
# Two tables, answering two separate questions:
#
#   Table 1  What does turning eviction on cost when nothing is actually evicted?
#            The budget is set large enough that no key is ever dropped, so the
#            difference is pure overhead: one virtual call per operation, plus -
#            far more importantly - every GET taking an EXCLUSIVE lock instead of a
#            shared one, because recording an access mutates the recency list.
#
#   Table 2  What happens as the cache gets too small for the working set? This is
#            the textbook thrashing curve, and it is the same curve an operating
#            system shows when physical memory cannot hold a process's working set.

set -euo pipefail

BENCH="${BENCH:-./build-release/benchmark}"
DURATION="${DURATION:-5}"
KEYS="${KEYS:-1024}"
SHARDS="${SHARDS:-16}"
VALUE_SIZE="${VALUE_SIZE:-256}"
READ_RATIO="${READ_RATIO:-0.9}"

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

# Per-entry footprint as ShardedMap accounts for it: key + value + a fixed overhead
# constant. Keys are "key:0".."key:1023", so ~8 bytes; close enough to size budgets in
# units of entries rather than magic byte counts.
OVERHEAD=192
PER_ENTRY=$(( 8 + VALUE_SIZE + OVERHEAD ))

echo "machine: $(uname -sm), ${CORES} logical cores"
echo "config : clients=${CLIENTS} duration=${DURATION}s keys=${KEYS} value=${VALUE_SIZE}B"
echo "         shards=${SHARDS} read-ratio=${READ_RATIO} per-entry=~${PER_ENTRY}B"
echo

run_one() {   # $1 = lock mode, $2 = max-memory bytes
    "$BENCH" --lock-mode "$1" --shards "$SHARDS" --clients "$CLIENTS" --duration "$DURATION" \
             --read-ratio "$READ_RATIO" --keys "$KEYS" --value-size "$VALUE_SIZE" --max-memory "$2"
}

field() { echo "$1" | awk -v k="$2" '$0 ~ "^"k {print $NF}'; }

echo "── Table 1: the cost of exact LRU (budget large enough that nothing is evicted) ──"
printf "%-30s %13s %9s %9s %9s\n" "configuration" "ops/sec" "p50 ns" "p95 ns" "p99 ns"

# 4x the working set: eviction is switched ON but never actually fires, so the
# numbers isolate overhead from the work of evicting.
ROOMY=$(( PER_ENTRY * KEYS * 4 ))

for mode in sharded global-shared; do
    for budget in 0 "$ROOMY"; do
        out=$(run_one "$mode" "$budget")
        if [ "$budget" = "0" ]; then label="$mode, eviction off"; else label="$mode, LRU on"; fi
        printf "%-30s %13s %9s %9s %9s\n" "$label" \
            "$(echo "$out"|awk '/^throughput/{print $3}')" \
            "$(echo "$out"|awk '/^latency p50/{print $4}')" \
            "$(echo "$out"|awk '/^latency p95/{print $4}')" \
            "$(echo "$out"|awk '/^latency p99/{print $4}')"
    done
done

echo
echo "── Table 2: thrashing — shrinking the cache below the working set ──"
printf "%-24s %13s %13s %9s %9s\n" "budget" "ops/sec" "evictions" "hit %" "p99 ns"

# Working set is KEYS entries. Sweep from comfortably larger down to a tiny fraction.
for frac in 400 100 50 25 10 5 2; do
    budget=$(( PER_ENTRY * KEYS * frac / 100 ))
    out=$(run_one sharded "$budget")

    ops=$(echo "$out"|awk '/^throughput/{print $3}')
    ev=$(echo "$out"|awk '/^evictions/{print $3}')
    hits=$(echo "$out"|awk '/^store hits/{print $4}')
    misses=$(echo "$out"|awk '/^store misses/{print $4}')
    p99=$(echo "$out"|awk '/^latency p99/{print $4}')

    if [ "$(( hits + misses ))" -gt 0 ]; then
        rate=$(echo "scale=1; 100 * $hits / ($hits + $misses)" | bc -l)
    else
        rate="n/a"
    fi
    printf "%-24s %13s %13s %9s %9s\n" "${frac}% of working set" "$ops" "$ev" "$rate" "$p99"
done

cat <<'EOF'

Reading these:

  Table 1   The gap between "eviction off" and "LRU on" is the price of EXACT LRU,
            and almost all of it is the lock change, not the virtual call. Recording
            an access reorders the recency list, so a GET mutates shared state and
            can no longer hold a shared lock. Exact LRU turns every read into a write.
            This is precisely why Redis uses APPROXIMATE LRU: it samples a few random
            keys and evicts the oldest of those, so its reads stay reads.

  Table 2   Above 100% of the working set the hit rate is ~100% and evictions are
            near zero - the cache holds everything that is being asked for. As the
            budget falls below the working set, every entry gets evicted before it is
            next needed, so the hit rate collapses while eviction work climbs. That is
            THRASHING, and it is the same curve an OS shows when physical memory
            cannot hold a process's working set: the system spends its time moving
            pages rather than doing work.

EOF
