#!/usr/bin/env bash
# Runs the locking-mode comparison and prints one table.
#
# These are the numbers that go on your CV, so run it against a build with
# sanitizers OFF and optimisation ON. AddressSanitizer alone costs 2-20x, and a
# number measured under it is not a number about your code.
#
#   cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release -DKVSTORE_SANITIZE=OFF
#   cmake --build build-release -j
#   ./bench/run_matrix.sh
#
# Close your browser and anything else busy first. A laptop that thermally
# throttles halfway through will make the last mode look worse than it is, so
# run the matrix twice and keep the second result.

set -euo pipefail   # -e stop on error, -u catch typos in variable names, -o pipefail see failures in pipes

BENCH="${BENCH:-./build-release/benchmark}"   # override with BENCH=... if your build dir differs
DURATION="${DURATION:-5}"                     # seconds per configuration
KEYS="${KEYS:-1024}"                          # smaller key space = more contention
SHARDS="${SHARDS:-16}"                        # only used by the sharded mode

# Bytes per value. This matters more than it looks: the copy into the map happens
# INSIDE the critical section, so value size IS critical-section length. Below
# ~32 bytes the critical section is so short that lock acquisition overhead
# dominates and sharding barely helps; above ~2KB memory bandwidth becomes the
# bottleneck and sharding stops helping again. 64-1024 is the band where the
# lock is genuinely the thing being contended — and where real caches live.
VALUE_SIZE="${VALUE_SIZE:-64}"

if [ ! -x "$BENCH" ]; then
    echo "benchmark binary not found at $BENCH" >&2
    echo "build it first:  cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release -DKVSTORE_SANITIZE=OFF && cmake --build build-release -j" >&2
    exit 1
fi

# Report the machine alongside the numbers. A throughput figure without a core
# count is unfalsifiable, and "how many cores?" is the first thing an interviewer
# will ask about a concurrency benchmark.
if command -v nproc >/dev/null 2>&1; then
    CORES=$(nproc)                                    # Linux
else
    CORES=$(sysctl -n hw.ncpu 2>/dev/null || echo 4)  # macOS has no nproc; sysctl is the equivalent
fi

# Default the client count to the CORE COUNT, not some round number. Running more
# load threads than cores measures the SCHEDULER as much as the locking: a thread
# that gets preempted while holding a lock blocks everyone waiting on it, and that
# convoy effect grows with the number of distinct locks — which makes sharding look
# WORSE than a global lock for reasons that have nothing to do with sharding.
# Measured here: at 8 clients on 2 cores, sharding lost to a global shared_mutex;
# at 2 clients on the same 2 cores it won by 2.5x. Same code, same machine.
CLIENTS="${CLIENTS:-$CORES}"

echo "machine: $(uname -sm), ${CORES} logical cores"
echo "config : clients=${CLIENTS} duration=${DURATION}s keys=${KEYS} shards=${SHARDS} value=${VALUE_SIZE}B"
echo

run_mix() {
    local label="$1"        # human-readable name for this workload column
    local read_ratio="$2"   # fraction of operations that are GET

    echo "── ${label} (read ratio ${read_ratio}) ─────────────────────────────"
    printf "%-16s %14s %10s %10s %10s\n" "lock mode" "ops/sec" "p50 ns" "p95 ns" "p99 ns"

    for mode in global-mutex global-shared sharded; do
        # Capture the whole report once, then pick fields out of it — running the
        # benchmark three times to read three numbers would triple the wall time
        # and give you three different runs' results in one row.
        local out
        out=$("$BENCH" --lock-mode "$mode" --shards "$SHARDS" --clients "$CLIENTS" \
                       --duration "$DURATION" --read-ratio "$read_ratio" --keys "$KEYS" \
                       --value-size "$VALUE_SIZE")

        local ops p50 p95 p99
        ops=$(echo "$out" | awk '/^throughput/  {print $3}')
        p50=$(echo "$out" | awk '/^latency p50/ {print $4}')
        p95=$(echo "$out" | awk '/^latency p95/ {print $4}')
        p99=$(echo "$out" | awk '/^latency p99/ {print $4}')

        printf "%-16s %14s %10s %10s %10s\n" "$mode" "$ops" "$p50" "$p95" "$p99"
    done
    echo
}

run_mix "read-heavy"  0.9   # what a cache actually sees; shared_mutex should shine here
run_mix "balanced"    0.5   # both axes matter
run_mix "write-heavy" 0.1   # readers cannot help; this is where SHARDING has to earn its place

cat <<'EOF'
Reading the table:

  sharded vs global-mutex   The headline ratio, and the one for your CV. Sharding removes
                            writer-writer serialisation, so the gain persists even in the
                            write-heavy row where readers cannot help.

  the p99 column            Usually a bigger and more interesting story than throughput.
                            A global lock produces a long tail because an unlucky thread
                            waits behind every other thread; sharding makes it wait behind
                            only the threads hitting its own shard.

  global-shared             May come out SLOWER than global-mutex at low thread counts.
                            That is not a bug. std::shared_mutex costs more to acquire than
                            std::mutex, and with only a couple of concurrent readers the
                            parallelism it unlocks does not pay for that overhead. Reader-
                            writer locks win when readers are many; here they are few.

  value size                Matters as much as thread count, and for a reason worth saying out
                            loud: the copy into the map happens inside the critical section, so
                            value size IS critical-section length. Sweep it with
                            VALUE_SIZE=16 / 256 / 4096 ./bench/run_matrix.sh and watch the ratio
                            rise then collapse - tiny values are dominated by lock acquisition,
                            huge ones by memory bandwidth.

  thread count              Matters enormously. More load threads than cores measures the
                            scheduler: a preempted lock holder stalls everyone behind it,
                            and that convoy gets worse with more distinct locks. This script
                            defaults clients to the core count for exactly that reason.

Quote the ratio with the workload and the core count attached, and be ready to
reproduce it on demand. A number without its conditions is not a measurement.
EOF
