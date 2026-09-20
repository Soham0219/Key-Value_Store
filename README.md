# KV-store

[![C++17](https://img.shields.io/badge/C%2B%2B-17-blue)](#requirements)
[![tests](https://img.shields.io/badge/tests-8%20suites%20passing-brightgreen)](#tests)
[![sanitizers](https://img.shields.io/badge/ASan%20%7C%20UBSan%20%7C%20TSan-clean-brightgreen)](#tests)
[![license](https://img.shields.io/badge/license-MIT-lightgrey)](LICENSE)

A networked key-value store built entirely from scratch in C++17 using raw POSIX system
calls.

Built to explore low-level storage engine design, concurrent data structures, and
non-blocking network programming without relying on any third-party framework. The
sockets, the log format, the locking, the event loop and the replication protocol are
all written directly on top of POSIX and the standard library.

---

## Core capabilities

* **Sharded concurrent storage.** The hash map is split into independent shards, each
  behind its own `std::shared_mutex`. Readers share a lock and run in parallel; a writer
  blocks only the one shard it touches, not the store.
* **Crash-safe write-ahead logging.** Every change is written to a log with a CRC
  checksum *before* memory is updated. On restart the log is replayed, and a half-written
  record left behind by a `kill -9` is detected and cut off.
* **Two network I/O engines**, chosen at startup:
  * `--io threads` — a thread per client. Uses every core.
  * `--io loop` — one thread with an event loop, `epoll` on Linux and `poll` elsewhere,
    for many mostly-idle connections.
* **Atomic transactions.** `BEGIN` / `COMMIT` / `ROLLBACK`. A commit is applied all at
  once and cannot be observed half-done. A client that disconnects mid-transaction rolls
  back automatically. *(Atomicity and durability — not isolation; see
  [what it does not do](#what-it-does-not-do).)*
* **LRU eviction.** Give it a memory budget and the least recently used keys are dropped
  to stay inside it.
* **Leader/follower replication.** The follower receives the leader's log, can be read
  from, resumes from where it left off after a disconnect, and can optionally be waited
  on before the leader answers the client.

---

## Codebase layout

```text
src/
├── net/        sockets, connections, the accept loop, the epoll/poll event loop
├── pool/       thread pool and its swappable task-scheduling policies
├── protocol/   command parsing, and the binary record format used by the log
├── store/      sharded thread-safe map and the LRU eviction policy
├── wal/        write-ahead log writer, crash recovery, log compaction
├── txn/        transaction buffer and atomic multi-key commit
└── repl/       leader/follower log streaming
tests/          one suite per area
bench/          load generator and measurement scripts
```

`src/main.cpp` only parses arguments and starts things up. Everything else lives in
`kvstore_core`, a static library, so the tests and the server run identical code.

---

## Quick start

### 1. Build

With AddressSanitizer and UndefinedBehaviorSanitizer on, which is the default:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j
```

### 2. Run

```bash
./build/kvstore --port 6380 --log /tmp/kvstore.log --io loop
```

### 3. Connect

```console
$ nc 127.0.0.1 6380
SET user:42 Alice
+OK
GET user:42
$Alice
BEGIN
+OK
SET balance:42 500
+OK
COMMIT
$committed=1
QUIT
+OK
```

Stop with `Ctrl-C`. Start it again with the same `--log` and the data is still there — the
log is replayed before the socket accepts anything.

---

## Commands

| Command | Payload | Reply |
| :--- | :--- | :--- |
| `SET` | `<key> <value>` | `+OK` |
| `GET` | `<key>` | `$<value>` or `$nil` |
| `DEL` | `<key>` | `+OK` |
| `BEGIN` / `COMMIT` / `ROLLBACK` | none | `+OK` / `$committed=<n>` / `$rolled_back=<n>` |
| `STATS` | none | `$hits=.. misses=.. writes=.. keys=..` |
| `COMPACT` | none | `$records=.. bytes_before=.. bytes_after=..` |
| `QUIT` | none | `+OK`, then the connection closes |

A value is the whole rest of the line, so `SET note hello there` stores `hello there`.
Commands are case-insensitive.

---

## Options

The ones you are most likely to use. `./build/kvstore --help` lists them all.

| Flag | Default | Meaning |
| :--- | :--- | :--- |
| `--port` | 6380 | TCP port |
| `--log` | *(none)* | Log file. Without it, everything is lost on exit |
| `--fsync` | `always` | `always` / `interval` / `never` |
| `--threads` | 4 | Worker threads, and the maximum number of clients |
| `--io` | `threads` | `threads` or `loop` |
| `--max-memory` | 0 | Byte budget; non-zero turns on LRU eviction |
| `--role` | `standalone` | `standalone` / `leader` / `follower` |

### Replication

```bash
# terminal 1 — the leader
./build/kvstore --port 6380 --log /tmp/leader.log --role leader --repl-port 6390

# terminal 2 — the follower
./build/kvstore --port 6381 --log /tmp/follower.log --role follower --peer 127.0.0.1:6390
```

Write to 6380, read it back from 6381. Writing to the follower returns `-ERR READONLY`.

---

## Tests

```bash
ctest --test-dir build --output-on-failure
```

Eight suites: `networking`, `concurrency`, `eviction`, `persistence`, `transactions`,
`scheduling`, `replication`, `event_loop`.

The persistence and transaction suites fork a child process, `SIGKILL` it in the middle
of a write, and then check that recovery does the right thing.

They also pass under ThreadSanitizer, which needs its own build directory because it
cannot be combined with AddressSanitizer:

```bash
cmake -S . -B build-tsan -DKVSTORE_TSAN=ON -DKVSTORE_SANITIZE=OFF
cmake --build build-tsan -j
ctest --test-dir build-tsan --output-on-failure
```

---

## Design notes and trade-offs

### 1. Sharding, to keep readers off each other

A single lock over the whole key space serialises everything. Instead, keys are hashed
into a fixed array of independent shards — `hash(key) & (shardCount - 1)`, a bitmask
rather than a modulo, which is why the shard count is rounded up to a power of two.

Readers take `std::shared_lock`, so any number of them read at the same time. A writer
takes an exclusive lock on one shard only.

Each shard is `alignas(64)` — one cache line. Without that, two threads writing to
*different* shards would still slow each other down as the shared cache line bounces
between cores. That is false sharing, and the alignment is the entire fix.

**Measured:** sharding gives 1.5x the throughput and 2.6x lower p95 versus a global
mutex. And a result that contradicted the textbook: `std::shared_mutex` was *slower* than
a plain `std::mutex` in every run. A reader/writer lock only wins when the critical
section is long enough to pay for its extra bookkeeping, and a hash lookup is not.

### 2. The log is written before memory, never after

A record is appended and synced *before* the map is changed. If the process dies in
between, replay re-applies a change memory never saw — harmless, because `SET a 1` twice
is still `SET a 1`. Reverse the order and a crash in that gap loses a write the client was
already told had succeeded.

Each record is `[length][crc32][payload]`, written little-endian byte by byte so the file
means the same thing on any machine. Replay stops at the **first** damaged record and
truncates the file there, rather than skipping it and carrying on — the log is an ordered
sequence, so skipping one produces a state that never existed, while stopping produces a
genuine prefix of history.

**Measured:** logging alone costs 18x on writes, and `--fsync always` costs another 81x
on top. Reads are untouched. That is the price of the word "durable", and it is why the
fsync mode is a flag rather than a decision made for you.

### 3. Level-triggered epoll, on purpose

`--io loop` runs one thread over non-blocking sockets (`O_NONBLOCK`) and a readiness
poller. It is **level-triggered**, not edge-triggered, and that was a deliberate choice.

Edge-triggered (`EPOLLET`) reports only the *transition* to ready. It means fewer wakeups
— and it means you **must** read in a loop until `EAGAIN`, because a read that leaves
bytes in the socket buffer will never be announced again and that connection hangs
forever with data sitting in the kernel. Level-triggered is also exactly what `poll()`
does, which is what lets one event loop drive both backends and be compared between them.

The part that actually takes the work is **output buffering**. A blocking server loops
until every byte is written, which is fine because it only parks its own thread. Here
there is no other thread to park, so a short write has to keep the remainder, register
for writability, and finish later — and it must *stop* asking about writability the
moment the buffer empties, or every idle connection wakes the loop on every iteration.

**Measured:** with 8 active clients and the connection count growing, `epoll` stays flat
at ~105,000 ops/sec from 8 connections to 5,000, while `poll` falls from 99,916 to 9,986
over the same range. `poll` rescans every registered descriptor on every call; `epoll`
returns only the ready ones.

> Full measurements, methodology, and every failure mode: **[DESIGN.md](DESIGN.md)**

---

## Benchmarks

Build without sanitizers first — they make everything 2–20x slower, so any number
measured under them is meaningless.

```bash
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release -DKVSTORE_SANITIZE=OFF
cmake --build build-release -j

./bench/run_locking.sh     # lock modes and shard counts
./bench/run_eviction.sh    # what LRU costs
./bench/run_wal.sh         # what durability costs
./bench/run_txn.sh         # what group commit buys
./bench/run_repl.sh        # what replication costs
./bench/run_io.sh          # threads vs event loop, poll vs epoll
```

`redis-benchmark` and `memtier_benchmark` speak the Redis protocol and cannot drive this
server, so all numbers come from the load generator in `bench/`.

---

## What it does not do

* **No isolation between transactions.** A commit is atomic and cannot be seen half-done,
  but there is no MVCC or snapshot isolation: two separate `GET`s can observe two
  different points in time.
* **No leader election or failover.** If the leader dies, writes stop until someone
  restarts a node as the leader.
* **With `--io threads`, the client limit is `--threads`, and it is hard** — a connection
  owns its thread until it disconnects, so the next client is not slow, it is unserved.
* **With `--io loop`, `COMPACT`, `STATS` and synchronous replication all block** the one
  thread for as long as they run.
* IPv4 only. No authentication, no TLS.

---

## Roadmap

- [x] Sharded hash map with per-shard reader/writer locks
- [x] Write-ahead log, CRC framing, crash recovery, compaction
- [x] Transactions with group commit
- [x] LRU eviction under a memory budget
- [x] Swappable scheduling policies with aging
- [x] Leader/follower replication with offset resume and sync/async modes
- [x] Event loop with `epoll`, output buffering and backpressure
- [x] Benchmarks for locking, eviction, durability, replication and I/O model
- [ ] RESP protocol support, so `redis-cli` and `redis-benchmark` can drive it
- [ ] Snapshot isolation, so multi-key reads see one consistent point in time
- [ ] One event loop per core with shared-nothing sharding

---

## Requirements

GCC 9+ or Clang 12+ with C++17, and CMake 3.16+. Linux or macOS. `epoll` is used on
Linux; macOS falls back to `poll`.

---

## License

[MIT](LICENSE)
