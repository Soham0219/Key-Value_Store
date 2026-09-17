#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include "net/socket.h"
#include "store/store.h"
#include "txn/transaction.h"
#include "wal/wal.h"

namespace {

using Clock = std::chrono::steady_clock;

struct Config {
    kvstore::store::LockMode lockMode = kvstore::store::LockMode::Sharded;
    std::size_t shards = 16;
    std::size_t clients = 8;
    int durationSeconds = 5;
    double readRatio = 0.9;
    std::size_t keySpace = 1024;
    std::size_t valueSize = 64;
    std::size_t maxMemory = 0;
    std::string logPath;
    kvstore::wal::FsyncMode fsyncMode = kvstore::wal::FsyncMode::Always;
    int fsyncIntervalMs = 1000;
    std::size_t txnSize = 0;
    bool tcp = false;
    std::uint16_t port = 6380;

    std::size_t connections = 0;
};

struct ThreadResult {
    std::uint64_t ops = 0;
    std::vector<std::uint32_t> latenciesNs;
};

constexpr std::size_t kMaxSamplesPerThread = 2'000'000;

std::uint32_t percentile(const std::vector<std::uint32_t>& sorted, double fraction) {
    if (sorted.empty()) {
        return 0;
    }
    std::size_t index = static_cast<std::size_t>(fraction * static_cast<double>(sorted.size()));
    if (index >= sorted.size()) {
        index = sorted.size() - 1;
    }
    return sorted[index];
}

void directWorker(kvstore::store::Store& store,
                  const Config& config,
                  const std::atomic<bool>& stop,
                  ThreadResult& result,
                  unsigned seed) {
    std::mt19937 rng(seed);
    std::uniform_int_distribution<std::size_t> keyDist(0, config.keySpace - 1);
    std::uniform_real_distribution<double> opDist(0.0, 1.0);

    result.latenciesNs.reserve(1 << 16);

    std::vector<std::string> keys;
    keys.reserve(config.keySpace);
    for (std::size_t i = 0; i < config.keySpace; ++i) {
        keys.push_back("key:" + std::to_string(i));
    }

    const std::string payload(config.valueSize, 'x');

    std::string scratch;

    std::unique_ptr<kvstore::txn::Transaction> transaction;
    std::size_t bufferedWrites = 0;

    while (!stop.load(std::memory_order_relaxed)) {
        const std::string& key = keys[keyDist(rng)];
        const bool isRead = opDist(rng) < config.readRatio;

        const Clock::time_point start = Clock::now();
        if (isRead) {
            if (transaction) {
                transaction->get(key, scratch);
            } else {
                store.get(key, scratch);
            }
        } else if (config.txnSize > 0) {
            if (!transaction) {
                transaction = std::make_unique<kvstore::txn::Transaction>(store);
                bufferedWrites = 0;
            }
            transaction->set(key, payload);
            if (++bufferedWrites >= config.txnSize) {
                transaction->commit();
                transaction.reset();
            }
        } else {
            store.set(key, payload);
        }
        const Clock::time_point end = Clock::now();

        ++result.ops;
        if (result.latenciesNs.size() < kMaxSamplesPerThread) {
            const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
            result.latenciesNs.push_back(static_cast<std::uint32_t>(ns));
        }
    }
}

kvstore::net::FileDescriptor connectLocal(std::uint16_t port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return kvstore::net::FileDescriptor{};
    }
    kvstore::net::FileDescriptor owned(fd);

    int one = 1;
    if (::setsockopt(owned.get(), IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) < 0) {
        return kvstore::net::FileDescriptor{};
    }

    timeval receiveTimeout{};
    receiveTimeout.tv_sec = 2;
    receiveTimeout.tv_usec = 0;
    if (::setsockopt(owned.get(), SOL_SOCKET, SO_RCVTIMEO, &receiveTimeout,
                     sizeof(receiveTimeout)) < 0) {
        return kvstore::net::FileDescriptor{};
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr) != 1) {
        return kvstore::net::FileDescriptor{};
    }
    if (::connect(owned.get(), reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) < 0) {
        return kvstore::net::FileDescriptor{};
    }
    return owned;
}

bool roundTrip(int fd, const std::string& command, std::string& reply) {
    std::size_t sent = 0;
    while (sent < command.size()) {
        ssize_t n = ::write(fd, command.data() + sent, command.size() - sent);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        sent += static_cast<std::size_t>(n);
    }

    reply.clear();
    char buffer[256];
    for (;;) {
        ssize_t n = ::read(fd, buffer, sizeof(buffer));
        if (n <= 0) {
            return false;
        }
        reply.append(buffer, static_cast<std::size_t>(n));
        if (reply.back() == '\n') {
            return true;
        }
    }
}

void tcpWorker(const Config& config,
               const std::atomic<bool>& stop,
               std::atomic<std::size_t>& ready,
               const std::atomic<bool>& go,
               ThreadResult& result,
               unsigned seed) {
    std::size_t perThread = 1;
    if (config.connections > config.clients) {
        perThread = config.connections / config.clients;
    }

    std::vector<kvstore::net::FileDescriptor> idle;
    idle.reserve(perThread > 0 ? perThread - 1 : 0);

    for (std::size_t i = 1; i < perThread; ++i) {
        kvstore::net::FileDescriptor extra = connectLocal(config.port);
        if (!extra.valid()) {
            std::cerr << "benchmark: only opened " << idle.size() << " of " << (perThread - 1)
                      << " idle connections on this thread (check ulimit -n)\n";
            break;
        }
        idle.push_back(std::move(extra));
    }

    kvstore::net::FileDescriptor fd = connectLocal(config.port);
    if (!fd.valid()) {
        std::cerr << "benchmark: could not connect to 127.0.0.1:" << config.port
                  << " — is the server running?\n";

        ready.fetch_add(1, std::memory_order_release);
        return;
    }

    std::mt19937 rng(seed);
    std::uniform_int_distribution<std::size_t> keyDist(0, config.keySpace - 1);
    std::uniform_real_distribution<double> opDist(0.0, 1.0);

    result.latenciesNs.reserve(1 << 16);
    std::string reply;

    std::vector<std::string> getCommands;
    std::vector<std::string> setCommands;
    getCommands.reserve(config.keySpace);
    setCommands.reserve(config.keySpace);
    for (std::size_t i = 0; i < config.keySpace; ++i) {
        getCommands.push_back("GET key:" + std::to_string(i) + "\n");
        setCommands.push_back("SET key:" + std::to_string(i) + " " +
                              std::string(config.valueSize, 'x') + "\n");
    }

    ready.fetch_add(1, std::memory_order_release);
    while (!go.load(std::memory_order_acquire) && !stop.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    while (!stop.load(std::memory_order_relaxed)) {
        const std::size_t keyId = keyDist(rng);
        const bool isRead = opDist(rng) < config.readRatio;
        const std::string& command = isRead ? getCommands[keyId] : setCommands[keyId];

        const Clock::time_point start = Clock::now();
        const bool ok = roundTrip(fd.get(), command, reply);
        const Clock::time_point end = Clock::now();

        if (!ok) {
            return;
        }

        ++result.ops;
        if (result.latenciesNs.size() < kMaxSamplesPerThread) {
            const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
            result.latenciesNs.push_back(static_cast<std::uint32_t>(ns));
        }
    }
}

void printUsage(const char* program) {
    std::cerr
        << "usage: " << program << " [options]\n"
        << "  --lock-mode MODE   none | global-mutex | global-shared | sharded (default sharded)\n"
        << "  --shards N         shard count for --lock-mode sharded (default 16)\n"
        << "  --clients N        concurrent load-generating threads (default 8)\n"
        << "  --duration N       seconds to run (default 5)\n"
        << "  --read-ratio F     fraction of operations that are GET, 0.0-1.0 (default 0.9)\n"
        << "  --keys N           key space size; smaller means more contention (default 1024)\n"
        << "  --value-size N     bytes per value (default 64); longer values = longer critical\n"
        << "                     sections, which is where sharding actually pays off\n"
        << "  --max-memory N     byte budget, 0 = unlimited (default 0). Non-zero turns on exact\n"
        << "                     LRU, which makes every GET take an EXCLUSIVE lock -- compare\n"
        << "                     0 against a limit to price what exact eviction costs you\n"
        << "  --log PATH         write-ahead log file; omit for no logging at all\n"
        << "  --fsync MODE       always | interval | never (default always)\n"
        << "  --fsync-interval-ms N   interval for --fsync interval (default 1000)\n"
        << "  --txn-size N       group N writes into one transaction (default 0 = none).\n"
        << "                     With --fsync always this is GROUP COMMIT: N writes cost\n"
        << "                     ONE disk wait instead of N. Sweep it to see the effect.\n"
        << "                     Run with a high --read-ratio and a low one: logging only\n"
        << "                     touches WRITES, so the cost is invisible on a read workload\n"
        << "  --transport T      direct | tcp (default direct)\n"
        << "  --port N           server port for --transport tcp (default 6380)\n"
        << "  --conns N          [tcp] TOTAL connections to hold open (default: one per\n"
        << "                     client thread). Anything above --clients is opened and\n"
        << "                     left IDLE. Idle connections are the whole point: they\n"
        << "                     cost nothing in an event loop, one thread each in a\n"
        << "                     thread pool, and O(n) per wait under poll() -- see\n"
        << "                     bench/run_io.sh\n";
}

bool parseArgs(int argc, char** argv, Config& config) {
    for (int i = 1; i < argc; ++i) {
        const bool hasValue = (i + 1 < argc);
        if (std::strcmp(argv[i], "--lock-mode") == 0 && hasValue) {
            if (!kvstore::store::parseLockMode(argv[++i], config.lockMode)) {
                return false;
            }
        } else if (std::strcmp(argv[i], "--shards") == 0 && hasValue) {
            config.shards = std::strtoul(argv[++i], nullptr, 10);
        } else if (std::strcmp(argv[i], "--clients") == 0 && hasValue) {
            config.clients = std::strtoul(argv[++i], nullptr, 10);
        } else if (std::strcmp(argv[i], "--duration") == 0 && hasValue) {
            config.durationSeconds = static_cast<int>(std::strtol(argv[++i], nullptr, 10));
        } else if (std::strcmp(argv[i], "--read-ratio") == 0 && hasValue) {
            config.readRatio = std::strtod(argv[++i], nullptr);
        } else if (std::strcmp(argv[i], "--keys") == 0 && hasValue) {
            config.keySpace = std::strtoul(argv[++i], nullptr, 10);
        } else if (std::strcmp(argv[i], "--value-size") == 0 && hasValue) {
            config.valueSize = std::strtoul(argv[++i], nullptr, 10);
        } else if (std::strcmp(argv[i], "--max-memory") == 0 && hasValue) {
            config.maxMemory = std::strtoul(argv[++i], nullptr, 10);
        } else if (std::strcmp(argv[i], "--log") == 0 && hasValue) {
            config.logPath = argv[++i];
        } else if (std::strcmp(argv[i], "--fsync") == 0 && hasValue) {
            if (!kvstore::wal::parseFsyncMode(argv[++i], config.fsyncMode)) {
                return false;
            }
        } else if (std::strcmp(argv[i], "--fsync-interval-ms") == 0 && hasValue) {
            config.fsyncIntervalMs = static_cast<int>(std::strtol(argv[++i], nullptr, 10));
        } else if (std::strcmp(argv[i], "--txn-size") == 0 && hasValue) {
            config.txnSize = std::strtoul(argv[++i], nullptr, 10);
        } else if (std::strcmp(argv[i], "--transport") == 0 && hasValue) {
            const std::string transport = argv[++i];
            if (transport == "direct") {
                config.tcp = false;
            } else if (transport == "tcp") {
                config.tcp = true;
            } else {
                return false;
            }
        } else if (std::strcmp(argv[i], "--conns") == 0 && hasValue) {
            config.connections = static_cast<std::size_t>(std::strtoul(argv[++i], nullptr, 10));
        } else if (std::strcmp(argv[i], "--port") == 0 && hasValue) {
            config.port = static_cast<std::uint16_t>(std::strtoul(argv[++i], nullptr, 10));
        } else {
            return false;
        }
    }

    return config.clients > 0 && config.durationSeconds > 0 && config.keySpace > 0 &&
           config.valueSize > 0 && config.readRatio >= 0.0 && config.readRatio <= 1.0;
}
}

int main(int argc, char** argv) {
    Config config;
    if (!parseArgs(argc, argv, config)) {
        printUsage(argv[0]);
        return EXIT_FAILURE;
    }

    kvstore::store::StoreConfig storeConfig;
    storeConfig.lockMode = config.lockMode;
    storeConfig.shardCount = config.shards;
    storeConfig.maxMemoryBytes = config.maxMemory;
    storeConfig.logPath = config.logPath;
    storeConfig.fsyncMode = config.fsyncMode;
    storeConfig.fsyncIntervalMs = config.fsyncIntervalMs;
    kvstore::store::Store store(storeConfig);

    if (!config.tcp) {
        const std::string payload(config.valueSize, 'x');
        for (std::size_t i = 0; i < config.keySpace; ++i) {
            store.applyFromLog("key:" + std::to_string(i), payload);
        }
    }

    std::atomic<std::size_t> ready{0};
    std::atomic<bool> go{false};
    std::atomic<bool> stop{false};
    std::vector<ThreadResult> results(config.clients);
    std::vector<std::thread> threads;
    threads.reserve(config.clients);

    for (std::size_t i = 0; i < config.clients; ++i) {
        if (config.tcp) {
            threads.emplace_back(tcpWorker, std::cref(config), std::cref(stop), std::ref(ready),
                                 std::cref(go), std::ref(results[i]),
                                 static_cast<unsigned>(i * 2654435761u));
        } else {
            threads.emplace_back(directWorker, std::ref(store), std::cref(config), std::cref(stop),
                                 std::ref(results[i]), static_cast<unsigned>(i * 2654435761u));
        }
    }

    if (config.tcp) {
        while (ready.load(std::memory_order_acquire) < config.clients) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    const Clock::time_point runStart = Clock::now();
    go.store(true, std::memory_order_release);

    std::this_thread::sleep_for(std::chrono::seconds(config.durationSeconds));
    stop.store(true, std::memory_order_relaxed);

    for (std::thread& thread : threads) {
        thread.join();
    }

    const double elapsedSeconds =
        std::chrono::duration<double>(Clock::now() - runStart).count();

    std::uint64_t totalOps = 0;
    std::vector<std::uint32_t> allLatencies;
    for (const ThreadResult& result : results) {
        totalOps += result.ops;
        allLatencies.insert(allLatencies.end(), result.latenciesNs.begin(), result.latenciesNs.end());
    }
    std::sort(allLatencies.begin(), allLatencies.end());

    const double throughput = (elapsedSeconds > 0.0)
        ? static_cast<double>(totalOps) / elapsedSeconds
        : 0.0;

    std::printf("\n");
    std::printf("transport      : %s\n", config.tcp ? "tcp" : "direct");
    if (config.tcp) {
        const std::size_t total = config.connections > config.clients ? config.connections
                                                                      : config.clients;
        std::printf("connections    : %zu total (%zu active, %zu idle)\n",
                    total, config.clients, total - config.clients);
    }
    std::printf("lock mode      : %s\n", kvstore::store::lockModeName(config.lockMode));
    if (config.lockMode == kvstore::store::LockMode::Sharded) {
        std::printf("shards         : %zu\n", store.shardCount());
    }
    std::printf("clients        : %zu\n", config.clients);
    std::printf("read ratio     : %.2f\n", config.readRatio);
    std::printf("key space      : %zu\n", config.keySpace);
    std::printf("value size     : %zu bytes\n", config.valueSize);
    if (config.txnSize > 0) {
        std::printf("txn size       : %zu writes per transaction (group commit)\n", config.txnSize);
    } else {
        std::printf("txn size       : none (every write commits on its own)\n");
    }

    if (config.logPath.empty()) {
        std::printf("write-ahead log: off (nothing is durable)\n");
    } else {
        std::printf("write-ahead log: %s, fsync=%s\n", config.logPath.c_str(),
                    kvstore::wal::fsyncModeName(config.fsyncMode));
    }
    if (config.maxMemory == 0) {
        std::printf("max memory     : unlimited (no eviction, reads take a SHARED lock)\n");
    } else {
        std::printf("max memory     : %zu bytes (LRU on, reads take an EXCLUSIVE lock)\n",
                    config.maxMemory);
    }
    std::printf("duration       : %.2f s\n", elapsedSeconds);
    std::printf("operations     : %llu\n", static_cast<unsigned long long>(totalOps));
    std::printf("throughput     : %.0f ops/sec\n", throughput);
    std::printf("latency p50    : %u ns\n", percentile(allLatencies, 0.50));
    std::printf("latency p95    : %u ns\n", percentile(allLatencies, 0.95));
    std::printf("latency p99    : %u ns\n", percentile(allLatencies, 0.99));
    std::printf("samples        : %zu\n", allLatencies.size());
    if (!config.tcp) {
        std::printf("store hits     : %llu\n", static_cast<unsigned long long>(store.hits()));
        std::printf("store misses   : %llu\n", static_cast<unsigned long long>(store.misses()));
        std::printf("evictions      : %llu\n", static_cast<unsigned long long>(store.evictions()));
        std::printf("bytes tracked  : %zu\n", store.bytesUsed());
        if (store.logEnabled()) {
            std::printf("log appends    : %llu\n",
                        static_cast<unsigned long long>(store.log()->appends()));
            std::printf("log syncs      : %llu\n",
                        static_cast<unsigned long long>(store.log()->syncs()));
            std::printf("log bytes      : %llu\n",
                        static_cast<unsigned long long>(store.log()->bytesWritten()));

            const std::uint64_t syncs = store.log()->syncs();
            if (syncs > 0) {
                std::printf("appends per sync: %.2f\n",
                            static_cast<double>(store.log()->appends()) /
                            static_cast<double>(syncs));
            }
        }
    }
    std::printf("\n");

    return EXIT_SUCCESS;
}
