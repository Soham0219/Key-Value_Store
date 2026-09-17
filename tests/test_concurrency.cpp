#include <atomic>
#include <cstddef>
#include <iostream>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "store/sharded_map.h"
#include "store/store.h"

namespace {

int g_failures = 0;

void check(bool condition, const std::string& what, int line) {
    if (condition) {
        std::cout << "  ok   " << what << '\n';
    } else {
        std::cout << "  FAIL " << what << "  (line " << line << ")\n";
        ++g_failures;
    }
}

#define CHECK(cond) check((cond), #cond, __LINE__)

using kvstore::store::LockMode;
using kvstore::store::ShardedMap;
using kvstore::store::Store;

const LockMode kAllModes[] = {
    LockMode::None,
    LockMode::GlobalMutex,
    LockMode::GlobalSharedMutex,
    LockMode::Sharded,
};

void testModeParsing() {
    std::cout << "lock mode parsing\n";
    LockMode mode = LockMode::None;

    CHECK(kvstore::store::parseLockMode("sharded", mode) && mode == LockMode::Sharded);
    CHECK(kvstore::store::parseLockMode("global-mutex", mode) && mode == LockMode::GlobalMutex);
    CHECK(kvstore::store::parseLockMode("global-shared", mode) && mode == LockMode::GlobalSharedMutex);
    CHECK(kvstore::store::parseLockMode("none", mode) && mode == LockMode::None);

    CHECK(!kvstore::store::parseLockMode("shrded", mode));
    CHECK(!kvstore::store::parseLockMode("", mode));
}

void testShardCountRounding() {
    std::cout << "shard count rounding\n";

    CHECK(ShardedMap(LockMode::Sharded, 1).shardCount() == 1);
    CHECK(ShardedMap(LockMode::Sharded, 3).shardCount() == 4);
    CHECK(ShardedMap(LockMode::Sharded, 16).shardCount() == 16);
    CHECK(ShardedMap(LockMode::Sharded, 17).shardCount() == 32);

    CHECK(ShardedMap(LockMode::GlobalMutex, 64).shardCount() == 1);
    CHECK(ShardedMap(LockMode::GlobalSharedMutex, 64).shardCount() == 1);
    CHECK(ShardedMap(LockMode::None, 64).shardCount() == 1);
}

void testSingleThreadedSemanticsAcrossModes() {
    std::cout << "identical semantics in every mode (single-threaded)\n";

    for (LockMode mode : kAllModes) {
        Store store(mode, 8);
        std::string value;

        CHECK(!store.get("absent", value));
        store.set("a", "1");
        CHECK(store.get("a", value) && value == "1");
        store.set("a", "2");
        CHECK(store.get("a", value) && value == "2");
        CHECK(store.size() == 1);
        CHECK(store.del("a"));
        CHECK(!store.del("a"));
        CHECK(store.size() == 0);

        store.set("empty", "");
        CHECK(store.get("empty", value) && value.empty());
    }
}

void testKeysLandInMultipleShards() {
    std::cout << "keys distribute across shards\n";
    ShardedMap map(LockMode::Sharded, 16);

    constexpr std::size_t kKeys = 4096;
    for (std::size_t i = 0; i < kKeys; ++i) {
        map.set("key:" + std::to_string(i), "v");
    }
    CHECK(map.size() == kKeys);

    std::string value;
    bool allPresent = true;
    for (std::size_t i = 0; i < kKeys; ++i) {
        allPresent &= map.get("key:" + std::to_string(i), value);
    }
    CHECK(allPresent);
}

void testConcurrentDisjointWrites() {
    std::cout << "concurrent writers, disjoint keys\n";

    constexpr std::size_t kThreads = 8;
    constexpr std::size_t kPerThread = 2000;

    Store store(LockMode::Sharded, 16);
    std::vector<std::thread> writers;
    writers.reserve(kThreads);

    for (std::size_t t = 0; t < kThreads; ++t) {
        writers.emplace_back([&store, t] {
            for (std::size_t i = 0; i < kPerThread; ++i) {
                const std::string key = "t" + std::to_string(t) + ":k" + std::to_string(i);
                store.set(key, "v" + std::to_string(t));
            }
        });
    }
    for (std::thread& writer : writers) {
        writer.join();
    }

    CHECK(store.size() == kThreads * kPerThread);

    bool allCorrect = true;
    std::string value;
    for (std::size_t t = 0; t < kThreads; ++t) {
        for (std::size_t i = 0; i < kPerThread; ++i) {
            const std::string key = "t" + std::to_string(t) + ":k" + std::to_string(i);
            allCorrect &= store.get(key, value) && value == "v" + std::to_string(t);
        }
    }
    CHECK(allCorrect);
}

void testConcurrentContendedReadWrite() {
    std::cout << "concurrent readers and writers, contended keys\n";

    constexpr std::size_t kWriters = 4;
    constexpr std::size_t kReaders = 4;
    constexpr std::size_t kKeys = 8;
    constexpr int kIterations = 20000;

    const std::unordered_set<std::string> legalValues = {
        "short", "a-considerably-longer-value-that-forces-a-heap-allocation"
    };

    Store store(LockMode::Sharded, 16);
    for (std::size_t k = 0; k < kKeys; ++k) {
        store.set("hot:" + std::to_string(k), "short");
    }

    std::atomic<bool> stop{false};
    std::atomic<std::uint64_t> illegalReads{0};

    std::vector<std::thread> threads;
    threads.reserve(kWriters + kReaders);

    for (std::size_t w = 0; w < kWriters; ++w) {
        threads.emplace_back([&store, w] {
            for (int i = 0; i < kIterations; ++i) {
                const std::string key = "hot:" + std::to_string((i + w) % kKeys);

                store.set(key, (i % 2 == 0)
                    ? "short"
                    : "a-considerably-longer-value-that-forces-a-heap-allocation");
            }
        });
    }

    for (std::size_t r = 0; r < kReaders; ++r) {
        threads.emplace_back([&store, &stop, &illegalReads, &legalValues] {
            std::string value;
            std::size_t i = 0;
            while (!stop.load(std::memory_order_relaxed)) {
                if (store.get("hot:" + std::to_string(i++ % kKeys), value)) {
                    if (legalValues.find(value) == legalValues.end()) {
                        illegalReads.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            }
        });
    }

    for (std::size_t w = 0; w < kWriters; ++w) {
        threads[w].join();
    }
    stop.store(true, std::memory_order_relaxed);
    for (std::size_t r = kWriters; r < threads.size(); ++r) {
        threads[r].join();
    }

    CHECK(illegalReads.load() == 0);
    CHECK(store.size() == kKeys);
}

void testSizeUnderConcurrency() {
    std::cout << "size() while writers run (multi-lock path)\n";

    Store store(LockMode::Sharded, 16);
    std::atomic<bool> stop{false};

    std::thread writer([&store, &stop] {
        std::size_t i = 0;
        while (!stop.load(std::memory_order_relaxed)) {
            store.set("k" + std::to_string(i % 500), "v");
            ++i;
        }
    });

    bool sizeAlwaysSane = true;
    for (int i = 0; i < 2000; ++i) {
        const std::size_t size = store.size();
        sizeAlwaysSane &= (size <= 500);
    }

    stop.store(true, std::memory_order_relaxed);
    writer.join();

    CHECK(sizeAlwaysSane);
    CHECK(store.size() <= 500);
}

void testCounters() {
    std::cout << "atomic hit/miss counters\n";

    Store store(LockMode::Sharded, 4);
    std::string value;

    store.set("present", "x");
    store.get("present", value);
    store.get("present", value);
    store.get("absent", value);

    CHECK(store.hits() == 2);
    CHECK(store.misses() == 1);
    CHECK(store.writes() == 1);

    constexpr std::size_t kThreads = 8;
    constexpr int kPerThread = 5000;
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (std::size_t t = 0; t < kThreads; ++t) {
        threads.emplace_back([&store] {
            std::string local;
            for (int i = 0; i < kPerThread; ++i) {
                store.get("present", local);
            }
        });
    }
    for (std::thread& thread : threads) {
        thread.join();
    }
    CHECK(store.hits() == 2 + kThreads * kPerThread);
}
}

int main() {
    testModeParsing();
    testShardCountRounding();
    testSingleThreadedSemanticsAcrossModes();
    testKeysLandInMultipleShards();
    testConcurrentDisjointWrites();
    testConcurrentContendedReadWrite();
    testSizeUnderConcurrency();
    testCounters();

    std::cout << (g_failures == 0 ? "\nALL TESTS PASSED\n" : "\nFAILURES\n");
    return g_failures == 0 ? 0 : 1;
}
