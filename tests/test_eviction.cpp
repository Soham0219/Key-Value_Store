#include <atomic>
#include <cstddef>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "protocol/parser.h"
#include "store/lru_policy.h"
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
using kvstore::store::LruPolicy;
using kvstore::store::ShardedMap;
using kvstore::store::Store;

constexpr std::size_t kOverhead = ShardedMap::kEntryOverheadBytes;

std::size_t entryBytes(const std::string& key, const std::string& value) {
    return key.size() + value.size() + kOverhead;
}

void testPolicyOrdering() {
    std::cout << "LruPolicy: recency ordering\n";
    LruPolicy policy;

    policy.onInsert("a", 10);
    policy.onInsert("b", 10);
    policy.onInsert("c", 10);

    std::string victim;
    CHECK(policy.evict(victim) && victim == "a");
    CHECK(policy.evict(victim) && victim == "b");
    CHECK(policy.evict(victim) && victim == "c");
    CHECK(!policy.evict(victim));
}

void testPolicyAccessPromotes() {
    std::cout << "LruPolicy: access promotes to most-recent\n";
    LruPolicy policy;

    policy.onInsert("a", 10);
    policy.onInsert("b", 10);
    policy.onInsert("c", 10);
    policy.onAccess("a");

    std::string victim;
    CHECK(policy.evict(victim) && victim == "b");
    CHECK(policy.evict(victim) && victim == "c");
    CHECK(policy.evict(victim) && victim == "a");

    policy.onAccess("never-seen");
    CHECK(policy.entries() == 0);
}

void testPolicyByteAccounting() {
    std::cout << "LruPolicy: byte accounting\n";
    LruPolicy policy;

    policy.onInsert("a", 100);
    policy.onInsert("b", 250);
    CHECK(policy.bytes() == 350);
    CHECK(policy.entries() == 2);

    policy.onInsert("a", 40);
    CHECK(policy.bytes() == 290);
    CHECK(policy.entries() == 2);

    policy.onErase("b");
    CHECK(policy.bytes() == 40);
    CHECK(policy.entries() == 1);

    policy.onErase("missing");
    CHECK(policy.bytes() == 40);

    std::string victim;
    policy.evict(victim);
    CHECK(policy.bytes() == 0);
}

void testPolicyOverwriteIsAUse() {
    std::cout << "LruPolicy: writing a key counts as using it\n";
    LruPolicy policy;

    policy.onInsert("a", 10);
    policy.onInsert("b", 10);
    policy.onInsert("a", 10);

    std::string victim;
    CHECK(policy.evict(victim) && victim == "b");
}

void testPolicyIteratorsSurviveChurn() {
    std::cout << "LruPolicy: stored iterators survive heavy churn\n";

    LruPolicy policy;
    constexpr int kKeys = 500;

    for (int i = 0; i < kKeys; ++i) {
        policy.onInsert("k" + std::to_string(i), 10);
    }
    for (int round = 0; round < 50; ++round) {
        for (int i = 0; i < kKeys; ++i) {
            policy.onAccess("k" + std::to_string(i));
        }
    }
    for (int i = 0; i < kKeys; i += 2) {
        policy.onErase("k" + std::to_string(i));
    }

    CHECK(policy.entries() == kKeys / 2);
    CHECK(policy.bytes() == (kKeys / 2) * 10);

    std::string victim;
    CHECK(policy.evict(victim) && victim == "k1");
}

void testUnlimitedMeansNoPolicy() {
    std::cout << "max-memory 0 means eviction is completely off\n";

    Store store(LockMode::Sharded, 16, 0);
    CHECK(!store.evictionEnabled());

    for (int i = 0; i < 5000; ++i) {
        store.set("k" + std::to_string(i), "value");
    }
    CHECK(store.size() == 5000);
    CHECK(store.evictions() == 0);
    CHECK(store.bytesUsed() == 0);
}

void testEvictionKeepsStoreUnderLimit() {
    std::cout << "a memory limit actually bounds the store\n";

    const std::string value(64, 'x');
    const std::size_t perEntry = entryBytes("key:1000", value);
    const std::size_t budget = perEntry * 100;

    Store store(LockMode::Sharded, 16, budget);
    CHECK(store.evictionEnabled());

    for (int i = 0; i < 5000; ++i) {
        store.set("key:" + std::to_string(i), value);
    }

    CHECK(store.evictions() > 0);

    CHECK(store.size() <= 200);
    CHECK(store.size() > 0);
}

void testEvictionOrderIsLruWithOneShard() {
    std::cout << "the key evicted is the least recently USED one\n";

    const std::string value = "v";
    const std::size_t perEntry = entryBytes("k0", value);
    const std::size_t budget = perEntry * 10 + 100;

    Store store(LockMode::GlobalSharedMutex, 1, budget);
    for (int i = 0; i < 10; ++i) {
        store.set("k" + std::to_string(i), value);
    }
    CHECK(store.size() == 10);
    CHECK(store.evictions() == 0);

    std::string out;
    CHECK(store.get("k0", out));

    store.set("k10", value);

    CHECK(store.evictions() >= 1);
    CHECK(store.get("k0", out));
    CHECK(!store.get("k1", out));
    CHECK(store.get("k10", out));
}

void testDeleteReturnsBudget() {
    std::cout << "DEL gives its bytes back to the budget\n";

    const std::string value(64, 'x');
    Store store(LockMode::GlobalSharedMutex, 1, entryBytes("key:0", value) * 50);

    for (int i = 0; i < 10; ++i) {
        store.set("key:" + std::to_string(i), value);
    }
    const std::size_t before = store.bytesUsed();
    CHECK(before > 0);

    store.del("key:0");
    CHECK(store.bytesUsed() < before);

    const std::size_t after = store.bytesUsed();
    store.del("key:does-not-exist");
    CHECK(store.bytesUsed() == after);
}

void testStatsCommandAndLine() {
    std::cout << "STATS parses and reports\n";

    using kvstore::protocol::CommandParser;
    using kvstore::protocol::CommandType;

    CHECK(CommandParser::parse("STATS").type == CommandType::Stats);
    CHECK(CommandParser::parse("stats").type == CommandType::Stats);
    CHECK(CommandParser::parse("STATS extra").type == CommandType::Stats);

    Store store(LockMode::Sharded, 4, 0);
    std::string out;
    store.set("a", "1");
    store.get("a", out);
    store.get("zz", out);

    const std::string stats = store.statsLine();
    CHECK(stats.find("hits=1") != std::string::npos);
    CHECK(stats.find("misses=1") != std::string::npos);
    CHECK(stats.find("writes=1") != std::string::npos);
    CHECK(stats.find("evictions=0") != std::string::npos);
    CHECK(stats.find("keys=1") != std::string::npos);
    CHECK(stats.find("lock=sharded") != std::string::npos);

    CHECK(stats.find('\n') == std::string::npos);
}

void testConcurrentEviction() {
    std::cout << "eviction under concurrent load\n";

    const std::string value(64, 'x');
    Store store(LockMode::Sharded, 16, entryBytes("key:1000", value) * 200);

    constexpr std::size_t kThreads = 8;
    constexpr int kOps = 20000;

    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (std::size_t t = 0; t < kThreads; ++t) {
        threads.emplace_back([&store, &value, t] {
            std::string out;
            for (int i = 0; i < kOps; ++i) {
                const std::string key = "key:" + std::to_string((i * 7 + static_cast<int>(t)) % 4000);
                if (i % 3 == 0) {
                    store.set(key, value);
                } else {
                    store.get(key, out);
                }
            }
        });
    }
    for (std::thread& thread : threads) {
        thread.join();
    }

    CHECK(store.evictions() > 0);
    CHECK(store.size() <= 400);
    CHECK(store.bytesUsed() > 0);
    CHECK(store.bytesUsed() < store.maxMemoryBytes() * 2);
}
}

int main() {
    testPolicyOrdering();
    testPolicyAccessPromotes();
    testPolicyByteAccounting();
    testPolicyOverwriteIsAUse();
    testPolicyIteratorsSurviveChurn();
    testUnlimitedMeansNoPolicy();
    testEvictionKeepsStoreUnderLimit();
    testEvictionOrderIsLruWithOneShard();
    testDeleteReturnsBudget();
    testStatsCommandAndLine();
    testConcurrentEviction();

    std::cout << (g_failures == 0 ? "\nALL TESTS PASSED\n" : "\nFAILURES\n");
    return g_failures == 0 ? 0 : 1;
}
