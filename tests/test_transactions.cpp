#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <stdexcept>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include <csignal>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "protocol/commands.h"
#include "protocol/parser.h"
#include "store/store.h"
#include "txn/transaction.h"
#include "wal/wal.h"

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

using kvstore::store::Store;
using kvstore::store::StoreConfig;
using kvstore::txn::Transaction;
using kvstore::wal::FsyncMode;

std::string tempLogPath(const char* label) {
    return "/tmp/kvstore_txn_" + std::string(label) + "_" + std::to_string(::getpid()) + ".log";
}

void removeFile(const std::string& path) {
    ::unlink(path.c_str());
}

void runInDoomedChild(const std::function<void()>& body) {
    const pid_t pid = ::fork();
    if (pid < 0) {
        std::cout << "  FAIL fork() failed\n";
        ++g_failures;
        return;
    }
    if (pid == 0) {
        body();
        ::raise(SIGKILL);
        ::_exit(1);
    }
    int status = 0;
    ::waitpid(pid, &status, 0);
}

void testWritesAreBuffered() {
    std::cout << "buffered writes are invisible until COMMIT\n";

    Store store;
    store.set("existing", "old");

    std::string value;
    {
        Transaction txn(store);
        txn.set("fresh", "new");
        txn.set("existing", "updated");
        txn.del("existing");

        CHECK(!store.get("fresh", value));
        CHECK(store.get("existing", value) && value == "old");
        CHECK(txn.pendingOperations() == 3);

        CHECK(txn.commit());
    }

    CHECK(store.get("fresh", value) && value == "new");
    CHECK(!store.get("existing", value));
    CHECK(store.transactionsCommitted() == 1);
}

void testReadYourOwnWrites() {
    std::cout << "a transaction sees its own uncommitted writes\n";

    Store store;
    store.set("k", "committed");

    Transaction txn(store);
    std::string value;

    CHECK(txn.get("k", value) && value == "committed");

    txn.set("k", "mine");

    CHECK(txn.get("k", value) && value == "mine");

    CHECK(store.get("k", value) && value == "committed");

    txn.del("k");
    CHECK(!txn.get("k", value));
    CHECK(store.get("k", value) && value == "committed");

    store.set("untouched", "x");
    CHECK(txn.get("untouched", value) && value == "x");

    txn.rollback();
}

void testOrderIsPreserved() {
    std::cout << "operations replay in order, last write wins\n";

    Store store;
    {
        Transaction txn(store);
        txn.set("k", "1");
        txn.set("k", "2");
        txn.del("k");
        txn.set("k", "3");
        CHECK(txn.pendingOperations() == 4);
        CHECK(txn.commit());
    }

    std::string value;
    CHECK(store.get("k", value) && value == "3");
    CHECK(store.size() == 1);
}

void testRollbackDiscards() {
    std::cout << "ROLLBACK leaves the store untouched\n";

    Store store;
    store.set("a", "original");

    {
        Transaction txn(store);
        txn.set("a", "changed");
        txn.set("b", "new");
        txn.rollback();

        CHECK(!txn.active());
        CHECK(txn.pendingOperations() == 0);
    }

    std::string value;
    CHECK(store.get("a", value) && value == "original");
    CHECK(!store.get("b", value));
    CHECK(store.size() == 1);
    CHECK(store.transactionsRolledBack() == 1);
    CHECK(store.transactionsCommitted() == 0);
}

void testDestructorRollsBack() {
    std::cout << "the DESTRUCTOR rolls back — the whole point of the class\n";

    Store store;
    store.set("a", "original");

    {
        Transaction txn(store);
        txn.set("a", "changed");
        txn.set("b", "new");
    }

    std::string value;
    CHECK(store.get("a", value) && value == "original");
    CHECK(!store.get("b", value));
    CHECK(store.transactionsRolledBack() == 1);

    try {
        Transaction txn(store);
        txn.set("a", "changed-by-doomed-code");
        throw std::runtime_error("something went wrong halfway through");
    } catch (const std::exception&) {
    }
    CHECK(store.get("a", value) && value == "original");
    CHECK(store.transactionsRolledBack() == 2);

    {
        Transaction txn(store);
        txn.set("c", "committed");
        CHECK(txn.commit());
    }
    CHECK(store.get("c", value) && value == "committed");
    CHECK(store.transactionsRolledBack() == 2);
}

void testDoubleCommitAndUseAfterFinish() {
    std::cout << "a finished transaction refuses further work\n";

    Store store;
    Transaction txn(store);
    txn.set("a", "1");
    CHECK(txn.commit());
    CHECK(!txn.active());

    CHECK(!txn.commit());

    txn.set("b", "2");
    CHECK(txn.pendingOperations() == 0);

    std::string value;
    CHECK(!store.get("b", value));

    txn.rollback();
    CHECK(store.transactionsRolledBack() == 0);
}

void testCrashMidTransaction() {
    std::cout << "kill -9 MID-TRANSACTION leaves zero partial state\n";
    const std::string path = tempLogPath("mid");
    removeFile(path);

    runInDoomedChild([&path] {
        StoreConfig config;
        config.logPath = path;
        config.fsyncMode = FsyncMode::Always;
        Store store(config);

        store.set("before", "durable");

        Transaction txn(store);
        txn.set("txn:a", "1");
        txn.set("txn:b", "2");
        txn.del("before");

    });

    StoreConfig config;
    config.logPath = path;
    Store recovered(config);

    std::string value;
    CHECK(recovered.get("before", value) && value == "durable");
    CHECK(!recovered.get("txn:a", value));
    CHECK(!recovered.get("txn:b", value));
    CHECK(recovered.size() == 1);
    CHECK(recovered.transactionsCommitted() == 0);

    removeFile(path);
}

void testCrashAfterCommit() {
    std::cout << "kill -9 AFTER commit keeps the whole transaction\n";
    const std::string path = tempLogPath("after");
    removeFile(path);

    runInDoomedChild([&path] {
        StoreConfig config;
        config.logPath = path;
        config.fsyncMode = FsyncMode::Always;
        Store store(config);

        Transaction txn(store);
        txn.set("a", "1");
        txn.set("b", "2");
        txn.set("c", "3");
        txn.commit();

    });

    StoreConfig config;
    config.logPath = path;
    Store recovered(config);

    std::string value;
    CHECK(recovered.get("a", value) && value == "1");
    CHECK(recovered.get("b", value) && value == "2");
    CHECK(recovered.get("c", value) && value == "3");
    CHECK(recovered.size() == 3);
    CHECK(recovered.transactionsCommitted() == 1);
    CHECK(!recovered.recoveryDiscardedTransaction());

    removeFile(path);
}

void testTruncatedTransactionIsDiscarded() {
    std::cout << "a transaction whose COMMIT marker never landed is discarded whole\n";
    const std::string path = tempLogPath("truncated");
    removeFile(path);

    {
        kvstore::wal::WriteAheadLog log(path, FsyncMode::Always, 0);
        log.append(kvstore::protocol::SetCommand("kept", "yes").serialize());

        log.append(kvstore::protocol::serializeBeginMarker(42));
        log.append(kvstore::protocol::SetCommand("lost:a", "1").serialize());
        log.append(kvstore::protocol::SetCommand("lost:b", "2").serialize());
    }

    StoreConfig config;
    config.logPath = path;
    Store store(config);

    std::string value;
    CHECK(store.get("kept", value) && value == "yes");
    CHECK(!store.get("lost:a", value));
    CHECK(!store.get("lost:b", value));
    CHECK(store.size() == 1);
    CHECK(store.recoveryDiscardedTransaction());

    CHECK(!store.recoveryResult().sawCorruption);

    removeFile(path);
}

void testCommitMarkerWithoutBeginStopsReplay() {
    std::cout << "a COMMIT with no BEGIN is refused rather than guessed at\n";
    const std::string path = tempLogPath("orphan");
    removeFile(path);

    {
        kvstore::wal::WriteAheadLog log(path, FsyncMode::Always, 0);
        log.append(kvstore::protocol::SetCommand("a", "1").serialize());
        log.append(kvstore::protocol::serializeCommitMarker(99));
        log.append(kvstore::protocol::SetCommand("b", "2").serialize());
    }

    StoreConfig config;
    config.logPath = path;
    Store store(config);

    std::string value;
    CHECK(store.get("a", value) && value == "1");
    CHECK(!store.get("b", value));
    CHECK(store.recoveryResult().sawUnknownRecord);

    removeFile(path);
}

void testTransactionSurvivesRestart() {
    std::cout << "committed transactions replay across a clean restart\n";
    const std::string path = tempLogPath("restart");
    removeFile(path);

    {
        StoreConfig config;
        config.logPath = path;
        config.fsyncMode = FsyncMode::Never;
        Store store(config);

        Transaction first(store);
        first.set("x", "1");
        first.set("y", "2");
        CHECK(first.commit());

        Transaction second(store);
        second.del("x");
        second.set("z", "3");
        CHECK(second.commit());

        Transaction third(store);
        third.set("never", "committed");
    }

    StoreConfig config;
    config.logPath = path;
    Store reopened(config);

    std::string value;
    CHECK(!reopened.get("x", value));
    CHECK(reopened.get("y", value) && value == "2");
    CHECK(reopened.get("z", value) && value == "3");
    CHECK(!reopened.get("never", value));
    CHECK(reopened.size() == 2);
    CHECK(reopened.transactionsCommitted() == 2);

    removeFile(path);
}

void testGroupCommitUsesOneSync() {
    std::cout << "a transaction of N writes costs ONE fsync\n";
    const std::string path = tempLogPath("groupsync");
    removeFile(path);

    StoreConfig config;
    config.logPath = path;
    config.fsyncMode = FsyncMode::Always;
    Store store(config);

    for (int i = 0; i < 10; ++i) {
        store.set("plain:" + std::to_string(i), "v");
    }
    CHECK(store.log()->syncs() == 10);

    const std::uint64_t syncsBefore = store.log()->syncs();
    {
        Transaction txn(store);
        for (int i = 0; i < 10; ++i) {
            txn.set("txn:" + std::to_string(i), "v");
        }
        CHECK(txn.commit());
    }

    CHECK(store.log()->syncs() == syncsBefore + 1);
    CHECK(store.log()->appends() == 10 + 12);

    removeFile(path);
}

void testConcurrentReaderNeverSeesPartialCommit() {
    std::cout << "a concurrent reader never observes half a transaction\n";

    Store store(kvstore::store::LockMode::Sharded, 16);
    {
        Transaction seed(store);
        seed.set("alpha", "0");
        seed.set("bravo", "0");
        seed.set("charlie", "0");
        seed.commit();
    }

    const std::vector<std::string> keys = {"alpha", "bravo", "charlie"};

    std::atomic<bool> stop{false};
    std::atomic<std::uint64_t> mismatches{0};
    std::atomic<std::uint64_t> readsPerformed{0};

    std::thread writer([&store, &stop] {
        for (int generation = 1; !stop.load(std::memory_order_relaxed); ++generation) {
            Transaction txn(store);
            const std::string value = std::to_string(generation);
            txn.set("alpha", value);
            txn.set("bravo", value);
            txn.set("charlie", value);
            txn.commit();
        }
    });

    std::thread reader([&store, &keys, &stop, &mismatches, &readsPerformed] {
        for (int i = 0; i < 100000 && !stop.load(std::memory_order_relaxed); ++i) {
            const std::vector<std::pair<bool, std::string>> values = store.getMany(keys);
            if (values[0].first && values[1].first && values[2].first) {
                if (values[0].second != values[1].second ||
                    values[1].second != values[2].second) {
                    mismatches.fetch_add(1, std::memory_order_relaxed);
                }
                readsPerformed.fetch_add(1, std::memory_order_relaxed);
            }
        }
    });

    reader.join();
    stop.store(true, std::memory_order_relaxed);
    writer.join();

    CHECK(readsPerformed.load() > 1000);
    CHECK(mismatches.load() == 0);
}

void testGetManyIsConsistent() {
    std::cout << "getMany reports presence per key and counts hits and misses\n";

    Store store;
    store.set("present", "yes");

    const std::vector<std::pair<bool, std::string>> values =
        store.getMany({"present", "absent", "present"});

    CHECK(values.size() == 3);
    CHECK(values[0].first && values[0].second == "yes");
    CHECK(!values[1].first);
    CHECK(values[2].first && values[2].second == "yes");
    CHECK(store.hits() == 2);
    CHECK(store.misses() == 1);

    CHECK(store.getMany({}).empty());
}

void testProtocolVerbs() {
    std::cout << "BEGIN / COMMIT / ROLLBACK parse\n";
    using kvstore::protocol::CommandParser;
    using kvstore::protocol::CommandType;

    CHECK(CommandParser::parse("BEGIN").type == CommandType::Begin);
    CHECK(CommandParser::parse("begin").type == CommandType::Begin);
    CHECK(CommandParser::parse("COMMIT").type == CommandType::Commit);
    CHECK(CommandParser::parse("ROLLBACK").type == CommandType::Rollback);
    CHECK(CommandParser::parse("rollback").type == CommandType::Rollback);

    std::uint64_t id = 0;
    CHECK(kvstore::protocol::parseBeginMarker(kvstore::protocol::serializeBeginMarker(7), id));
    CHECK(id == 7);
    CHECK(kvstore::protocol::parseCommitMarker(
        kvstore::protocol::serializeCommitMarker(0xFFEEDDCCBBAA9988ull), id));
    CHECK(id == 0xFFEEDDCCBBAA9988ull);

    CHECK(!kvstore::protocol::parseCommitMarker(kvstore::protocol::serializeBeginMarker(1), id));
    CHECK(!kvstore::protocol::parseBeginMarker("b", id));
    CHECK(!kvstore::protocol::parseBeginMarker("b123456789", id));
}
}

int main() {
    testCrashMidTransaction();
    testCrashAfterCommit();

    testWritesAreBuffered();
    testReadYourOwnWrites();
    testOrderIsPreserved();
    testRollbackDiscards();
    testDestructorRollsBack();
    testDoubleCommitAndUseAfterFinish();
    testTruncatedTransactionIsDiscarded();
    testCommitMarkerWithoutBeginStopsReplay();
    testTransactionSurvivesRestart();
    testGroupCommitUsesOneSync();
    testProtocolVerbs();
    testGetManyIsConsistent();

    testConcurrentReaderNeverSeesPartialCommit();

    std::cout << (g_failures == 0 ? "\nALL TESTS PASSED\n" : "\nFAILURES\n");
    return g_failures == 0 ? 0 : 1;
}
