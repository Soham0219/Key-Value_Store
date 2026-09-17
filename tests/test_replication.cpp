#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include <sys/socket.h>
#include <unistd.h>

#include "net/connection.h"
#include "protocol/commands.h"
#include "repl/follower.h"
#include "repl/leader.h"
#include "store/record_applier.h"
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

using kvstore::repl::Follower;
using kvstore::repl::Leader;
using kvstore::repl::ReplConfig;
using kvstore::repl::ReplMode;
using kvstore::repl::Role;
using kvstore::store::Mutation;
using kvstore::store::RecordApplier;
using kvstore::store::Store;
using kvstore::store::StoreConfig;
using kvstore::wal::FsyncMode;

using Clock = std::chrono::steady_clock;

std::string tempPath(const char* label) {
    return "/tmp/kvstore_repl_" + std::string(label) + "_" + std::to_string(::getpid()) + ".log";
}

void removeLog(const std::string& path) {
    ::unlink(path.c_str());
    ::unlink((path + ".replstate").c_str());
}

std::uint16_t nextPort() {
    static std::uint16_t next = static_cast<std::uint16_t>(17000 + (::getpid() % 2000));
    return next++;
}

template <typename Predicate>
bool waitFor(Predicate predicate, int timeoutMs = 5000) {
    const Clock::time_point deadline = Clock::now() + std::chrono::milliseconds(timeoutMs);
    while (Clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return predicate();
}

StoreConfig logged(const std::string& path, bool readOnly = false) {
    StoreConfig config;
    config.logPath = path;

    config.fsyncMode = FsyncMode::Interval;
    config.fsyncIntervalMs = 50;
    config.readOnly = readOnly;
    return config;
}

std::string getOr(Store& store, const std::string& key, const char* fallback) {
    std::string value;
    return store.get(key, value) ? value : std::string(fallback);
}

void testRecordApplier() {
    std::cout << "RecordApplier: one state machine for recovery and replication\n";

    const std::string path = tempPath("applier");
    removeLog(path);
    {
        Store store(logged(path));
        RecordApplier applier(store);

        CHECK(applier.apply(kvstore::protocol::SetCommand("a", "1").serialize()));
        CHECK(getOr(store, "a", "<none>") == "1");

        CHECK(applier.apply(kvstore::protocol::serializeBeginMarker(7)));
        CHECK(applier.apply(kvstore::protocol::SetCommand("b", "2").serialize()));
        CHECK(applier.insideTransaction());
        CHECK(getOr(store, "b", "<none>") == "<none>");

        applier.abandonOpenTransaction();
        CHECK(!applier.insideTransaction());
        CHECK(getOr(store, "b", "<none>") == "<none>");

        CHECK(applier.apply(kvstore::protocol::serializeBeginMarker(8)));
        CHECK(applier.apply(kvstore::protocol::SetCommand("c", "3").serialize()));
        CHECK(applier.apply(kvstore::protocol::SetCommand("d", "4").serialize()));
        CHECK(getOr(store, "c", "<none>") == "<none>");
        CHECK(applier.apply(kvstore::protocol::serializeCommitMarker(8)));
        CHECK(getOr(store, "c", "<none>") == "3");
        CHECK(getOr(store, "d", "<none>") == "4");

        CHECK(!applier.apply(kvstore::protocol::serializeCommitMarker(99)));
    }
    removeLog(path);
}

void testFollowerCatchesUp() {
    std::cout << "Replication: a follower converges on the leader's state\n";

    const std::string leaderLog = tempPath("lead1");
    const std::string followerLog = tempPath("foll1");
    removeLog(leaderLog);
    removeLog(followerLog);

    const std::uint16_t port = nextPort();
    {
        Store leaderStore(logged(leaderLog));
        Store followerStore(logged(followerLog, true));

        leaderStore.set("before", "yes");

        ReplConfig leaderConfig;
        leaderConfig.role = Role::Leader;
        leaderConfig.listenPort = port;
        Leader leader(leaderStore, leaderConfig);
        leaderStore.setReplicator(&leader);
        leader.start();

        ReplConfig followerConfig;
        followerConfig.role = Role::Follower;
        followerConfig.peerPort = port;
        Follower follower(followerStore, followerConfig);
        followerStore.setReplicator(&follower);
        follower.start();

        CHECK(waitFor([&] { return follower.linkUp(); }));
        CHECK(waitFor([&] { return leader.followerCount() == 1; }));

        CHECK(waitFor([&] { return getOr(followerStore, "before", "") == "yes"; }));

        for (int i = 0; i < 200; ++i) {
            leaderStore.set("key" + std::to_string(i), "value" + std::to_string(i));
        }
        leaderStore.del("key7");

        CHECK(waitFor([&] { return followerStore.logOffset() == leaderStore.logOffset(); }));

        CHECK(followerStore.size() == leaderStore.size());
        CHECK(getOr(followerStore, "key0", "") == "value0");
        CHECK(getOr(followerStore, "key199", "") == "value199");
        CHECK(getOr(followerStore, "key7", "<gone>") == "<gone>");

        std::vector<Mutation> mutations;
        mutations.push_back(Mutation{false, "txn_a", "1"});
        mutations.push_back(Mutation{false, "txn_b", "2"});
        CHECK(leaderStore.commitTransaction(leaderStore.nextTransactionId(), mutations));
        CHECK(waitFor([&] { return followerStore.logOffset() == leaderStore.logOffset(); }));
        CHECK(getOr(followerStore, "txn_a", "") == "1");
        CHECK(getOr(followerStore, "txn_b", "") == "2");

        follower.stop();
        leader.stop();
    }
    removeLog(leaderLog);
    removeLog(followerLog);
}

void testResumeAfterReconnect() {
    std::cout << "Replication: a reconnecting follower RESUMES, it does not re-copy\n";

    const std::string leaderLog = tempPath("lead2");
    const std::string followerLog = tempPath("foll2");
    removeLog(leaderLog);
    removeLog(followerLog);

    const std::uint16_t port = nextPort();
    {
        Store leaderStore(logged(leaderLog));
        Store followerStore(logged(followerLog, true));

        ReplConfig leaderConfig;
        leaderConfig.role = Role::Leader;
        leaderConfig.listenPort = port;
        Leader leader(leaderStore, leaderConfig);
        leaderStore.setReplicator(&leader);
        leader.start();

        ReplConfig followerConfig;
        followerConfig.role = Role::Follower;
        followerConfig.peerPort = port;

        {
            Follower follower(followerStore, followerConfig);
            followerStore.setReplicator(&follower);
            follower.start();

            for (int i = 0; i < 50; ++i) {
                leaderStore.set("a" + std::to_string(i), "1");
            }
            CHECK(waitFor([&] { return followerStore.logOffset() == leaderStore.logOffset(); }));

            CHECK(leader.fullResyncs() == 1);
            follower.stop();
        }

        CHECK(waitFor([&] { return leader.followerCount() == 0; }));

        for (int i = 0; i < 50; ++i) {
            leaderStore.set("b" + std::to_string(i), "2");
        }
        const std::uint64_t offsetWhileDown = followerStore.logOffset();
        CHECK(offsetWhileDown < leaderStore.logOffset());

        {
            Follower follower(followerStore, followerConfig);
            followerStore.setReplicator(&follower);
            follower.start();

            CHECK(waitFor([&] { return followerStore.logOffset() == leaderStore.logOffset(); }));
            CHECK(getOr(followerStore, "b49", "") == "2");

            CHECK(leader.fullResyncs() == 1);
            CHECK(follower.fullResyncs() == 0);

            follower.stop();
        }
        leader.stop();
    }
    removeLog(leaderLog);
    removeLog(followerLog);
}

void testSyncWaitsForTheFollower() {
    std::cout << "Replication: sync mode waits for the follower's acknowledgement\n";

    const std::string leaderLog = tempPath("lead3");
    const std::string followerLog = tempPath("foll3");
    removeLog(leaderLog);
    removeLog(followerLog);

    const std::uint16_t port = nextPort();
    {
        Store leaderStore(logged(leaderLog));
        Store followerStore(logged(followerLog, true));

        ReplConfig leaderConfig;
        leaderConfig.role = Role::Leader;
        leaderConfig.listenPort = port;
        leaderConfig.mode = ReplMode::Sync;
        leaderConfig.ackTimeoutMs = 2000;
        Leader leader(leaderStore, leaderConfig);
        leaderStore.setReplicator(&leader);
        leader.start();

        ReplConfig followerConfig;
        followerConfig.role = Role::Follower;
        followerConfig.peerPort = port;
        Follower follower(followerStore, followerConfig);
        followerStore.setReplicator(&follower);
        follower.start();

        CHECK(waitFor([&] { return leader.followerCount() == 1; }));

        bool everyWriteWasReplicated = true;
        for (int i = 0; i < 50; ++i) {
            leaderStore.set("s" + std::to_string(i), "v");

            if (followerStore.logOffset() < leaderStore.logOffset()) {
                everyWriteWasReplicated = false;
                break;
            }
        }
        CHECK(everyWriteWasReplicated);
        CHECK(leader.ackTimeouts() == 0);

        follower.stop();
        leader.stop();
    }
    removeLog(leaderLog);
    removeLog(followerLog);
}

void testSyncWithNoFollower() {
    std::cout << "Replication: sync mode with no follower proceeds rather than blocking\n";

    const std::string leaderLog = tempPath("lead4");
    removeLog(leaderLog);

    const std::uint16_t port = nextPort();
    {
        Store leaderStore(logged(leaderLog));

        ReplConfig leaderConfig;
        leaderConfig.role = Role::Leader;
        leaderConfig.listenPort = port;
        leaderConfig.mode = ReplMode::Sync;
        leaderConfig.ackTimeoutMs = 3000;
        Leader leader(leaderStore, leaderConfig);
        leaderStore.setReplicator(&leader);
        leader.start();

        const Clock::time_point start = Clock::now();
        for (int i = 0; i < 20; ++i) {
            leaderStore.set("n" + std::to_string(i), "v");
        }
        const auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                   Clock::now() - start).count();

        CHECK(elapsedMs < 1000);
        CHECK(leaderStore.statsLine().find("sync_no_follower=20") != std::string::npos);

        leader.stop();
    }
    removeLog(leaderLog);
}

void testFollowerIsReadOnly() {
    std::cout << "Replication: a follower refuses client writes with -ERR READONLY\n";

    const std::string followerLog = tempPath("ro");
    removeLog(followerLog);
    {
        Store store(logged(followerLog, true));
        CHECK(store.readOnly());

        int fds[2] = {-1, -1};
        CHECK(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);

        std::atomic<bool> shuttingDown{false};

        kvstore::net::Connection connection(kvstore::net::FileDescriptor{fds[0]}, store,
                                            shuttingDown);
        std::thread worker([&connection] { connection.run(); });

        const std::string request = "SET k v\nDEL k\nCOMPACT\nGET k\nQUIT\n";
        const ssize_t written = ::write(fds[1], request.data(), request.size());
        CHECK(written == static_cast<ssize_t>(request.size()));

        std::string replies;
        char buffer[512];
        std::size_t lines = 0;
        while (lines < 5) {
            ssize_t n = ::read(fds[1], buffer, sizeof(buffer));
            if (n <= 0) {
                break;
            }
            for (ssize_t i = 0; i < n; ++i) {
                if (buffer[i] == '\n') {
                    ++lines;
                }
            }
            replies.append(buffer, static_cast<std::size_t>(n));
        }
        worker.join();
        ::close(fds[1]);

        CHECK(replies.find("-ERR READONLY this node is a replica") != std::string::npos);
        CHECK(replies.find("-ERR READONLY a replica must not compact") != std::string::npos);
        CHECK(replies.find("$nil\n") != std::string::npos);
        CHECK(store.size() == 0);
    }
    removeLog(followerLog);
}

void testFullResyncAfterCompaction() {
    std::cout << "Replication: compaction forces a full resync, and the follower recovers\n";

    const std::string leaderLog = tempPath("lead5");
    const std::string followerLog = tempPath("foll5");
    removeLog(leaderLog);
    removeLog(followerLog);

    const std::uint16_t port = nextPort();
    {
        Store leaderStore(logged(leaderLog));
        Store followerStore(logged(followerLog, true));

        ReplConfig leaderConfig;
        leaderConfig.role = Role::Leader;
        leaderConfig.listenPort = port;
        Leader leader(leaderStore, leaderConfig);
        leaderStore.setReplicator(&leader);
        leader.start();

        ReplConfig followerConfig;
        followerConfig.role = Role::Follower;
        followerConfig.peerPort = port;
        Follower follower(followerStore, followerConfig);
        followerStore.setReplicator(&follower);
        follower.start();

        for (int i = 0; i < 100; ++i) {
            leaderStore.set("hot", std::to_string(i));
        }
        leaderStore.set("doomed", "x");
        leaderStore.del("doomed");

        CHECK(waitFor([&] { return followerStore.logOffset() == leaderStore.logOffset(); }));
        CHECK(getOr(followerStore, "hot", "") == "99");

        const kvstore::wal::CompactionResult result = leaderStore.compact();
        CHECK(result.ok);
        CHECK(result.bytesAfter < result.bytesBefore);

        leaderStore.set("after", "compaction");

        CHECK(waitFor([&] { return follower.fullResyncs() >= 1; }, 8000));
        CHECK(waitFor([&] { return followerStore.logOffset() == leaderStore.logOffset(); }, 8000));

        CHECK(getOr(followerStore, "hot", "") == "99");
        CHECK(getOr(followerStore, "after", "") == "compaction");
        CHECK(getOr(followerStore, "doomed", "<gone>") == "<gone>");
        CHECK(followerStore.size() == leaderStore.size());

        follower.stop();
        leader.stop();
    }
    removeLog(leaderLog);
    removeLog(followerLog);
}

void measureSyncCost() {
    std::cout << "Replication: measuring the latency cost of sync mode\n";

    const int kWrites = 500;

    auto measure = [&](ReplMode mode) -> double {
        const std::string leaderLog = tempPath(mode == ReplMode::Sync ? "msync" : "masync");
        const std::string followerLog = tempPath(mode == ReplMode::Sync ? "fsync" : "fasync");
        removeLog(leaderLog);
        removeLog(followerLog);

        double microsPerWrite = 0.0;
        const std::uint16_t port = nextPort();
        {
            Store leaderStore(logged(leaderLog));
            Store followerStore(logged(followerLog, true));

            ReplConfig leaderConfig;
            leaderConfig.role = Role::Leader;
            leaderConfig.listenPort = port;
            leaderConfig.mode = mode;
            Leader leader(leaderStore, leaderConfig);
            leaderStore.setReplicator(&leader);
            leader.start();

            ReplConfig followerConfig;
            followerConfig.role = Role::Follower;
            followerConfig.peerPort = port;
            Follower follower(followerStore, followerConfig);
            followerStore.setReplicator(&follower);
            follower.start();

            waitFor([&] { return leader.followerCount() == 1; });

            std::vector<std::string> keys;
            keys.reserve(static_cast<std::size_t>(kWrites));
            for (int i = 0; i < kWrites; ++i) {
                keys.push_back("m" + std::to_string(i));
            }
            const std::string value(64, 'x');

            const Clock::time_point start = Clock::now();
            for (int i = 0; i < kWrites; ++i) {
                leaderStore.set(keys[static_cast<std::size_t>(i)], value);
            }
            const auto totalUs = std::chrono::duration_cast<std::chrono::microseconds>(
                                     Clock::now() - start).count();
            microsPerWrite = static_cast<double>(totalUs) / kWrites;

            if (mode == ReplMode::Async) {
                waitFor([&] { return followerStore.logOffset() == leaderStore.logOffset(); });

                waitFor([&] { return leader.ackedOffset() == leaderStore.logOffset(); });
            }
            std::cout << "    " << kvstore::repl::replModeName(mode) << ": "
                      << microsPerWrite << " us/write, stats: " << leader.statsLine() << '\n';

            follower.stop();
            leader.stop();
        }
        removeLog(leaderLog);
        removeLog(followerLog);
        return microsPerWrite;
    };

    const double asyncUs = measure(ReplMode::Async);
    const double syncUs = measure(ReplMode::Sync);

    std::cout << "    sync costs " << (syncUs - asyncUs) << " us more per write ("
              << (asyncUs > 0.0 ? syncUs / asyncUs : 0.0) << "x) over loopback\n";

    CHECK(syncUs > 0.0 && asyncUs > 0.0);
}
}

int main() {
    std::cout << "=== leader/follower replication ===\n";

    testRecordApplier();
    testFollowerCatchesUp();
    testResumeAfterReconnect();
    testSyncWaitsForTheFollower();
    testSyncWithNoFollower();
    testFollowerIsReadOnly();
    testFullResyncAfterCompaction();
    measureSyncCost();

    if (g_failures == 0) {
        std::cout << "ALL TESTS PASSED\n";
        return 0;
    }
    std::cout << g_failures << " CHECK(s) FAILED\n";
    return 1;
}
