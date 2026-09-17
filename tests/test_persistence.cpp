#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iostream>
#include <string>
#include <vector>

#include <csignal>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "protocol/command.h"
#include "protocol/commands.h"
#include "store/store.h"
#include "wal/compactor.h"
#include "wal/record.h"
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
using kvstore::wal::FsyncMode;
using kvstore::wal::ReplayResult;
using kvstore::wal::WriteAheadLog;

std::string tempLogPath(const char* label) {
    return "/tmp/kvstore_test_" + std::string(label) + "_" + std::to_string(::getpid()) + ".log";
}

void removeFile(const std::string& path) {
    ::unlink(path.c_str());
}

long fileSize(const std::string& path) {
    struct stat st {};
    if (::stat(path.c_str(), &st) != 0) {
        return -1;
    }
    return static_cast<long>(st.st_size);
}

void appendRaw(const std::string& path, const std::string& bytes) {
    int fd = ::open(path.c_str(), O_WRONLY | O_APPEND | O_CREAT, 0644);
    if (fd < 0) {
        return;
    }
    ssize_t written = ::write(fd, bytes.data(), bytes.size());
    (void)written;
    ::close(fd);
}

void flipByte(const std::string& path, long offset) {
    int fd = ::open(path.c_str(), O_RDWR);
    if (fd < 0) {
        return;
    }
    unsigned char byte = 0;
    if (::pread(fd, &byte, 1, offset) == 1) {
        byte ^= 0x01;
        ssize_t n = ::pwrite(fd, &byte, 1, offset);
        (void)n;
    }
    ::close(fd);
}

std::vector<std::string> replayPayloads(const std::string& path, ReplayResult& result) {
    std::vector<std::string> payloads;
    result = WriteAheadLog::replay(path, [&payloads](const std::string& payload) {
        payloads.push_back(payload);
        return true;
    });
    return payloads;
}

void testCrc32() {
    std::cout << "CRC-32\n";

    const std::string checkVector = "123456789";
    CHECK(kvstore::wal::crc32(checkVector.data(), checkVector.size()) == 0xCBF43926u);

    const std::string a = "hello world";
    const std::string b = "hello xorld";
    CHECK(kvstore::wal::crc32(a.data(), a.size()) != kvstore::wal::crc32(b.data(), b.size()));

    const std::string z1 = std::string(1, '\0') + "x";
    const std::string z2 = std::string(2, '\0') + "x";
    CHECK(kvstore::wal::crc32(z1.data(), z1.size()) != kvstore::wal::crc32(z2.data(), z2.size()));
}

void testFraming() {
    std::cout << "record framing\n";
    using kvstore::wal::ParseResult;

    const std::string payload = "hello";
    const std::string framed = kvstore::wal::frame(payload);
    CHECK(framed.size() == kvstore::wal::kHeaderBytes + payload.size());

    std::string out;
    std::size_t next = 0;
    CHECK(kvstore::wal::parseRecord(framed, 0, out, next) == ParseResult::Ok);
    CHECK(out == payload);
    CHECK(next == framed.size());

    const std::string binary = std::string("a\0b\nc", 5);
    const std::string binaryFramed = kvstore::wal::frame(binary);
    CHECK(kvstore::wal::parseRecord(binaryFramed, 0, out, next) == ParseResult::Ok);
    CHECK(out == binary);

    bool allIncomplete = true;
    for (std::size_t cut = 0; cut < framed.size(); ++cut) {
        allIncomplete &= (kvstore::wal::parseRecord(framed.substr(0, cut), 0, out, next) ==
                          ParseResult::Incomplete);
    }
    CHECK(allIncomplete);

    std::string damaged = framed;
    damaged[kvstore::wal::kHeaderBytes] ^= 0x01;
    CHECK(kvstore::wal::parseRecord(damaged, 0, out, next) == ParseResult::Corrupt);

    std::string absurd = framed;
    absurd[0] = static_cast<char>(0xFF);
    absurd[1] = static_cast<char>(0xFF);
    absurd[2] = static_cast<char>(0xFF);
    absurd[3] = static_cast<char>(0xFF);
    CHECK(kvstore::wal::parseRecord(absurd, 0, out, next) == ParseResult::Corrupt);
}

void testCommandSerialization() {
    std::cout << "Command serialize / deserialize\n";
    using kvstore::protocol::deserializeCommand;
    using kvstore::protocol::DelCommand;
    using kvstore::protocol::SetCommand;

    const SetCommand set("user:42", "Rahul");
    auto roundTripped = deserializeCommand(set.serialize());
    CHECK(roundTripped != nullptr);
    CHECK(roundTripped->tag() == 's');
    CHECK(static_cast<SetCommand*>(roundTripped.get())->key() == "user:42");
    CHECK(static_cast<SetCommand*>(roundTripped.get())->value() == "Rahul");

    const DelCommand del("user:42");
    auto delRound = deserializeCommand(del.serialize());
    CHECK(delRound != nullptr);
    CHECK(delRound->tag() == 'd');
    CHECK(static_cast<DelCommand*>(delRound.get())->key() == "user:42");

    const std::string nasty = std::string("a b\nc\0d", 7);
    const SetCommand binary("k", nasty);
    auto binaryRound = deserializeCommand(binary.serialize());
    CHECK(binaryRound != nullptr);
    CHECK(static_cast<SetCommand*>(binaryRound.get())->value() == nasty);

    CHECK(deserializeCommand("") == nullptr);
    CHECK(deserializeCommand("z\x01\x00\x00\x00k") == nullptr);
    CHECK(deserializeCommand("s\xFF\xFF\xFF\xFF") == nullptr);
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

void testCrashRecoveryWithFsyncAlways() {
    std::cout << "kill -9 with fsync=always, then replay\n";
    const std::string path = tempLogPath("crash_always");
    removeFile(path);

    constexpr int kKeys = 200;

    runInDoomedChild([&path] {
        StoreConfig config;
        config.logPath = path;
        config.fsyncMode = FsyncMode::Always;
        Store store(config);
        for (int i = 0; i < kKeys; ++i) {
            store.set("key:" + std::to_string(i), "value:" + std::to_string(i));
        }

    });

    StoreConfig config;
    config.logPath = path;
    Store recovered(config);

    CHECK(recovered.recoveryResult().recordsApplied == kKeys);
    CHECK(recovered.size() == kKeys);

    std::string value;
    bool allPresent = true;
    for (int i = 0; i < kKeys; ++i) {
        allPresent &= recovered.get("key:" + std::to_string(i), value) &&
                      value == "value:" + std::to_string(i);
    }
    CHECK(allPresent);
    CHECK(!recovered.recoveryResult().sawCorruption);

    removeFile(path);
}

void testCrashRecoveryWithoutFsync() {
    std::cout << "kill -9 with fsync=never — survives anyway (page cache is the KERNEL's)\n";
    const std::string path = tempLogPath("crash_never");
    removeFile(path);

    constexpr int kKeys = 200;

    runInDoomedChild([&path] {
        StoreConfig config;
        config.logPath = path;
        config.fsyncMode = FsyncMode::Never;
        Store store(config);
        for (int i = 0; i < kKeys; ++i) {
            store.set("key:" + std::to_string(i), "value:" + std::to_string(i));
        }
    });

    StoreConfig config;
    config.logPath = path;
    Store recovered(config);

    CHECK(recovered.recoveryResult().recordsApplied == kKeys);
    CHECK(recovered.size() == kKeys);

    removeFile(path);
}

void testCrashMidTransactionLeavesPrefix() {
    std::cout << "kill -9 mid-write leaves a valid PREFIX of history\n";
    const std::string path = tempLogPath("crash_prefix");
    removeFile(path);

    runInDoomedChild([&path] {
        StoreConfig config;
        config.logPath = path;
        config.fsyncMode = FsyncMode::Always;
        Store store(config);
        store.set("a", "1");
        store.set("b", "2");
        store.set("a", "3");
        store.del("b");
        store.set("c", "4");
    });

    StoreConfig config;
    config.logPath = path;
    Store recovered(config);

    std::string value;
    CHECK(recovered.get("a", value) && value == "3");
    CHECK(!recovered.get("b", value));
    CHECK(recovered.get("c", value) && value == "4");
    CHECK(recovered.size() == 2);

    removeFile(path);
}

void testTornTailIsTruncated() {
    std::cout << "torn tail is discarded and truncated away\n";
    const std::string path = tempLogPath("torn");
    removeFile(path);

    {
        WriteAheadLog log(path, FsyncMode::Always, 0);
        log.append("one");
        log.append("two");
        log.append("three");
    }
    const long goodSize = fileSize(path);

    const std::string fullFrame = kvstore::wal::frame("four");
    appendRaw(path, fullFrame.substr(0, fullFrame.size() - 2));
    CHECK(fileSize(path) > goodSize);

    ReplayResult result;
    const std::vector<std::string> payloads = replayPayloads(path, result);

    CHECK(payloads.size() == 3);
    CHECK(payloads[0] == "one" && payloads[1] == "two" && payloads[2] == "three");
    CHECK(result.truncated);
    CHECK(!result.sawCorruption);

    CHECK(fileSize(path) == goodSize);

    {
        WriteAheadLog log(path, FsyncMode::Always, 0);
        log.append("four");
    }
    ReplayResult second;
    const std::vector<std::string> after = replayPayloads(path, second);
    CHECK(after.size() == 4);
    CHECK(after[3] == "four");
    CHECK(!second.truncated);

    removeFile(path);
}

void testChecksumCatchesCorruption() {
    std::cout << "a flipped bit is caught by the checksum\n";
    const std::string path = tempLogPath("corrupt");
    removeFile(path);

    {
        WriteAheadLog log(path, FsyncMode::Always, 0);
        log.append("first");
        log.append("second");
        log.append("third");
    }

    const long secondBodyOffset =
        static_cast<long>(kvstore::wal::kHeaderBytes + 5 + kvstore::wal::kHeaderBytes);
    flipByte(path, secondBodyOffset);

    ReplayResult result;
    const std::vector<std::string> payloads = replayPayloads(path, result);

    CHECK(payloads.size() == 1);
    CHECK(payloads[0] == "first");
    CHECK(result.truncated);
    CHECK(result.sawCorruption);

    CHECK(fileSize(path) == static_cast<long>(kvstore::wal::kHeaderBytes + 5));

    removeFile(path);
}

void testUnknownRecordStopsReplay() {
    std::cout << "an unrecognised record stops replay without being mistaken for corruption\n";
    const std::string path = tempLogPath("unknown");
    removeFile(path);

    {
        WriteAheadLog log(path, FsyncMode::Always, 0);
        log.append(kvstore::protocol::SetCommand("a", "1").serialize());
        log.append("zNOT-A-COMMAND");
        log.append(kvstore::protocol::SetCommand("b", "2").serialize());
    }

    StoreConfig config;
    config.logPath = path;
    Store store(config);

    std::string value;
    CHECK(store.get("a", value) && value == "1");
    CHECK(!store.get("b", value));
    CHECK(store.recoveryResult().sawUnknownRecord);
    CHECK(!store.recoveryResult().sawCorruption);

    removeFile(path);
}

void testStoreRoundTrip() {
    std::cout << "store survives a clean restart\n";
    const std::string path = tempLogPath("roundtrip");
    removeFile(path);

    {
        StoreConfig config;
        config.logPath = path;
        config.fsyncMode = FsyncMode::Never;
        Store store(config);
        store.set("name", "rahul");
        store.set("city", "jaipur");
        store.set("name", "soham");
        store.del("city");
    }

    StoreConfig config;
    config.logPath = path;
    Store reopened(config);

    std::string value;
    CHECK(reopened.get("name", value) && value == "soham");
    CHECK(!reopened.get("city", value));
    CHECK(reopened.size() == 1);
    CHECK(reopened.recoveryResult().recordsApplied == 4);

    removeFile(path);
}

void testFsyncModes() {
    std::cout << "fsync modes issue the syncs they promise\n";

    {
        const std::string path = tempLogPath("fsync_always");
        removeFile(path);
        WriteAheadLog log(path, FsyncMode::Always, 0);
        for (int i = 0; i < 20; ++i) {
            log.append("record");
        }
        CHECK(log.appends() == 20);
        CHECK(log.syncs() == 20);
        removeFile(path);
    }

    {
        const std::string path = tempLogPath("fsync_never");
        removeFile(path);
        WriteAheadLog log(path, FsyncMode::Never, 0);
        for (int i = 0; i < 20; ++i) {
            log.append("record");
        }
        CHECK(log.appends() == 20);
        CHECK(log.syncs() == 0);
        removeFile(path);
    }

    {
        const std::string path = tempLogPath("fsync_interval");
        removeFile(path);
        WriteAheadLog log(path, FsyncMode::Interval, 20);
        for (int i = 0; i < 500; ++i) {
            log.append("record");
        }
        ::usleep(120 * 1000);

        CHECK(log.appends() == 500);

        CHECK(log.syncs() > 0);
        CHECK(log.syncs() < 500);
        removeFile(path);
    }
}

void testCompaction() {
    std::cout << "compaction shrinks the log without changing the state\n";
    const std::string path = tempLogPath("compact");
    removeFile(path);

    StoreConfig config;
    config.logPath = path;
    config.fsyncMode = FsyncMode::Never;

    long beforeSize = 0;
    {
        Store store(config);

        for (int round = 0; round < 500; ++round) {
            for (int k = 0; k < 10; ++k) {
                store.set("key:" + std::to_string(k), "round:" + std::to_string(round));
            }
        }
        beforeSize = fileSize(path);
        CHECK(store.size() == 10);

        const kvstore::wal::CompactionResult result = store.compact();
        CHECK(result.ok);
        CHECK(result.recordsWritten == 10);
        CHECK(result.bytesAfter < result.bytesBefore);
        CHECK(fileSize(path) < beforeSize);

        CHECK(store.size() == 10);
        store.set("after", "compaction");
        CHECK(store.size() == 11);
    }

    Store reopened(config);
    std::string value;
    CHECK(reopened.size() == 11);
    CHECK(reopened.get("key:0", value) && value == "round:499");
    CHECK(reopened.get("after", value) && value == "compaction");
    CHECK(reopened.recoveryResult().recordsApplied == 11);

    removeFile(path);
}

void testCompactionWithDeletes() {
    std::cout << "compaction drops deleted keys entirely\n";
    const std::string path = tempLogPath("compact_del");
    removeFile(path);

    StoreConfig config;
    config.logPath = path;
    config.fsyncMode = FsyncMode::Never;

    {
        Store store(config);
        for (int i = 0; i < 100; ++i) {
            store.set("key:" + std::to_string(i), "v");
        }
        for (int i = 0; i < 90; ++i) {
            store.del("key:" + std::to_string(i));
        }
        CHECK(store.size() == 10);

        const kvstore::wal::CompactionResult result = store.compact();
        CHECK(result.ok);

        CHECK(result.recordsWritten == 10);
    }

    Store reopened(config);
    std::string value;
    CHECK(reopened.size() == 10);
    CHECK(!reopened.get("key:0", value));
    CHECK(reopened.get("key:99", value));

    removeFile(path);
}

void testNoLogMeansNoDurability() {
    std::cout << "without --log the store is purely in-memory\n";

    Store store;
    CHECK(!store.logEnabled());
    store.set("a", "1");
    CHECK(store.size() == 1);
    CHECK(store.statsLine().find("log=off") != std::string::npos);
}
}

int main() {
    testCrc32();
    testFraming();
    testCommandSerialization();

    testCrashRecoveryWithFsyncAlways();
    testCrashRecoveryWithoutFsync();
    testCrashMidTransactionLeavesPrefix();

    testTornTailIsTruncated();
    testChecksumCatchesCorruption();
    testUnknownRecordStopsReplay();

    testStoreRoundTrip();
    testFsyncModes();
    testCompaction();
    testCompactionWithDeletes();
    testNoLogMeansNoDurability();

    std::cout << (g_failures == 0 ? "\nALL TESTS PASSED\n" : "\nFAILURES\n");
    return g_failures == 0 ? 0 : 1;
}
