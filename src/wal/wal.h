#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "net/socket.h"

namespace kvstore {
namespace wal {

enum class FsyncMode {
    Always,
    Interval,
    Never
};

bool parseFsyncMode(const std::string& text, FsyncMode& out);
const char* fsyncModeName(FsyncMode mode);

int syncFile(int fd);

struct ReplayResult {
    std::uint64_t recordsApplied = 0;
    std::uint64_t bytesConsumed = 0;
    std::uint64_t fileBytes = 0;
    bool truncated = false;
    bool sawCorruption = false;
    bool sawUnknownRecord = false;
};

class WriteAheadLog {
public:

    WriteAheadLog(std::string path, FsyncMode mode, int intervalMs);

    ~WriteAheadLog();

    WriteAheadLog(const WriteAheadLog&) = delete;
    WriteAheadLog& operator=(const WriteAheadLog&) = delete;

    std::uint64_t append(const std::string& payload);

    std::uint64_t appendBatch(const std::vector<std::string>& payloads);

    void sync();

    void reopenAfterCompaction();

    std::uint64_t offset() const;

    void truncateAll();

    const std::string& path() const noexcept { return path_; }
    FsyncMode mode() const noexcept { return mode_; }

    std::uint64_t appends() const noexcept { return appends_.load(std::memory_order_relaxed); }
    std::uint64_t syncs() const noexcept { return syncs_.load(std::memory_order_relaxed); }
    std::uint64_t bytesWritten() const noexcept {
        return bytesWritten_.load(std::memory_order_relaxed);
    }

    static ReplayResult replay(const std::string& path,
                               const std::function<bool(const std::string&)>& onRecord);

private:
    void syncLocked();
    void syncLoop();

    std::string path_;
    FsyncMode mode_;
    int intervalMs_;

    net::FileDescriptor fd_;

    mutable std::mutex mutex_;

    std::condition_variable stopCv_;
    bool stopping_ = false;
    std::thread syncThread_;

    std::uint64_t offset_ = 0;

    std::atomic<std::uint64_t> appends_{0};
    std::atomic<std::uint64_t> syncs_{0};
    std::atomic<std::uint64_t> bytesWritten_{0};
};
}
}
