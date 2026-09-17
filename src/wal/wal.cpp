#include "wal/wal.h"

#include <cerrno>
#include <chrono>
#include <cstring>
#include <stdexcept>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "wal/record.h"

namespace kvstore {
namespace wal {
namespace {

std::runtime_error syscallError(const char* what, int err) {
    return std::runtime_error(std::string(what) + " failed: " + std::strerror(err));
}

void writeAll(int fd, const char* data, std::size_t length) {
    std::size_t written = 0;
    while (written < length) {
        ssize_t n = ::write(fd, data + written, length - written);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw syscallError("write(log)", errno);
        }
        written += static_cast<std::size_t>(n);
    }
}
}

bool parseFsyncMode(const std::string& text, FsyncMode& out) {
    if (text == "always") {
        out = FsyncMode::Always;
    } else if (text == "interval") {
        out = FsyncMode::Interval;
    } else if (text == "never") {
        out = FsyncMode::Never;
    } else {
        return false;
    }
    return true;
}

const char* fsyncModeName(FsyncMode mode) {
    switch (mode) {
        case FsyncMode::Always:   return "always";
        case FsyncMode::Interval: return "interval";
        case FsyncMode::Never:    return "never";
    }
    return "unknown";
}

int syncFile(int fd) {
#if defined(__APPLE__)

    if (::fcntl(fd, F_FULLFSYNC) == 0) {
        return 0;
    }

    return ::fsync(fd);
#elif defined(__linux__)

    return ::fdatasync(fd);
#else
    return ::fsync(fd);
#endif
}

WriteAheadLog::WriteAheadLog(std::string path, FsyncMode mode, int intervalMs)
    : path_(std::move(path)), mode_(mode), intervalMs_(intervalMs) {
    int fd = ::open(path_.c_str(), O_WRONLY | O_APPEND | O_CREAT, 0644);
    if (fd < 0) {
        throw syscallError("open(log)", errno);
    }
    fd_ = net::FileDescriptor(fd);

    const off_t end = ::lseek(fd_.get(), 0, SEEK_END);
    if (end < 0) {
        throw syscallError("lseek(log)", errno);
    }
    offset_ = static_cast<std::uint64_t>(end);

    if (mode_ == FsyncMode::Interval) {
        if (intervalMs_ <= 0) {
            intervalMs_ = 1000;
        }

        syncThread_ = std::thread([this] { syncLoop(); });
    }
}

WriteAheadLog::~WriteAheadLog() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stopping_ = true;
    }
    stopCv_.notify_all();

    if (syncThread_.joinable()) {
        syncThread_.join();
    }

    try {
        std::lock_guard<std::mutex> lock(mutex_);
        if (fd_.valid()) {
            syncLocked();
        }
    } catch (...) {
    }
}

std::uint64_t WriteAheadLog::append(const std::string& payload) {
    const std::string framed = frame(payload);

    std::lock_guard<std::mutex> lock(mutex_);

    writeAll(fd_.get(), framed.data(), framed.size());

    offset_ += framed.size();

    appends_.fetch_add(1, std::memory_order_relaxed);
    bytesWritten_.fetch_add(framed.size(), std::memory_order_relaxed);

    if (mode_ == FsyncMode::Always) {
        syncLocked();
    }
    return offset_;
}

std::uint64_t WriteAheadLog::appendBatch(const std::vector<std::string>& payloads) {
    if (payloads.empty()) {
        return offset();
    }

    std::string blob;
    std::size_t totalSize = 0;
    for (const std::string& payload : payloads) {
        totalSize += kHeaderBytes + payload.size();
    }
    blob.reserve(totalSize);
    for (const std::string& payload : payloads) {
        blob += frame(payload);
    }

    std::lock_guard<std::mutex> lock(mutex_);

    writeAll(fd_.get(), blob.data(), blob.size());

    offset_ += blob.size();

    appends_.fetch_add(payloads.size(), std::memory_order_relaxed);
    bytesWritten_.fetch_add(blob.size(), std::memory_order_relaxed);

    if (mode_ == FsyncMode::Always) {
        syncLocked();
    }

    return offset_;
}

std::uint64_t WriteAheadLog::offset() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return offset_;
}

void WriteAheadLog::truncateAll() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (::ftruncate(fd_.get(), 0) != 0) {
        throw syscallError("ftruncate(log for resync)", errno);
    }

    if (syncFile(fd_.get()) != 0) {
        throw syscallError("fsync after resync truncate", errno);
    }
    offset_ = 0;
}

void WriteAheadLog::sync() {
    std::lock_guard<std::mutex> lock(mutex_);
    syncLocked();
}

void WriteAheadLog::syncLocked() {
    if (syncFile(fd_.get()) != 0) {
        throw syscallError("fsync(log)", errno);
    }
    syncs_.fetch_add(1, std::memory_order_relaxed);
}

void WriteAheadLog::syncLoop() {
    std::unique_lock<std::mutex> lock(mutex_);
    for (;;) {
        if (stopCv_.wait_for(lock, std::chrono::milliseconds(intervalMs_),
                             [this] { return stopping_; })) {
            return;
        }

        try {
            if (fd_.valid()) {
                syncLocked();
            }
        } catch (const std::exception&) {
        }
    }
}

void WriteAheadLog::reopenAfterCompaction() {
    std::lock_guard<std::mutex> lock(mutex_);

    fd_.reset();

    int fd = ::open(path_.c_str(), O_WRONLY | O_APPEND, 0644);
    if (fd < 0) {
        throw syscallError("reopen(log)", errno);
    }
    fd_ = net::FileDescriptor(fd);

    const off_t end = ::lseek(fd_.get(), 0, SEEK_END);
    if (end < 0) {
        throw syscallError("lseek after compaction", errno);
    }
    offset_ = static_cast<std::uint64_t>(end);
}

ReplayResult WriteAheadLog::replay(const std::string& path,
                                   const std::function<bool(const std::string&)>& onRecord) {
    ReplayResult result;

    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        if (errno == ENOENT) {
            return result;
        }
        throw syscallError("open(log for replay)", errno);
    }
    net::FileDescriptor reader(fd);

    std::string buffer;
    char chunk[64 * 1024];
    for (;;) {
        ssize_t n = ::read(reader.get(), chunk, sizeof(chunk));
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw syscallError("read(log)", errno);
        }
        if (n == 0) {
            break;
        }
        buffer.append(chunk, static_cast<std::size_t>(n));
    }
    result.fileBytes = buffer.size();

    std::size_t offset = 0;
    while (offset < buffer.size()) {
        std::string payload;
        std::size_t nextOffset = 0;
        const ParseResult parsed = parseRecord(buffer, offset, payload, nextOffset);

        if (parsed == ParseResult::Incomplete) {
            result.truncated = true;
            break;
        }
        if (parsed == ParseResult::Corrupt) {
            result.truncated = true;
            result.sawCorruption = true;
            break;
        }

        if (!onRecord(payload)) {
            result.sawUnknownRecord = true;
            break;
        }

        ++result.recordsApplied;
        offset = nextOffset;
    }
    result.bytesConsumed = offset;

    if (result.truncated || result.sawUnknownRecord) {
        int wfd = ::open(path.c_str(), O_WRONLY);
        if (wfd < 0) {
            throw syscallError("open(log for truncate)", errno);
        }
        net::FileDescriptor writer(wfd);
        if (::ftruncate(writer.get(), static_cast<off_t>(offset)) != 0) {
            throw syscallError("ftruncate(log)", errno);
        }

        if (syncFile(writer.get()) != 0) {
            throw syscallError("fsync after truncate", errno);
        }
    }

    return result;
}
}
}
