#include "wal/compactor.h"

#include <cerrno>
#include <cstring>
#include <string>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "net/socket.h"
#include "protocol/commands.h"
#include "wal/record.h"
#include "wal/wal.h"

namespace kvstore {
namespace wal {
namespace {

std::string directoryOf(const std::string& path) {
    const std::size_t slash = path.find_last_of('/');
    if (slash == std::string::npos) {
        return ".";
    }
    if (slash == 0) {
        return "/";
    }
    return path.substr(0, slash);
}

bool writeAll(int fd, const char* data, std::size_t length, std::string& error) {
    std::size_t written = 0;
    while (written < length) {
        ssize_t n = ::write(fd, data + written, length - written);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            error = std::string("write(compact) failed: ") + std::strerror(errno);
            return false;
        }
        written += static_cast<std::size_t>(n);
    }
    return true;
}

std::uint64_t fileSize(const std::string& path) {
    struct stat st {};
    if (::stat(path.c_str(), &st) != 0) {
        return 0;
    }
    return static_cast<std::uint64_t>(st.st_size);
}
}

CompactionResult Compactor::rewrite(
        const std::string& path,
        const std::vector<std::pair<std::string, std::string>>& entries) {
    CompactionResult result;
    result.bytesBefore = fileSize(path);

    const std::string tempPath = path + kTempSuffix;

    int fd = ::open(tempPath.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        result.error = std::string("open(compact) failed: ") + std::strerror(errno);
        return result;
    }
    net::FileDescriptor temp(fd);

    for (const auto& entry : entries) {
        protocol::SetCommand command(entry.first, entry.second);
        const std::string framed = frame(command.serialize());
        if (!writeAll(temp.get(), framed.data(), framed.size(), result.error)) {
            ::unlink(tempPath.c_str());
            return result;
        }
        ++result.recordsWritten;
    }

    if (syncFile(temp.get()) != 0) {
        result.error = std::string("fsync(compact) failed: ") + std::strerror(errno);
        ::unlink(tempPath.c_str());
        return result;
    }
    temp.reset();

    if (::rename(tempPath.c_str(), path.c_str()) != 0) {
        result.error = std::string("rename(compact) failed: ") + std::strerror(errno);
        ::unlink(tempPath.c_str());
        return result;
    }

    const std::string dir = directoryOf(path);
    int dirFd = ::open(dir.c_str(), O_RDONLY);
    if (dirFd >= 0) {
        net::FileDescriptor dirHandle(dirFd);

        (void)::fsync(dirHandle.get());
    }

    result.bytesAfter = fileSize(path);
    result.ok = true;
    return result;
}
}
}
