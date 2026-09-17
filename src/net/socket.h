#pragma once

#include <cstdint>
#include <string>

namespace kvstore {
namespace net {

class FileDescriptor {
public:
    FileDescriptor() noexcept = default;

    explicit FileDescriptor(int fd) noexcept;

    ~FileDescriptor();

    FileDescriptor(const FileDescriptor&) = delete;
    FileDescriptor& operator=(const FileDescriptor&) = delete;

    FileDescriptor(FileDescriptor&& other) noexcept;
    FileDescriptor& operator=(FileDescriptor&& other) noexcept;

    int get() const noexcept { return fd_; }
    bool valid() const noexcept { return fd_ >= 0; }

    int release() noexcept;
    void reset() noexcept;

private:
    int fd_ = -1;
};

class ListeningSocket {
public:

    ListeningSocket(std::uint16_t port, int backlog);

    FileDescriptor accept();

    int fd() const noexcept { return fd_.get(); }

private:
    FileDescriptor fd_;
};

FileDescriptor connectTo(const std::string& host, std::uint16_t port);

bool setNoDelay(int fd);

bool setNonBlocking(int fd);
}
}
