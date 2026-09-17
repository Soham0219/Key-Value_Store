#include "socket.h"

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

namespace kvstore {
namespace net {
namespace {

std::runtime_error syscallError(const char* what, int err) {
    return std::runtime_error(std::string(what) + " failed: " + std::strerror(err));
}
}

FileDescriptor::FileDescriptor(int fd) noexcept : fd_(fd) {}

FileDescriptor::~FileDescriptor() {
    reset();
}

FileDescriptor::FileDescriptor(FileDescriptor&& other) noexcept : fd_(other.fd_) {
    other.fd_ = -1;
}

FileDescriptor& FileDescriptor::operator=(FileDescriptor&& other) noexcept {
    if (this != &other) {
        reset();
        fd_ = other.fd_;
        other.fd_ = -1;
    }
    return *this;
}

int FileDescriptor::release() noexcept {
    int fd = fd_;
    fd_ = -1;
    return fd;
}

void FileDescriptor::reset() noexcept {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
}

ListeningSocket::ListeningSocket(std::uint16_t port, int backlog) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        throw syscallError("socket", errno);
    }
    fd_ = FileDescriptor(fd);

    int reuse = 1;

    if (::setsockopt(fd_.get(), SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        throw syscallError("setsockopt(SO_REUSEADDR)", errno);
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;

    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (::bind(fd_.get(), reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) < 0) {
        throw syscallError("bind", errno);
    }

    if (::listen(fd_.get(), backlog) < 0) {
        throw syscallError("listen", errno);
    }
}

FileDescriptor ListeningSocket::accept() {
    int client = ::accept(fd_.get(), nullptr, nullptr);
    if (client < 0) {
        if (errno == EINTR) {
            return FileDescriptor{};
        }
        if (errno == ECONNABORTED) {
            return FileDescriptor{};
        }

        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return FileDescriptor{};
        }
        throw syscallError("accept", errno);
    }
    return FileDescriptor(client);
}

bool setNoDelay(int fd) {
    int one = 1;

    return ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) == 0;
}

bool setNonBlocking(int fd) {
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        return false;
    }
    return ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

FileDescriptor connectTo(const std::string& host, std::uint16_t port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return FileDescriptor{};
    }
    FileDescriptor owned(fd);

    setNoDelay(owned.get());

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
        return FileDescriptor{};
    }

    if (::connect(owned.get(), reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) < 0) {
        return FileDescriptor{};
    }
    return owned;
}
}
}
