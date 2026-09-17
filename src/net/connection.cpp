#include "net/connection.h"

#include <cerrno>
#include <utility>

#include <poll.h>
#include <unistd.h>

namespace kvstore {
namespace net {
namespace {

constexpr std::size_t kReadChunk = 4096;
}

Connection::Connection(FileDescriptor fd, store::Store& store, const std::atomic<bool>& shuttingDown)
    : fd_(std::move(fd)),
      shuttingDown_(shuttingDown),
      session_(store) {}

void Connection::run() {
    for (;;) {
        for (;;) {
            std::size_t newline = inbuf_.find('\n');
            if (newline == std::string::npos) {
                break;
            }

            std::string line = inbuf_.substr(0, newline);
            inbuf_.erase(0, newline + 1);

            if (!line.empty() && line.back() == '\r') {
                line.pop_back();
            }

            bool quit = false;
            std::string response = session_.handleLine(line, quit);
            if (!writeAll(response)) {
                return;
            }
            if (quit) {
                return;
            }
        }

        if (inbuf_.size() > kMaxLineLength) {
            writeAll("-ERR line too long\n");
            return;
        }

        if (shuttingDown_.load(std::memory_order_relaxed)) {
            writeAll("-ERR server shutting down\n");
            return;
        }

        ReadResult result = readMore();
        if (result == ReadResult::Closed || result == ReadResult::Error) {
            return;
        }
    }
}

Connection::ReadResult Connection::readMore() {
    pollfd pfd{};
    pfd.fd = fd_.get();
    pfd.events = POLLIN;

    int ready = ::poll(&pfd, 1, kPollTimeoutMs);
    if (ready < 0) {
        if (errno == EINTR) {
            return ReadResult::Timeout;
        }
        return ReadResult::Error;
    }
    if (ready == 0) {
        return ReadResult::Timeout;
    }

    char buffer[kReadChunk];

    ssize_t n = ::read(fd_.get(), buffer, sizeof(buffer));
    if (n < 0) {
        if (errno == EINTR) {
            return ReadResult::Timeout;
        }
        return ReadResult::Error;
    }
    if (n == 0) {
        return ReadResult::Closed;
    }

    inbuf_.append(buffer, static_cast<std::size_t>(n));
    return ReadResult::Data;
}

bool Connection::writeAll(const std::string& data) {
    std::size_t sent = 0;

    while (sent < data.size()) {
        ssize_t n = ::write(fd_.get(), data.data() + sent, data.size() - sent);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        sent += static_cast<std::size_t>(n);
    }
    return true;
}
}
}
