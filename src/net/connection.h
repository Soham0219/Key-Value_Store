#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <string>

#include "net/socket.h"
#include "protocol/session.h"
#include "store/store.h"

namespace kvstore {
namespace net {

class Connection {
public:

    Connection(FileDescriptor fd, store::Store& store, const std::atomic<bool>& shuttingDown);

    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    void run();

    static constexpr std::size_t kMaxLineLength = 64 * 1024;

private:

    enum class ReadResult {
        Data,
        Timeout,
        Closed,
        Error
    };

    ReadResult readMore();
    bool writeAll(const std::string& data);

    static constexpr int kPollTimeoutMs = 200;

    FileDescriptor fd_;
    const std::atomic<bool>& shuttingDown_;

    std::string inbuf_;

    protocol::Session session_;
};
}
}
