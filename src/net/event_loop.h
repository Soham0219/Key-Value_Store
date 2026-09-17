#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "net/poller.h"
#include "net/socket.h"
#include "protocol/session.h"
#include "store/store.h"

namespace kvstore {
namespace net {

class EventLoop {
public:

    EventLoop(ListeningSocket& listener,
              store::Store& store,
              const std::atomic<bool>& shuttingDown,
              PollerKind pollerKind);

    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    void run();

    const char* pollerName() const { return poller_->name(); }

    std::size_t connectionCount() const { return clients_.size(); }
    std::uint64_t accepted() const noexcept { return accepted_; }
    std::uint64_t commandsHandled() const noexcept { return commands_; }
    std::uint64_t shortWrites() const noexcept { return shortWrites_; }
    std::uint64_t evictedForBacklog() const noexcept { return evictedForBacklog_; }

    static constexpr int kWaitTimeoutMs = 200;

    static constexpr std::size_t kReadChunk = 16 * 1024;

    static constexpr std::size_t kMaxAcceptsPerEvent = 64;

    static constexpr std::size_t kMaxOutputBytes = 8 * 1024 * 1024;

    static constexpr std::size_t kPauseReadingAboveBytes = 1 * 1024 * 1024;

private:

    struct Client {
        explicit Client(FileDescriptor socket, store::Store& store)
            : fd(std::move(socket)), session(store) {}

        FileDescriptor fd;
        protocol::Session session;

        std::string inbuf;
        std::string outbuf;

        std::size_t outSent = 0;

        bool wantsRead = true;
        bool wantsWrite = false;
        bool closeAfterFlush = false;
        bool readingPaused = false;
    };

    void acceptNew();
    void handleClient(Client& client, const ReadyEvent& event, bool& close);
    bool readFrom(Client& client, bool& peerClosed);
    void processInput(Client& client);
    bool flush(Client& client);
    void updateInterest(Client& client);
    void closeClient(int fd);

    ListeningSocket& listener_;
    store::Store& store_;
    const std::atomic<bool>& shuttingDown_;

    std::unique_ptr<Poller> poller_;
    std::vector<ReadyEvent> events_;

    std::unordered_map<int, std::unique_ptr<Client>> clients_;

    std::vector<int> pendingClose_;

    std::uint64_t accepted_ = 0;
    std::uint64_t commands_ = 0;
    std::uint64_t shortWrites_ = 0;
    std::uint64_t evictedForBacklog_ = 0;
};
}
}
