#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "net/socket.h"
#include "repl/replicator.h"
#include "store/record_applier.h"
#include "store/store.h"

namespace kvstore {
namespace repl {

class Follower final : public Replicator {
public:
    Follower(store::Store& store, const ReplConfig& config);
    ~Follower() override;

    void start() override;
    void stop() override;

    void onAppend(std::uint64_t) override {}

    void onLogReplaced() override {}

    std::string statsLine() const override;
    const char* describe() const override { return "follower"; }

    bool linkUp() const noexcept { return linkUp_.load(std::memory_order_relaxed); }
    std::uint64_t recordsApplied() const noexcept {
        return recordsApplied_.load(std::memory_order_relaxed);
    }
    std::uint64_t fullResyncs() const noexcept {
        return fullResyncs_.load(std::memory_order_relaxed);
    }
    std::uint64_t connectAttempts() const noexcept {
        return connectAttempts_.load(std::memory_order_relaxed);
    }

    static constexpr int kPollTimeoutMs = 200;
    static constexpr int kReconnectDelayMs = 200;

    static constexpr std::size_t kMaxBufferedTransactionBytes = 64u * 1024u * 1024u;

private:
    void linkLoop();
    bool runOneConnection(net::FileDescriptor socket);
    bool handshake(int fd, bool& fullResync);
    bool consume(int fd);
    bool flushPending();
    bool sendAck(int fd, std::uint64_t offset);
    void sleepBeforeRetry();

    std::string stateFilePath() const;
    std::string loadLeaderToken() const;
    void saveLeaderToken(const std::string& token) const;

    store::Store& store_;
    ReplConfig config_;

    store::RecordApplier applier_;

    std::thread linkThread_;

    mutable std::mutex mutex_;
    std::condition_variable stopCv_;
    bool stopping_ = false;

    std::string recvBuffer_;

    std::vector<std::string> pending_;
    std::size_t pendingBytes_ = 0;
    bool insideTransaction_ = false;

    std::atomic<bool> openTransaction_{false};

    std::atomic<bool> linkUp_{false};
    std::atomic<std::uint64_t> recordsApplied_{0};
    std::atomic<std::uint64_t> bytesReceived_{0};
    std::atomic<std::uint64_t> fullResyncs_{0};
    std::atomic<std::uint64_t> connectAttempts_{0};
    std::atomic<std::uint64_t> disconnects_{0};
};
}
}
