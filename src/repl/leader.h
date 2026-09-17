#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "net/socket.h"
#include "repl/replicator.h"
#include "store/store.h"

namespace kvstore {
namespace repl {

class Leader final : public Replicator {
public:

    Leader(store::Store& store, const ReplConfig& config);
    ~Leader() override;

    void start() override;
    void stop() override;
    void onAppend(std::uint64_t endOffset) override;
    std::string statsLine() const override;
    const char* describe() const override { return "leader"; }

    void onLogReplaced() override;

    std::size_t followerCount() const;
    std::uint64_t ackedOffset() const;
    std::uint64_t fullResyncs() const;
    std::uint64_t ackTimeouts() const;

    static constexpr int kPollTimeoutMs = 200;

    static constexpr std::size_t kStreamChunkBytes = 64 * 1024;

private:

    struct Session {
        net::FileDescriptor socket;
        net::FileDescriptor logReader;
        net::FileDescriptor wakeRead;
        net::FileDescriptor wakeWrite;
        std::thread thread;

        std::atomic<bool> stopping{false};
        std::atomic<bool> finished{false};

        std::uint64_t sentOffset = 0;
        std::atomic<std::uint64_t> acked{0};
        std::atomic<std::uint64_t> bytesSent{0};
        std::string ackBuffer;
    };

    void acceptLoop();
    void sessionLoop(const std::shared_ptr<Session>& session);
    bool performHandshake(Session& session, net::FileDescriptor& socket);
    bool drainAcks(Session& session);
    bool shipPending(Session& session);
    void noteAck(std::uint64_t offset);
    void reapFinishedSessions();
    void wakeSessions();

    store::Store& store_;
    ReplConfig config_;

    std::unique_ptr<net::ListeningSocket> listener_;
    std::thread acceptThread_;

    mutable std::mutex mutex_;

    std::condition_variable ackCv_;

    bool stopping_ = false;
    std::vector<std::shared_ptr<Session>> sessions_;

    struct PendingRecord {
        std::uint64_t offset;
        std::chrono::steady_clock::time_point at;
    };
    std::deque<PendingRecord> pending_;

    static constexpr std::size_t kMaxLagSamples = 1u << 16;
    std::vector<std::uint32_t> lagSamplesUs_;
    std::uint64_t lagCount_ = 0;
    std::uint64_t lagMaxUs_ = 0;
    std::uint64_t lagSumUs_ = 0;

    std::atomic<std::uint64_t> connections_{0};
    std::atomic<std::uint64_t> fullResyncs_{0};
    std::atomic<std::uint64_t> ackTimeouts_{0};
    std::atomic<std::uint64_t> syncDegraded_{0};
};
}
}
