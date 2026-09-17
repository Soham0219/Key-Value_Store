#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "net/event_loop.h"
#include "net/poller.h"
#include "net/socket.h"
#include "pool/thread_pool.h"
#include "repl/replicator.h"
#include "store/store.h"

namespace kvstore {
namespace net {

enum class IoMode {
    ThreadPool,
    EventLoop
};

bool parseIoMode(const std::string& text, IoMode& out);
const char* ioModeName(IoMode mode);

struct IoConfig {
    IoMode mode = IoMode::ThreadPool;
    PollerKind poller = PollerKind::Auto;
};

class Server {
public:

    Server(std::uint16_t port,
           std::size_t threadCount,
           store::LockMode lockMode = store::LockMode::Sharded,
           std::size_t shardCount = store::Store::kDefaultShardCount,
           std::size_t maxMemoryBytes = 0);

    Server(std::uint16_t port, std::size_t threadCount, const store::StoreConfig& storeConfig);

    Server(std::uint16_t port,
           std::size_t threadCount,
           const store::StoreConfig& storeConfig,
           pool::SchedulingPolicy schedulingPolicy,
           bool aging);

    Server(std::uint16_t port,
           std::size_t threadCount,
           const store::StoreConfig& storeConfig,
           pool::SchedulingPolicy schedulingPolicy,
           bool aging,
           const repl::ReplConfig& replConfig);

    Server(std::uint16_t port,
           std::size_t threadCount,
           const store::StoreConfig& storeConfig,
           pool::SchedulingPolicy schedulingPolicy,
           bool aging,
           const repl::ReplConfig& replConfig,
           const IoConfig& ioConfig);

    ~Server();

    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    void run();

    static void requestShutdown() noexcept;

    static const std::atomic<bool>& shutdownRequested() noexcept;

    static constexpr int kListenBacklog = 128;

private:
    static constexpr int kAcceptTimeoutMs = 200;

    ListeningSocket listener_;
    store::Store store_;

    std::unique_ptr<repl::Replicator> replicator_;

    std::unique_ptr<pool::ThreadPool> pool_;
    std::unique_ptr<EventLoop> eventLoop_;

    IoConfig ioConfig_;

    void runThreadPool();
    void printBanner();
};
}
}
