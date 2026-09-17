#include "net/server.h"

#include <cerrno>
#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include <poll.h>

#include "net/connection.h"
#include "repl/follower.h"
#include "repl/leader.h"

namespace kvstore {
namespace net {
namespace {

std::atomic<bool> g_shutdownRequested{false};

static_assert(std::atomic<bool>::is_always_lock_free,
              "shutdown flag must be lock-free to be safe inside a signal handler");
}

void Server::requestShutdown() noexcept {
    g_shutdownRequested.store(true, std::memory_order_relaxed);
}

const std::atomic<bool>& Server::shutdownRequested() noexcept {
    return g_shutdownRequested;
}

namespace {

store::StoreConfig makeConfig(store::LockMode lockMode,
                              std::size_t shardCount,
                              std::size_t maxMemoryBytes) {
    store::StoreConfig config;
    config.lockMode = lockMode;
    config.shardCount = shardCount;
    config.maxMemoryBytes = maxMemoryBytes;
    return config;
}
}

Server::Server(std::uint16_t port,
               std::size_t threadCount,
               store::LockMode lockMode,
               std::size_t shardCount,
               std::size_t maxMemoryBytes)
    : Server(port, threadCount, makeConfig(lockMode, shardCount, maxMemoryBytes)) {}

Server::Server(std::uint16_t port, std::size_t threadCount, const store::StoreConfig& storeConfig)
    : Server(port, threadCount, storeConfig, pool::SchedulingPolicy::Fcfs, false) {}

Server::Server(std::uint16_t port,
               std::size_t threadCount,
               const store::StoreConfig& storeConfig,
               pool::SchedulingPolicy schedulingPolicy,
               bool aging)
    : Server(port, threadCount, storeConfig, schedulingPolicy, aging, repl::ReplConfig{}) {}

Server::Server(std::uint16_t port,
               std::size_t threadCount,
               const store::StoreConfig& storeConfig,
               pool::SchedulingPolicy schedulingPolicy,
               bool aging,
               const repl::ReplConfig& replConfig)
    : Server(port, threadCount, storeConfig, schedulingPolicy, aging, replConfig, IoConfig{}) {}

Server::Server(std::uint16_t port,
               std::size_t threadCount,
               const store::StoreConfig& storeConfig,
               pool::SchedulingPolicy schedulingPolicy,
               bool aging,
               const repl::ReplConfig& replConfig,
               const IoConfig& ioConfig)
    : listener_(port, kListenBacklog),
      store_(storeConfig),
      ioConfig_(ioConfig) {
    if (replConfig.role == repl::Role::Leader) {
        replicator_ = std::make_unique<repl::Leader>(store_, replConfig);
    } else if (replConfig.role == repl::Role::Follower) {
        replicator_ = std::make_unique<repl::Follower>(store_, replConfig);
    }

    if (replicator_) {
        store_.setReplicator(replicator_.get());
        replicator_->start();
    }

    if (ioConfig_.mode == IoMode::EventLoop) {
        eventLoop_ = std::make_unique<EventLoop>(listener_, store_, g_shutdownRequested,
                                                 ioConfig_.poller);
    } else {
        pool_ = std::make_unique<pool::ThreadPool>(threadCount, schedulingPolicy, aging);
    }
}

bool parseIoMode(const std::string& text, IoMode& out) {
    if (text == "threads" || text == "pool") {
        out = IoMode::ThreadPool;
    } else if (text == "loop" || text == "event-loop" || text == "async") {
        out = IoMode::EventLoop;
    } else {
        return false;
    }
    return true;
}

const char* ioModeName(IoMode mode) {
    switch (mode) {
        case IoMode::ThreadPool: return "threads";
        case IoMode::EventLoop:  return "event-loop";
    }
    return "unknown";
}

Server::~Server() {
    if (replicator_) {
        replicator_->stop();
    }
}

void Server::run() {
    printBanner();

    if (ioConfig_.mode == IoMode::EventLoop) {
        eventLoop_->run();
        std::cerr << "event loop finished: accepted=" << eventLoop_->accepted()
                  << " commands=" << eventLoop_->commandsHandled()
                  << " short_writes=" << eventLoop_->shortWrites()
                  << " dropped_for_backlog=" << eventLoop_->evictedForBacklog() << '\n';
    } else {
        runThreadPool();
    }

    if (replicator_) {
        std::cerr << "stopping replication\n";
        replicator_->stop();
    }
    std::cerr << "shutdown complete\n";
}

void Server::printBanner() {
    std::cerr << "kvstore listening, ";

    if (ioConfig_.mode == IoMode::EventLoop) {
        std::cerr << "io=event-loop (poller=" << eventLoop_->pollerName() << ", 1 thread)";
    } else {
        std::cerr << "io=threads (" << pool_->threadCount() << " workers, so at most "
                  << pool_->threadCount() << " concurrent clients)";
    }
    std::cerr << ", lock-mode="
              << store::lockModeName(store_.mode()) << ", shards=" << store_.shardCount();

    if (store_.evictionEnabled()) {
        std::cerr << ", max-memory=" << store_.maxMemoryBytes() << " bytes (LRU eviction on;"
                     " reads take an exclusive lock)";
    } else {
        std::cerr << ", max-memory=unlimited (no eviction)";
    }

    if (pool_) {
        std::cerr << ", schedule=" << pool_->policyName();
    }

    std::cerr << ", role=" << (replicator_ ? replicator_->describe() : "standalone");
    std::cerr << '\n';

    if (store_.logEnabled()) {
        const wal::ReplayResult& recovery = store_.recoveryResult();
        std::cerr << "write-ahead log: " << store_.log()->path()
                  << " fsync=" << wal::fsyncModeName(store_.log()->mode())
                  << ", recovered " << recovery.recordsApplied << " records ("
                  << recovery.bytesConsumed << " of " << recovery.fileBytes << " bytes)";

        if (store_.recoveryDiscardedTransaction()) {
            std::cerr << " [discarded an uncommitted transaction at the end of the log]";
        }
        if (recovery.sawCorruption) {
            std::cerr << " [CHECKSUM FAILURE: truncated a damaged tail]";
        } else if (recovery.sawUnknownRecord) {
            std::cerr << " [UNKNOWN RECORD: log written by another version?]";
        } else if (recovery.truncated) {
            std::cerr << " [torn tail truncated - normal after a crash]";
        }
        std::cerr << ", keys=" << store_.size() << '\n';
    } else {
        std::cerr << "write-ahead log: disabled (data is lost on exit)\n";
    }

    if (replicator_) {
        std::cerr << "replication: " << replicator_->statsLine() << '\n';
    }
}

void Server::runThreadPool() {
    while (!g_shutdownRequested.load(std::memory_order_relaxed)) {
        pollfd pfd{};
        pfd.fd = listener_.fd();
        pfd.events = POLLIN;

        int ready = ::poll(&pfd, 1, kAcceptTimeoutMs);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            std::cerr << "poll() on listening socket failed; stopping accept loop\n";
            break;
        }
        if (ready == 0) {
            continue;
        }

        try {
            FileDescriptor client = listener_.accept();
            if (!client.valid()) {
                continue;
            }

            auto connection = std::make_shared<Connection>(
                std::move(client),
                store_,
                g_shutdownRequested);

            if (!pool_->submit([connection] { connection->run(); })) {
                std::cerr << "refusing connection: server is shutting down\n";
            }

        } catch (const std::exception& e) {
            std::cerr << "accept failed: " << e.what() << '\n';
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }

    std::cerr << "shutting down: joining workers\n";

    pool_->shutdown();
}
}
}
