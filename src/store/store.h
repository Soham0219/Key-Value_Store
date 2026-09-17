#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "store/sharded_map.h"
#include "wal/compactor.h"
#include "wal/wal.h"

namespace kvstore {
namespace repl {

class Replicator;
}

namespace store {

struct StoreConfig {
    LockMode lockMode = LockMode::Sharded;
    std::size_t shardCount = 16;
    std::size_t maxMemoryBytes = 0;

    std::string logPath;
    wal::FsyncMode fsyncMode = wal::FsyncMode::Always;
    int fsyncIntervalMs = 1000;

    bool readOnly = false;
};

class Store {
public:
    static constexpr std::size_t kDefaultShardCount = 16;

    explicit Store(LockMode mode = LockMode::Sharded,
                   std::size_t shardCount = kDefaultShardCount,
                   std::size_t maxMemoryBytes = 0);

    explicit Store(const StoreConfig& config);

    ~Store();

    Store(const Store&) = delete;
    Store& operator=(const Store&) = delete;

    bool get(const std::string& key, std::string& out);

    std::vector<std::pair<bool, std::string>> getMany(const std::vector<std::string>& keys);

    void set(const std::string& key, std::string value);
    bool del(const std::string& key);

    void applyFromLog(const std::string& key, const std::string& value);
    void eraseFromLog(const std::string& key);

    std::size_t size() const;
    std::size_t bytesUsed() const;

    std::uint64_t hits() const noexcept { return hits_.load(std::memory_order_relaxed); }
    std::uint64_t misses() const noexcept { return misses_.load(std::memory_order_relaxed); }
    std::uint64_t writes() const noexcept { return writes_.load(std::memory_order_relaxed); }
    std::uint64_t evictions() const noexcept { return map_.evictions(); }

    std::string statsLine() const;

    std::size_t maxMemoryBytes() const noexcept { return map_.maxMemoryBytes(); }
    bool evictionEnabled() const noexcept { return map_.evictionEnabled(); }
    LockMode mode() const noexcept { return map_.mode(); }
    std::size_t shardCount() const noexcept { return map_.shardCount(); }

    std::uint64_t nextTransactionId() noexcept {
        return nextTxnId_.fetch_add(1, std::memory_order_relaxed);
    }

    bool commitTransaction(std::uint64_t txnId, const std::vector<Mutation>& mutations);

    void noteRollback() noexcept { txnRolledBack_.fetch_add(1, std::memory_order_relaxed); }

    std::uint64_t transactionsCommitted() const noexcept {
        return txnCommitted_.load(std::memory_order_relaxed);
    }
    std::uint64_t transactionsRolledBack() const noexcept {
        return txnRolledBack_.load(std::memory_order_relaxed);
    }

    wal::CompactionResult compact();

    void setReplicator(repl::Replicator* replicator) noexcept { replicator_ = replicator; }

    bool readOnly() const noexcept { return readOnly_; }

    std::uint64_t logOffset() const;

    std::string replicationToken() const;

    void applyTransactionFromLog(const std::vector<Mutation>& mutations);

    std::uint64_t appendReplicatedRecords(const std::vector<std::string>& payloads);

    void resetForFullResync();

    bool logEnabled() const noexcept { return log_ != nullptr; }
    const wal::WriteAheadLog* log() const noexcept { return log_.get(); }

    const wal::ReplayResult& recoveryResult() const noexcept { return recovery_; }

    bool recoveryDiscardedTransaction() const noexcept { return recoveryDiscardedTransaction_; }

private:

    wal::ReplayResult recoverFromLog();

    ShardedMap map_;

    std::string logPath_;
    std::unique_ptr<wal::WriteAheadLog> log_;
    mutable std::shared_mutex walEpoch_;
    wal::ReplayResult recovery_;
    bool recoveryDiscardedTransaction_ = false;

    repl::Replicator* replicator_ = nullptr;
    bool readOnly_ = false;

    std::atomic<std::uint64_t> hits_{0};
    std::atomic<std::uint64_t> misses_{0};
    std::atomic<std::uint64_t> writes_{0};

    std::atomic<std::uint64_t> nextTxnId_{1};
    std::atomic<std::uint64_t> txnCommitted_{0};
    std::atomic<std::uint64_t> txnRolledBack_{0};
};
}
}
