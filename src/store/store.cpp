#include "store/store.h"

#include <string>
#include <utility>

#include <sys/stat.h>

#include "protocol/command.h"
#include "protocol/commands.h"
#include "repl/replicator.h"
#include "store/record_applier.h"

namespace kvstore {
namespace store {

Store::Store(LockMode mode, std::size_t shardCount, std::size_t maxMemoryBytes)
    : map_(mode, shardCount, maxMemoryBytes) {}

Store::Store(const StoreConfig& config)
    : map_(config.lockMode, config.shardCount, config.maxMemoryBytes),
      logPath_(config.logPath),
      readOnly_(config.readOnly) {
    if (logPath_.empty()) {
        return;
    }

    recovery_ = recoverFromLog();

    log_ = std::make_unique<wal::WriteAheadLog>(logPath_, config.fsyncMode,
                                               config.fsyncIntervalMs);
}

Store::~Store() = default;

wal::ReplayResult Store::recoverFromLog() {
    RecordApplier applier(*this);

    wal::ReplayResult result = wal::WriteAheadLog::replay(
        logPath_,

        [&applier](const std::string& payload) { return applier.apply(payload); });

    if (applier.insideTransaction()) {
        recoveryDiscardedTransaction_ = true;
    }
    return result;
}

bool Store::commitTransaction(std::uint64_t txnId, const std::vector<Mutation>& mutations) {
    if (mutations.empty()) {
        txnCommitted_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    if (log_) {
        std::shared_lock<std::shared_mutex> epoch(walEpoch_);

        std::vector<std::string> payloads;
        payloads.reserve(mutations.size() + 2);
        payloads.push_back(protocol::serializeBeginMarker(txnId));
        for (const Mutation& mutation : mutations) {
            if (mutation.erase) {
                payloads.push_back(protocol::DelCommand(mutation.key).serialize());
            } else {
                payloads.push_back(protocol::SetCommand(mutation.key, mutation.value).serialize());
            }
        }
        payloads.push_back(protocol::serializeCommitMarker(txnId));

        std::uint64_t endOffset = 0;
        try {
            endOffset = log_->appendBatch(payloads);
        } catch (const std::exception&) {
            return false;
        }

        map_.applyAtomically(mutations);

        if (replicator_ != nullptr) {
            replicator_->onAppend(endOffset);
        }
    } else {
        map_.applyAtomically(mutations);
    }

    writes_.fetch_add(mutations.size(), std::memory_order_relaxed);
    txnCommitted_.fetch_add(1, std::memory_order_relaxed);
    return true;
}

bool Store::get(const std::string& key, std::string& out) {
    const bool found = map_.get(key, out);

    if (found) {
        hits_.fetch_add(1, std::memory_order_relaxed);
    } else {
        misses_.fetch_add(1, std::memory_order_relaxed);
    }
    return found;
}

std::vector<std::pair<bool, std::string>> Store::getMany(const std::vector<std::string>& keys) {
    std::vector<std::pair<bool, std::string>> results = map_.getMany(keys);
    for (const auto& result : results) {
        if (result.first) {
            hits_.fetch_add(1, std::memory_order_relaxed);
        } else {
            misses_.fetch_add(1, std::memory_order_relaxed);
        }
    }
    return results;
}

void Store::set(const std::string& key, std::string value) {
    if (log_) {
        std::shared_lock<std::shared_mutex> epoch(walEpoch_);

        const protocol::SetCommand command(key, value);

        const std::uint64_t endOffset = log_->append(command.serialize());

        map_.set(key, std::move(value));

        if (replicator_ != nullptr) {
            replicator_->onAppend(endOffset);
        }
    } else {
        map_.set(key, std::move(value));
    }
    writes_.fetch_add(1, std::memory_order_relaxed);
}

bool Store::del(const std::string& key) {
    bool removed = false;
    if (log_) {
        std::shared_lock<std::shared_mutex> epoch(walEpoch_);

        const protocol::DelCommand command(key);
        const std::uint64_t endOffset = log_->append(command.serialize());

        removed = map_.del(key);

        if (replicator_ != nullptr) {
            replicator_->onAppend(endOffset);
        }
    } else {
        removed = map_.del(key);
    }
    writes_.fetch_add(1, std::memory_order_relaxed);
    return removed;
}

void Store::applyFromLog(const std::string& key, const std::string& value) {
    map_.set(key, value);
}

void Store::eraseFromLog(const std::string& key) {
    map_.del(key);
}

void Store::applyTransactionFromLog(const std::vector<Mutation>& mutations) {
    if (mutations.empty()) {
        txnCommitted_.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    map_.applyAtomically(mutations);
    writes_.fetch_add(mutations.size(), std::memory_order_relaxed);
    txnCommitted_.fetch_add(1, std::memory_order_relaxed);
}

std::uint64_t Store::logOffset() const {
    return log_ ? log_->offset() : 0;
}

std::string Store::replicationToken() const {
    if (logPath_.empty()) {
        return std::string();
    }
    struct stat info {};
    if (::stat(logPath_.c_str(), &info) != 0) {
        return std::string();
    }

    return std::to_string(static_cast<unsigned long long>(info.st_dev)) + "-" +
           std::to_string(static_cast<unsigned long long>(info.st_ino));
}

std::uint64_t Store::appendReplicatedRecords(const std::vector<std::string>& payloads) {
    if (!log_) {
        return 0;
    }

    std::shared_lock<std::shared_mutex> epoch(walEpoch_);

    return log_->appendBatch(payloads);
}

void Store::resetForFullResync() {
    std::unique_lock<std::shared_mutex> block(walEpoch_);

    map_.clear();
    if (log_) {
        log_->truncateAll();
    }
}

std::size_t Store::size() const {
    return map_.size();
}

std::size_t Store::bytesUsed() const {
    return map_.bytesUsed();
}

wal::CompactionResult Store::compact() {
    if (!log_) {
        wal::CompactionResult result;
        result.error = "no write-ahead log configured";
        return result;
    }

    std::unique_lock<std::shared_mutex> block(walEpoch_);

    const std::vector<std::pair<std::string, std::string>> entries = map_.snapshot();

    wal::CompactionResult result = wal::Compactor::rewrite(log_->path(), entries);

    if (result.ok) {
        log_->reopenAfterCompaction();

        if (replicator_ != nullptr) {
            replicator_->onLogReplaced();
        }
    }

    return result;
}

std::string Store::statsLine() const {
    std::string out =
        "hits=" + std::to_string(hits()) +
        " misses=" + std::to_string(misses()) +
        " writes=" + std::to_string(writes()) +
        " evictions=" + std::to_string(evictions()) +
        " keys=" + std::to_string(size()) +
        " bytes=" + std::to_string(bytesUsed()) +
        " limit=" + std::to_string(maxMemoryBytes()) +
        " shards=" + std::to_string(shardCount()) +
        " lock=" + lockModeName(mode());

    out += " txn_commits=" + std::to_string(transactionsCommitted()) +
           " txn_rollbacks=" + std::to_string(transactionsRolledBack());

    if (log_) {
        out += " log=" + std::string(wal::fsyncModeName(log_->mode())) +
               " log_appends=" + std::to_string(log_->appends()) +
               " log_syncs=" + std::to_string(log_->syncs()) +
               " log_bytes=" + std::to_string(log_->bytesWritten()) +
               " recovered=" + std::to_string(recovery_.recordsApplied);
    } else {
        out += " log=off";
    }

    if (replicator_ != nullptr) {
        out += " " + replicator_->statsLine();
    } else {
        out += " repl=off";
    }
    return out;
}
}
}
