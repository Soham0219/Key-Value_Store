#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "store/eviction_policy.h"
#include "store/mutation.h"

namespace kvstore {
namespace store {

enum class LockMode {
    None,
    GlobalMutex,
    GlobalSharedMutex,
    Sharded
};

bool parseLockMode(const std::string& text, LockMode& out);

const char* lockModeName(LockMode mode);

class ShardedMap {
public:

    ShardedMap(LockMode mode, std::size_t shardCount, std::size_t maxMemoryBytes = 0);

    ShardedMap(const ShardedMap&) = delete;
    ShardedMap& operator=(const ShardedMap&) = delete;

    bool get(const std::string& key, std::string& out) const;

    void set(const std::string& key, std::string value);
    bool del(const std::string& key);

    std::size_t size() const;

    std::size_t bytesUsed() const;

    std::vector<std::pair<std::string, std::string>> snapshot() const;

    void applyAtomically(const std::vector<Mutation>& mutations);

    std::vector<std::pair<bool, std::string>> getMany(const std::vector<std::string>& keys) const;

    void clear();

    std::size_t shardCount() const noexcept { return shards_.size(); }
    LockMode mode() const noexcept { return mode_; }
    bool evictionEnabled() const noexcept { return maxMemoryBytes_ > 0; }
    std::size_t maxMemoryBytes() const noexcept { return maxMemoryBytes_; }
    std::uint64_t evictions() const noexcept {
        return evictions_.load(std::memory_order_relaxed);
    }

    static constexpr std::size_t kEntryOverheadBytes = 192;

private:

    static constexpr std::size_t kCacheLineBytes = 64;

    struct alignas(kCacheLineBytes) Shard {
        mutable std::shared_mutex mutex;
        std::unordered_map<std::string, std::string> map;

        std::unique_ptr<EvictionPolicy> policy;
    };

    std::size_t shardFor(const std::string& key) const noexcept;

    static void clearShard(Shard& shard);

    void evictIfNeeded(Shard& shard);

    LockMode mode_;
    std::vector<Shard> shards_;
    mutable std::mutex globalMutex_;

    std::size_t maxMemoryBytes_ = 0;
    std::size_t shardBudgetBytes_ = 0;
    mutable std::atomic<std::uint64_t> evictions_{0};
};
}
}
