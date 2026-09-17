#include "store/sharded_map.h"

#include <algorithm>
#include <functional>
#include <utility>

#include "store/lru_policy.h"

namespace kvstore {
namespace store {
namespace {

std::size_t roundUpToPowerOfTwo(std::size_t n) {
    if (n <= 1) {
        return 1;
    }
    std::size_t power = 1;
    while (power < n) {
        power <<= 1;
    }
    return power;
}
}

bool parseLockMode(const std::string& text, LockMode& out) {
    if (text == "none") {
        out = LockMode::None;
    } else if (text == "global-mutex") {
        out = LockMode::GlobalMutex;
    } else if (text == "global-shared") {
        out = LockMode::GlobalSharedMutex;
    } else if (text == "sharded") {
        out = LockMode::Sharded;
    } else {
        return false;
    }
    return true;
}

const char* lockModeName(LockMode mode) {
    switch (mode) {
        case LockMode::None:              return "none";
        case LockMode::GlobalMutex:       return "global-mutex";
        case LockMode::GlobalSharedMutex: return "global-shared";
        case LockMode::Sharded:           return "sharded";
    }
    return "unknown";
}

ShardedMap::ShardedMap(LockMode mode, std::size_t shardCount, std::size_t maxMemoryBytes)
    : mode_(mode),

      shards_(mode == LockMode::Sharded ? roundUpToPowerOfTwo(shardCount) : 1),
      maxMemoryBytes_(maxMemoryBytes) {
    if (maxMemoryBytes_ == 0) {
        return;
    }

    shardBudgetBytes_ = maxMemoryBytes_ / shards_.size();
    if (shardBudgetBytes_ == 0) {
        shardBudgetBytes_ = 1;
    }

    for (Shard& shard : shards_) {
        shard.policy = std::make_unique<LruPolicy>();
    }
}

std::size_t ShardedMap::shardFor(const std::string& key) const noexcept {
    const std::size_t hash = std::hash<std::string>{}(key);
    return hash & (shards_.size() - 1);
}

void ShardedMap::evictIfNeeded(Shard& shard) {
    if (!shard.policy) {
        return;
    }

    while (shard.policy->bytes() > shardBudgetBytes_ && shard.policy->entries() > 1) {
        std::string victim;
        if (!shard.policy->evict(victim)) {
            break;
        }
        shard.map.erase(victim);
        evictions_.fetch_add(1, std::memory_order_relaxed);
    }
}

bool ShardedMap::get(const std::string& key, std::string& out) const {
    const Shard& shard = shards_[mode_ == LockMode::Sharded ? shardFor(key) : 0];

    switch (mode_) {
        case LockMode::None: {
            auto it = shard.map.find(key);
            if (it == shard.map.end()) {
                return false;
            }
            out = it->second;
            if (shard.policy) {
                const_cast<EvictionPolicy*>(shard.policy.get())->onAccess(key);
            }
            return true;
        }

        case LockMode::GlobalMutex: {
            std::lock_guard<std::mutex> lock(globalMutex_);
            auto it = shard.map.find(key);
            if (it == shard.map.end()) {
                return false;
            }
            out = it->second;
            if (shard.policy) {
                const_cast<EvictionPolicy*>(shard.policy.get())->onAccess(key);
            }
            return true;
        }

        case LockMode::GlobalSharedMutex:
        case LockMode::Sharded: {
            if (shard.policy) {
                std::unique_lock<std::shared_mutex> lock(shard.mutex);
                auto it = shard.map.find(key);
                if (it == shard.map.end()) {
                    return false;
                }
                out = it->second;
                const_cast<EvictionPolicy*>(shard.policy.get())->onAccess(key);
                return true;
            }

            std::shared_lock<std::shared_mutex> lock(shard.mutex);
            auto it = shard.map.find(key);
            if (it == shard.map.end()) {
                return false;
            }

            out = it->second;
            return true;
        }
    }
    return false;
}

void ShardedMap::set(const std::string& key, std::string value) {
    Shard& shard = shards_[mode_ == LockMode::Sharded ? shardFor(key) : 0];

    const std::size_t entryBytes = key.size() + value.size() + kEntryOverheadBytes;

    switch (mode_) {
        case LockMode::None:

            shard.map[key] = std::move(value);
            if (shard.policy) {
                shard.policy->onInsert(key, entryBytes);
                evictIfNeeded(shard);
            }
            return;

        case LockMode::GlobalMutex: {
            std::lock_guard<std::mutex> lock(globalMutex_);
            shard.map[key] = std::move(value);
            if (shard.policy) {
                shard.policy->onInsert(key, entryBytes);
                evictIfNeeded(shard);
            }
            return;
        }

        case LockMode::GlobalSharedMutex:
        case LockMode::Sharded: {
            std::unique_lock<std::shared_mutex> lock(shard.mutex);
            shard.map[key] = std::move(value);
            if (shard.policy) {
                shard.policy->onInsert(key, entryBytes);
                evictIfNeeded(shard);
            }
            return;
        }
    }
}

bool ShardedMap::del(const std::string& key) {
    Shard& shard = shards_[mode_ == LockMode::Sharded ? shardFor(key) : 0];

    switch (mode_) {
        case LockMode::None: {
            const bool removed = shard.map.erase(key) > 0;
            if (shard.policy) {
                shard.policy->onErase(key);
            }
            return removed;
        }

        case LockMode::GlobalMutex: {
            std::lock_guard<std::mutex> lock(globalMutex_);
            const bool removed = shard.map.erase(key) > 0;
            if (shard.policy) {
                shard.policy->onErase(key);
            }
            return removed;
        }

        case LockMode::GlobalSharedMutex:
        case LockMode::Sharded: {
            std::unique_lock<std::shared_mutex> lock(shard.mutex);
            const bool removed = shard.map.erase(key) > 0;
            if (shard.policy) {
                shard.policy->onErase(key);
            }
            return removed;
        }
    }
    return false;
}

std::size_t ShardedMap::size() const {
    if (mode_ == LockMode::None) {
        std::size_t total = 0;
        for (const Shard& shard : shards_) {
            total += shard.map.size();
        }
        return total;
    }

    if (mode_ == LockMode::GlobalMutex) {
        std::lock_guard<std::mutex> lock(globalMutex_);
        return shards_[0].map.size();
    }

    std::vector<std::shared_lock<std::shared_mutex>> locks;
    locks.reserve(shards_.size());

    std::size_t total = 0;
    for (std::size_t i = 0; i < shards_.size(); ++i) {
        locks.emplace_back(shards_[i].mutex);
        total += shards_[i].map.size();
    }
    return total;
}

std::vector<std::pair<bool, std::string>> ShardedMap::getMany(
        const std::vector<std::string>& keys) const {
    std::vector<std::pair<bool, std::string>> results(keys.size());

    const auto readAll = [this, &keys, &results]() {
        for (std::size_t i = 0; i < keys.size(); ++i) {
            const Shard& shard =
                shards_[mode_ == LockMode::Sharded ? shardFor(keys[i]) : 0];
            auto it = shard.map.find(keys[i]);
            if (it == shard.map.end()) {
                results[i].first = false;
                continue;
            }
            results[i].first = true;
            results[i].second = it->second;
            if (shard.policy) {
                const_cast<EvictionPolicy*>(shard.policy.get())->onAccess(keys[i]);
            }
        }
    };

    if (keys.empty()) {
        return results;
    }

    if (mode_ == LockMode::None || mode_ == LockMode::GlobalMutex) {
        std::unique_lock<std::mutex> lock(globalMutex_, std::defer_lock);
        if (mode_ == LockMode::GlobalMutex) {
            lock.lock();
        }
        readAll();
        return results;
    }

    std::vector<std::size_t> touched;
    touched.reserve(keys.size());
    for (const std::string& key : keys) {
        touched.push_back(shardFor(key));
    }
    std::sort(touched.begin(), touched.end());
    touched.erase(std::unique(touched.begin(), touched.end()), touched.end());

    if (evictionEnabled()) {
        std::vector<std::unique_lock<std::shared_mutex>> locks;
        locks.reserve(touched.size());
        for (std::size_t index : touched) {
            locks.emplace_back(shards_[index].mutex);
        }
        readAll();
        return results;
    }

    std::vector<std::shared_lock<std::shared_mutex>> locks;
    locks.reserve(touched.size());
    for (std::size_t index : touched) {
        locks.emplace_back(shards_[index].mutex);
    }
    readAll();
    return results;
}

std::vector<std::pair<std::string, std::string>> ShardedMap::snapshot() const {
    std::vector<std::pair<std::string, std::string>> entries;

    if (mode_ == LockMode::None || mode_ == LockMode::GlobalMutex) {
        std::unique_lock<std::mutex> lock(globalMutex_, std::defer_lock);
        if (mode_ == LockMode::GlobalMutex) {
            lock.lock();
        }
        entries.reserve(shards_[0].map.size());
        for (const auto& pair : shards_[0].map) {
            entries.emplace_back(pair.first, pair.second);
        }
        return entries;
    }

    std::vector<std::shared_lock<std::shared_mutex>> locks;
    locks.reserve(shards_.size());

    std::size_t total = 0;
    for (std::size_t i = 0; i < shards_.size(); ++i) {
        locks.emplace_back(shards_[i].mutex);
        total += shards_[i].map.size();
    }
    entries.reserve(total);

    for (const Shard& shard : shards_) {
        for (const auto& pair : shard.map) {
            entries.emplace_back(pair.first, pair.second);
        }
    }
    return entries;
}

void ShardedMap::applyAtomically(const std::vector<Mutation>& mutations) {
    if (mutations.empty()) {
        return;
    }

    const auto applyOne = [](Shard& shard, const Mutation& mutation) {
        if (mutation.erase) {
            shard.map.erase(mutation.key);
            if (shard.policy) {
                shard.policy->onErase(mutation.key);
            }
        } else {
            const std::size_t entryBytes =
                mutation.key.size() + mutation.value.size() + kEntryOverheadBytes;
            shard.map[mutation.key] = mutation.value;
            if (shard.policy) {
                shard.policy->onInsert(mutation.key, entryBytes);
            }
        }
    };

    if (mode_ == LockMode::None || mode_ == LockMode::GlobalMutex) {
        std::unique_lock<std::mutex> lock(globalMutex_, std::defer_lock);
        if (mode_ == LockMode::GlobalMutex) {
            lock.lock();
        }
        for (const Mutation& mutation : mutations) {
            applyOne(shards_[0], mutation);
        }
        evictIfNeeded(shards_[0]);
        return;
    }

    std::vector<std::size_t> touched;
    touched.reserve(mutations.size());
    for (const Mutation& mutation : mutations) {
        touched.push_back(shardFor(mutation.key));
    }

    std::sort(touched.begin(), touched.end());
    touched.erase(std::unique(touched.begin(), touched.end()), touched.end());

    std::vector<std::unique_lock<std::shared_mutex>> locks;
    locks.reserve(touched.size());
    for (std::size_t index : touched) {
        locks.emplace_back(shards_[index].mutex);
    }

    for (const Mutation& mutation : mutations) {
        applyOne(shards_[shardFor(mutation.key)], mutation);
    }

    for (std::size_t index : touched) {
        evictIfNeeded(shards_[index]);
    }
}

void ShardedMap::clear() {
    if (mode_ == LockMode::None || mode_ == LockMode::GlobalMutex) {
        std::unique_lock<std::mutex> lock(globalMutex_, std::defer_lock);
        if (mode_ == LockMode::GlobalMutex) {
            lock.lock();
        }
        clearShard(shards_[0]);
        return;
    }

    std::vector<std::unique_lock<std::shared_mutex>> locks;
    locks.reserve(shards_.size());
    for (std::size_t i = 0; i < shards_.size(); ++i) {
        locks.emplace_back(shards_[i].mutex);
    }

    for (Shard& shard : shards_) {
        clearShard(shard);
    }
}

void ShardedMap::clearShard(Shard& shard) {
    if (shard.policy) {
        for (const auto& pair : shard.map) {
            shard.policy->onErase(pair.first);
        }
    }
    shard.map.clear();
}

std::size_t ShardedMap::bytesUsed() const {
    if (maxMemoryBytes_ == 0) {
        return 0;
    }

    if (mode_ == LockMode::GlobalMutex) {
        std::lock_guard<std::mutex> lock(globalMutex_);
        return shards_[0].policy ? shards_[0].policy->bytes() : 0;
    }
    if (mode_ == LockMode::None) {
        std::size_t total = 0;
        for (const Shard& shard : shards_) {
            total += shard.policy ? shard.policy->bytes() : 0;
        }
        return total;
    }

    std::vector<std::shared_lock<std::shared_mutex>> locks;
    locks.reserve(shards_.size());

    std::size_t total = 0;
    for (std::size_t i = 0; i < shards_.size(); ++i) {
        locks.emplace_back(shards_[i].mutex);
        total += shards_[i].policy ? shards_[i].policy->bytes() : 0;
    }
    return total;
}
}
}
