#include "store/lru_policy.h"

#include <utility>

namespace kvstore {
namespace store {

LruPolicy::~LruPolicy() = default;

void LruPolicy::onAccess(const std::string& key) {
    auto it = index_.find(key);
    if (it == index_.end()) {
        return;
    }

    recency_.splice(recency_.begin(), recency_, it->second.node);
}

void LruPolicy::onInsert(const std::string& key, std::size_t bytes) {
    auto it = index_.find(key);

    if (it != index_.end()) {
        bytes_ -= it->second.bytes;
        it->second.bytes = bytes;
        bytes_ += bytes;
        recency_.splice(recency_.begin(), recency_, it->second.node);
        return;
    }

    recency_.push_front(key);
    index_.emplace(key, Entry{recency_.begin(), bytes});
    bytes_ += bytes;
}

void LruPolicy::onErase(const std::string& key) {
    auto it = index_.find(key);
    if (it == index_.end()) {
        return;
    }
    bytes_ -= it->second.bytes;
    recency_.erase(it->second.node);
    index_.erase(it);
}

bool LruPolicy::evict(std::string& victim) {
    if (recency_.empty()) {
        return false;
    }

    victim = std::move(recency_.back());
    recency_.pop_back();

    auto it = index_.find(victim);
    if (it != index_.end()) {
        bytes_ -= it->second.bytes;
        index_.erase(it);
    }
    return true;
}
}
}
