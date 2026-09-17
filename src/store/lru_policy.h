#pragma once

#include <cstddef>
#include <list>
#include <string>
#include <unordered_map>

#include "store/eviction_policy.h"

namespace kvstore {
namespace store {

class LruPolicy final : public EvictionPolicy {
public:
    LruPolicy() = default;
    ~LruPolicy() override;

    void onAccess(const std::string& key) override;
    void onInsert(const std::string& key, std::size_t bytes) override;
    void onErase(const std::string& key) override;
    bool evict(std::string& victim) override;

    std::size_t bytes() const noexcept override { return bytes_; }
    std::size_t entries() const noexcept override { return index_.size(); }

private:

    struct Entry {
        std::list<std::string>::iterator node;
        std::size_t bytes;
    };

    std::list<std::string> recency_;
    std::unordered_map<std::string, Entry> index_;
    std::size_t bytes_ = 0;
};
}
}
