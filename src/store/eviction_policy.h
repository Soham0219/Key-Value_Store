#pragma once

#include <cstddef>
#include <string>

namespace kvstore {
namespace store {

class EvictionPolicy {
public:

    virtual ~EvictionPolicy();

    virtual void onAccess(const std::string& key) = 0;

    virtual void onInsert(const std::string& key, std::size_t bytes) = 0;

    virtual void onErase(const std::string& key) = 0;

    virtual bool evict(std::string& victim) = 0;

    virtual std::size_t bytes() const noexcept = 0;
    virtual std::size_t entries() const noexcept = 0;

protected:

    EvictionPolicy() = default;
    EvictionPolicy(const EvictionPolicy&) = delete;
    EvictionPolicy& operator=(const EvictionPolicy&) = delete;
};
}
}
