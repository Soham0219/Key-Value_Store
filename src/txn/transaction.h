#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "store/mutation.h"
#include "store/store.h"

namespace kvstore {
namespace txn {

class Transaction {
public:

    explicit Transaction(store::Store& store);

    ~Transaction();

    Transaction(const Transaction&) = delete;
    Transaction& operator=(const Transaction&) = delete;
    Transaction(Transaction&&) = delete;
    Transaction& operator=(Transaction&&) = delete;

    void set(const std::string& key, std::string value);
    void del(const std::string& key);

    bool get(const std::string& key, std::string& out) const;

    bool commit();

    void rollback() noexcept;

    bool active() const noexcept { return active_; }

    std::size_t pendingOperations() const noexcept { return mutations_.size(); }
    std::uint64_t id() const noexcept { return id_; }

private:
    store::Store& store_;
    std::uint64_t id_;

    std::vector<store::Mutation> mutations_;

    std::unordered_map<std::string, std::size_t> latest_;

    bool active_ = true;
};
}
}
