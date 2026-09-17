#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "store/mutation.h"

namespace kvstore {
namespace store {

class Store;

class RecordApplier {
public:

    explicit RecordApplier(Store& store);

    bool apply(const std::string& payload);

    bool insideTransaction() const noexcept { return insideTransaction_; }

    void abandonOpenTransaction();

    std::uint64_t recordsApplied() const noexcept { return recordsApplied_; }
    std::uint64_t transactionsApplied() const noexcept { return transactionsApplied_; }

private:
    Store& store_;
    std::vector<Mutation> pending_;
    bool insideTransaction_ = false;
    std::uint64_t openTxnId_ = 0;
    std::uint64_t recordsApplied_ = 0;
    std::uint64_t transactionsApplied_ = 0;
};
}
}
