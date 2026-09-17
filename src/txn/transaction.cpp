#include "txn/transaction.h"

#include <utility>

namespace kvstore {
namespace txn {

Transaction::Transaction(store::Store& store)
    : store_(store),
      id_(store.nextTransactionId()) {}

Transaction::~Transaction() {
    if (active_) {
        rollback();
    }
}

void Transaction::set(const std::string& key, std::string value) {
    if (!active_) {
        return;
    }

    store::Mutation mutation;
    mutation.erase = false;
    mutation.key = key;
    mutation.value = std::move(value);

    mutations_.push_back(std::move(mutation));

    latest_[key] = mutations_.size() - 1;
}

void Transaction::del(const std::string& key) {
    if (!active_) {
        return;
    }

    store::Mutation mutation;
    mutation.erase = true;
    mutation.key = key;

    mutations_.push_back(std::move(mutation));
    latest_[key] = mutations_.size() - 1;
}

bool Transaction::get(const std::string& key, std::string& out) const {
    auto it = latest_.find(key);
    if (it != latest_.end()) {
        const store::Mutation& mutation = mutations_[it->second];
        if (mutation.erase) {
            return false;
        }
        out = mutation.value;
        return true;
    }

    return store_.get(key, out);
}

bool Transaction::commit() {
    if (!active_) {
        return false;
    }

    const bool ok = store_.commitTransaction(id_, mutations_);
    if (!ok) {
        return false;
    }

    active_ = false;
    mutations_.clear();
    latest_.clear();
    return true;
}

void Transaction::rollback() noexcept {
    if (!active_) {
        return;
    }
    active_ = false;

    mutations_.clear();
    latest_.clear();

    store_.noteRollback();
}
}
}
