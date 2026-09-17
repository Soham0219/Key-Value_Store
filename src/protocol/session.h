#pragma once

#include <memory>
#include <string>

#include "store/store.h"
#include "txn/transaction.h"

namespace kvstore {
namespace protocol {

class Session {
public:

    explicit Session(store::Store& store);

    Session(const Session&) = delete;
    Session& operator=(const Session&) = delete;

    std::string handleLine(const std::string& line, bool& quit);

    bool inTransaction() const noexcept { return transaction_ != nullptr; }

private:
    store::Store& store_;

    std::unique_ptr<txn::Transaction> transaction_;
};
}
}
