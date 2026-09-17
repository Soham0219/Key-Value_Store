#include "store/record_applier.h"

#include <memory>

#include "protocol/command.h"
#include "protocol/commands.h"
#include "store/store.h"

namespace kvstore {
namespace store {

RecordApplier::RecordApplier(Store& store) : store_(store) {}

bool RecordApplier::apply(const std::string& payload) {
    std::uint64_t txnId = 0;

    if (protocol::parseBeginMarker(payload, txnId)) {
        if (insideTransaction_) {
            return false;
        }
        insideTransaction_ = true;
        openTxnId_ = txnId;
        pending_.clear();
        ++recordsApplied_;
        return true;
    }

    if (protocol::parseCommitMarker(payload, txnId)) {
        if (!insideTransaction_ || txnId != openTxnId_) {
            return false;
        }

        store_.applyTransactionFromLog(pending_);
        pending_.clear();
        insideTransaction_ = false;
        ++transactionsApplied_;
        ++recordsApplied_;
        return true;
    }

    std::unique_ptr<protocol::Command> command = protocol::deserializeCommand(payload);
    if (!command) {
        return false;
    }

    if (insideTransaction_) {
        pending_.push_back(command->toMutation());
    } else {
        command->apply(store_);
    }
    ++recordsApplied_;
    return true;
}

void RecordApplier::abandonOpenTransaction() {
    pending_.clear();
    insideTransaction_ = false;
    openTxnId_ = 0;
}
}
}
