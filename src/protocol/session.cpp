#include "protocol/session.h"

#include <utility>

#include "protocol/parser.h"

namespace kvstore {
namespace protocol {

Session::Session(store::Store& store) : store_(store) {}

std::string Session::handleLine(const std::string& line, bool& quit) {
    ParsedCommand cmd = CommandParser::parse(line);

    if (store_.readOnly()) {
        switch (cmd.type) {
            case CommandType::Set:
            case CommandType::Del:
                return "-ERR READONLY this node is a replica; write to the leader\n";
            case CommandType::Compact:
                return "-ERR READONLY a replica must not compact its log\n";
            default:
                break;
        }
    }

    switch (cmd.type) {
        case CommandType::Set:

            if (transaction_) {
                transaction_->set(cmd.key, std::move(cmd.value));
                return "+OK\n";
            }

            store_.set(cmd.key, std::move(cmd.value));
            return "+OK\n";

        case CommandType::Get: {
            std::string value;

            const bool found = transaction_ ? transaction_->get(cmd.key, value)
                                            : store_.get(cmd.key, value);
            if (!found) {
                return "$nil\n";
            }
            return "$" + value + "\n";
        }

        case CommandType::Del:
            if (transaction_) {
                transaction_->del(cmd.key);
                return "+OK\n";
            }
            store_.del(cmd.key);
            return "+OK\n";

        case CommandType::Stats:

            return "$" + store_.statsLine() + "\n";

        case CommandType::Compact: {
            const wal::CompactionResult result = store_.compact();
            if (!result.ok) {
                return "-ERR compaction failed: " + result.error + "\n";
            }
            return "$records=" + std::to_string(result.recordsWritten) +
                   " bytes_before=" + std::to_string(result.bytesBefore) +
                   " bytes_after=" + std::to_string(result.bytesAfter) + "\n";
        }

        case CommandType::Begin:

            if (transaction_) {
                return "-ERR already in a transaction\n";
            }
            transaction_ = std::make_unique<txn::Transaction>(store_);
            return "+OK\n";

        case CommandType::Commit: {
            if (!transaction_) {
                return "-ERR no transaction in progress\n";
            }
            const std::size_t operations = transaction_->pendingOperations();
            if (!transaction_->commit()) {
                return "-ERR commit failed; transaction still open\n";
            }
            transaction_.reset();
            return "$committed=" + std::to_string(operations) + "\n";
        }

        case CommandType::Rollback: {
            if (!transaction_) {
                return "-ERR no transaction in progress\n";
            }
            const std::size_t discarded = transaction_->pendingOperations();
            transaction_->rollback();
            transaction_.reset();
            return "$rolled_back=" + std::to_string(discarded) + "\n";
        }

        case CommandType::Quit:
            quit = true;
            return "+OK\n";

        case CommandType::Invalid:
        default:

            return "-ERR " + cmd.error + "\n";
    }
}
}
}
