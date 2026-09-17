#include "protocol/command.h"

#include <cstdint>

#include "protocol/commands.h"

namespace kvstore {
namespace protocol {
namespace {

bool readU32(const std::string& data, std::size_t& offset, std::uint32_t& out) {
    if (offset + 4 > data.size()) {
        return false;
    }

    out = static_cast<std::uint32_t>(static_cast<unsigned char>(data[offset])) |
          (static_cast<std::uint32_t>(static_cast<unsigned char>(data[offset + 1])) << 8) |
          (static_cast<std::uint32_t>(static_cast<unsigned char>(data[offset + 2])) << 16) |
          (static_cast<std::uint32_t>(static_cast<unsigned char>(data[offset + 3])) << 24);
    offset += 4;
    return true;
}

bool readBlob(const std::string& data, std::size_t& offset, std::string& out) {
    std::uint32_t length = 0;
    if (!readU32(data, offset, length)) {
        return false;
    }
    if (offset + length > data.size()) {
        return false;
    }
    out.assign(data, offset, length);
    offset += length;
    return true;
}
}

Command::~Command() = default;

std::unique_ptr<Command> deserializeCommand(const std::string& payload) {
    if (payload.empty()) {
        return nullptr;
    }

    std::size_t offset = 1;
    switch (payload[0]) {
        case kSetTag: {
            std::string key;
            std::string value;
            if (!readBlob(payload, offset, key) || !readBlob(payload, offset, value)) {
                return nullptr;
            }

            if (offset != payload.size()) {
                return nullptr;
            }
            return std::make_unique<SetCommand>(std::move(key), std::move(value));
        }
        case kDelTag: {
            std::string key;
            if (!readBlob(payload, offset, key)) {
                return nullptr;
            }
            if (offset != payload.size()) {
                return nullptr;
            }
            return std::make_unique<DelCommand>(std::move(key));
        }
        default:
            return nullptr;
    }
}
}
}
