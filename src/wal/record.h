#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace kvstore {
namespace wal {

constexpr std::size_t kHeaderBytes = 8;

constexpr std::uint32_t kMaxPayloadBytes = 64u * 1024u * 1024u;

std::uint32_t crc32(const void* data, std::size_t length);

std::string frame(const std::string& payload);

enum class ParseResult {
    Ok,
    Incomplete,
    Corrupt
};

ParseResult parseRecord(const std::string& buffer,
                        std::size_t offset,
                        std::string& payload,
                        std::size_t& nextOffset);
}
}
