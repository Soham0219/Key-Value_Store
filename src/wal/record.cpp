#include "wal/record.h"

#include <array>

namespace kvstore {
namespace wal {
namespace {

std::array<std::uint32_t, 256> buildCrcTable() {
    std::array<std::uint32_t, 256> table{};
    for (std::uint32_t i = 0; i < 256; ++i) {
        std::uint32_t crc = i;
        for (int bit = 0; bit < 8; ++bit) {
            crc = (crc & 1u) ? ((crc >> 1) ^ 0xEDB88320u) : (crc >> 1);
        }
        table[i] = crc;
    }
    return table;
}

const std::array<std::uint32_t, 256>& crcTable() {
    static const std::array<std::uint32_t, 256> table = buildCrcTable();
    return table;
}

void appendLittleEndian32(std::string& out, std::uint32_t value) {
    out.push_back(static_cast<char>(value & 0xFFu));
    out.push_back(static_cast<char>((value >> 8) & 0xFFu));
    out.push_back(static_cast<char>((value >> 16) & 0xFFu));
    out.push_back(static_cast<char>((value >> 24) & 0xFFu));
}

std::uint32_t readLittleEndian32(const std::string& buffer, std::size_t offset) {
    return static_cast<std::uint32_t>(static_cast<unsigned char>(buffer[offset])) |
           (static_cast<std::uint32_t>(static_cast<unsigned char>(buffer[offset + 1])) << 8) |
           (static_cast<std::uint32_t>(static_cast<unsigned char>(buffer[offset + 2])) << 16) |
           (static_cast<std::uint32_t>(static_cast<unsigned char>(buffer[offset + 3])) << 24);
}
}

std::uint32_t crc32(const void* data, std::size_t length) {
    const unsigned char* bytes = static_cast<const unsigned char*>(data);
    const std::array<std::uint32_t, 256>& table = crcTable();

    std::uint32_t crc = 0xFFFFFFFFu;
    for (std::size_t i = 0; i < length; ++i) {
        crc = table[(crc ^ bytes[i]) & 0xFFu] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFFu;
}

std::string frame(const std::string& payload) {
    std::string out;
    out.reserve(kHeaderBytes + payload.size());

    appendLittleEndian32(out, static_cast<std::uint32_t>(payload.size()));

    appendLittleEndian32(out, crc32(payload.data(), payload.size()));
    out.append(payload);
    return out;
}

ParseResult parseRecord(const std::string& buffer,
                        std::size_t offset,
                        std::string& payload,
                        std::size_t& nextOffset) {
    if (offset + kHeaderBytes > buffer.size()) {
        return ParseResult::Incomplete;
    }

    const std::uint32_t length = readLittleEndian32(buffer, offset);
    const std::uint32_t expectedCrc = readLittleEndian32(buffer, offset + 4);

    if (length > kMaxPayloadBytes) {
        return ParseResult::Corrupt;
    }

    const std::size_t bodyStart = offset + kHeaderBytes;
    if (bodyStart + length > buffer.size()) {
        return ParseResult::Incomplete;
    }

    const std::uint32_t actualCrc = crc32(buffer.data() + bodyStart, length);
    if (actualCrc != expectedCrc) {
        return ParseResult::Corrupt;
    }

    payload.assign(buffer, bodyStart, length);
    nextOffset = bodyStart + length;
    return ParseResult::Ok;
}
}
}
