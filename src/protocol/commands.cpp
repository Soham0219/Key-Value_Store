#include "protocol/commands.h"

#include <cstdint>
#include <utility>

#include "store/store.h"

namespace kvstore {
namespace protocol {
namespace {

void appendU32(std::string& out, std::uint32_t value) {
    out.push_back(static_cast<char>(value & 0xFFu));
    out.push_back(static_cast<char>((value >> 8) & 0xFFu));
    out.push_back(static_cast<char>((value >> 16) & 0xFFu));
    out.push_back(static_cast<char>((value >> 24) & 0xFFu));
}

void appendBlob(std::string& out, const std::string& blob) {
    appendU32(out, static_cast<std::uint32_t>(blob.size()));
    out.append(blob);
}

void appendU64(std::string& out, std::uint64_t value) {
    for (int shift = 0; shift < 64; shift += 8) {
        out.push_back(static_cast<char>((value >> shift) & 0xFFu));
    }
}

bool parseMarker(const std::string& payload, char expectedTag, std::uint64_t& txnId) {
    if (payload.size() != 9 || payload[0] != expectedTag) {
        return false;
    }
    std::uint64_t value = 0;
    for (int i = 0; i < 8; ++i) {
        value |= static_cast<std::uint64_t>(static_cast<unsigned char>(payload[1 + i])) << (i * 8);
    }
    txnId = value;
    return true;
}
}

std::string serializeBeginMarker(std::uint64_t txnId) {
    std::string out;
    out.reserve(9);
    out.push_back(kBeginTag);
    appendU64(out, txnId);
    return out;
}

std::string serializeCommitMarker(std::uint64_t txnId) {
    std::string out;
    out.reserve(9);
    out.push_back(kCommitTag);
    appendU64(out, txnId);
    return out;
}

bool parseBeginMarker(const std::string& payload, std::uint64_t& txnId) {
    return parseMarker(payload, kBeginTag, txnId);
}

bool parseCommitMarker(const std::string& payload, std::uint64_t& txnId) {
    return parseMarker(payload, kCommitTag, txnId);
}

SetCommand::SetCommand(std::string key, std::string value)
    : key_(std::move(key)), value_(std::move(value)) {}

std::string SetCommand::serialize() const {
    std::string out;

    out.reserve(1 + 4 + key_.size() + 4 + value_.size());
    out.push_back(kSetTag);
    appendBlob(out, key_);
    appendBlob(out, value_);
    return out;
}

store::Mutation SetCommand::toMutation() const {
    store::Mutation mutation;
    mutation.erase = false;
    mutation.key = key_;
    mutation.value = value_;
    return mutation;
}

void SetCommand::apply(store::Store& store) const {
    store.applyFromLog(key_, value_);
}

DelCommand::DelCommand(std::string key) : key_(std::move(key)) {}

std::string DelCommand::serialize() const {
    std::string out;
    out.reserve(1 + 4 + key_.size());
    out.push_back(kDelTag);
    appendBlob(out, key_);
    return out;
}

store::Mutation DelCommand::toMutation() const {
    store::Mutation mutation;
    mutation.erase = true;
    mutation.key = key_;
    return mutation;
}

void DelCommand::apply(store::Store& store) const {
    store.eraseFromLog(key_);
}
}
}
