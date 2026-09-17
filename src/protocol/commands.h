#pragma once

#include <cstdint>
#include <string>

#include "protocol/command.h"

namespace kvstore {
namespace protocol {

constexpr char kSetTag = 's';
constexpr char kDelTag = 'd';

constexpr char kBeginTag = 'b';
constexpr char kCommitTag = 'c';

std::string serializeBeginMarker(std::uint64_t txnId);
std::string serializeCommitMarker(std::uint64_t txnId);

bool parseBeginMarker(const std::string& payload, std::uint64_t& txnId);
bool parseCommitMarker(const std::string& payload, std::uint64_t& txnId);

class SetCommand final : public Command {
public:

    SetCommand(std::string key, std::string value);

    std::string serialize() const override;
    void apply(store::Store& store) const override;
    store::Mutation toMutation() const override;
    char tag() const override { return kSetTag; }

    const std::string& key() const noexcept { return key_; }
    const std::string& value() const noexcept { return value_; }

private:
    std::string key_;
    std::string value_;
};

class DelCommand final : public Command {
public:
    explicit DelCommand(std::string key);

    std::string serialize() const override;
    void apply(store::Store& store) const override;
    store::Mutation toMutation() const override;
    char tag() const override { return kDelTag; }

    const std::string& key() const noexcept { return key_; }

private:
    std::string key_;
};
}
}
