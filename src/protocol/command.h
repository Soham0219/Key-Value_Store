#pragma once

#include <memory>
#include <string>

#include "store/mutation.h"

namespace kvstore {
namespace store {

class Store;
}

namespace protocol {

class Command {
public:

    virtual ~Command();

    virtual std::string serialize() const = 0;

    virtual void apply(store::Store& store) const = 0;

    virtual char tag() const = 0;

    virtual store::Mutation toMutation() const = 0;

protected:
    Command() = default;
    Command(const Command&) = delete;
    Command& operator=(const Command&) = delete;
};

std::unique_ptr<Command> deserializeCommand(const std::string& payload);
}
}
