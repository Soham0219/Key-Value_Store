#pragma once

#include <string>

namespace kvstore {
namespace protocol {

enum class CommandType {
    Set,
    Get,
    Del,
    Stats,
    Compact,
    Begin,
    Commit,
    Rollback,
    Quit,
    Invalid
};

struct ParsedCommand {
    CommandType type = CommandType::Invalid;
    std::string key;
    std::string value;
    std::string error;
};

class CommandParser {
public:

    static ParsedCommand parse(const std::string& line);
};
}
}
