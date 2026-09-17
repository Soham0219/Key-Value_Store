#include "parser.h"

#include <cctype>
#include <cstddef>

namespace kvstore {
namespace protocol {
namespace {

std::string upper(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    for (char c : s) {
        out.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(c))));
    }
    return out;
}

std::size_t skipSpaces(const std::string& s, std::size_t from) {
    while (from < s.size() && s[from] == ' ') {
        ++from;
    }
    return (from < s.size()) ? from : std::string::npos;
}

ParsedCommand invalid(std::string why) {
    ParsedCommand cmd;
    cmd.type = CommandType::Invalid;
    cmd.error = std::move(why);
    return cmd;
}
}

ParsedCommand CommandParser::parse(const std::string& line) {
    std::size_t verbStart = skipSpaces(line, 0);
    if (verbStart == std::string::npos) {
        return invalid("empty command");
    }

    std::size_t verbEnd = line.find(' ', verbStart);
    std::string verb = upper(line.substr(verbStart, verbEnd - verbStart));

    if (verb == "STATS") {
        ParsedCommand cmd;
        cmd.type = CommandType::Stats;
        return cmd;
    }

    if (verb == "COMPACT") {
        ParsedCommand cmd;
        cmd.type = CommandType::Compact;
        return cmd;
    }

    if (verb == "BEGIN") {
        ParsedCommand cmd;
        cmd.type = CommandType::Begin;
        return cmd;
    }
    if (verb == "COMMIT") {
        ParsedCommand cmd;
        cmd.type = CommandType::Commit;
        return cmd;
    }
    if (verb == "ROLLBACK") {
        ParsedCommand cmd;
        cmd.type = CommandType::Rollback;
        return cmd;
    }

    if (verb == "QUIT") {
        ParsedCommand cmd;
        cmd.type = CommandType::Quit;
        return cmd;
    }

    bool needsValue = (verb == "SET");
    bool known = needsValue || verb == "GET" || verb == "DEL";
    if (!known) {
        return invalid("unknown command '" + verb + "'");
    }

    if (verbEnd == std::string::npos) {
        return invalid(verb + " requires a key");
    }
    std::size_t keyStart = skipSpaces(line, verbEnd);
    if (keyStart == std::string::npos) {
        return invalid(verb + " requires a key");
    }
    std::size_t keyEnd = line.find(' ', keyStart);

    ParsedCommand cmd;
    cmd.key = line.substr(keyStart, keyEnd - keyStart);

    if (!needsValue) {
        cmd.type = (verb == "GET") ? CommandType::Get : CommandType::Del;
        return cmd;
    }

    if (keyEnd == std::string::npos) {
        return invalid("SET requires a value");
    }
    std::size_t valueStart = skipSpaces(line, keyEnd);
    if (valueStart == std::string::npos) {
        return invalid("SET requires a value");
    }

    cmd.type = CommandType::Set;
    cmd.value = line.substr(valueStart);
    return cmd;
}
}
}
