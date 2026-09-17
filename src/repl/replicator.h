#pragma once

#include <cstdint>
#include <string>

namespace kvstore {
namespace repl {

enum class Role {
    Standalone,
    Leader,
    Follower
};

bool parseRole(const std::string& text, Role& out);
const char* roleName(Role role);

enum class ReplMode {
    Async,
    Sync
};

bool parseReplMode(const std::string& text, ReplMode& out);
const char* replModeName(ReplMode mode);

struct ReplConfig {
    Role role = Role::Standalone;

    std::uint16_t listenPort = 6390;

    std::string peerHost = "127.0.0.1";
    std::uint16_t peerPort = 6390;

    ReplMode mode = ReplMode::Async;

    int ackTimeoutMs = 1000;
};

class Replicator {
public:
    virtual ~Replicator();

    virtual void start() = 0;

    virtual void stop() = 0;

    virtual void onAppend(std::uint64_t endOffset) = 0;

    virtual void onLogReplaced() = 0;

    virtual std::string statsLine() const = 0;

    virtual const char* describe() const = 0;

protected:
    Replicator() = default;
    Replicator(const Replicator&) = delete;
    Replicator& operator=(const Replicator&) = delete;
};
}
}
