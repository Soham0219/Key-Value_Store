#include "repl/replicator.h"

namespace kvstore {
namespace repl {

Replicator::~Replicator() = default;

bool parseRole(const std::string& text, Role& out) {
    if (text == "standalone" || text == "none") {
        out = Role::Standalone;
    } else if (text == "leader" || text == "primary" || text == "master") {
        out = Role::Leader;
    } else if (text == "follower" || text == "replica" || text == "slave") {
        out = Role::Follower;
    } else {
        return false;
    }
    return true;
}

const char* roleName(Role role) {
    switch (role) {
        case Role::Standalone: return "standalone";
        case Role::Leader:     return "leader";
        case Role::Follower:   return "follower";
    }
    return "unknown";
}

bool parseReplMode(const std::string& text, ReplMode& out) {
    if (text == "async") {
        out = ReplMode::Async;
    } else if (text == "sync") {
        out = ReplMode::Sync;
    } else {
        return false;
    }
    return true;
}

const char* replModeName(ReplMode mode) {
    switch (mode) {
        case ReplMode::Async: return "async";
        case ReplMode::Sync:  return "sync";
    }
    return "unknown";
}
}
}
