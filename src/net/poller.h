#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

namespace kvstore {
namespace net {

struct ReadyEvent {
    int fd = -1;
    bool readable = false;
    bool writable = false;

    bool closed = false;
};

class Poller {
public:
    virtual ~Poller();

    virtual bool add(int fd, bool wantRead, bool wantWrite) = 0;

    virtual bool modify(int fd, bool wantRead, bool wantWrite) = 0;

    virtual bool remove(int fd) = 0;

    virtual int wait(std::vector<ReadyEvent>& out, int timeoutMs) = 0;

    virtual const char* name() const = 0;

    virtual std::size_t size() const = 0;

protected:
    Poller() = default;
    Poller(const Poller&) = delete;
    Poller& operator=(const Poller&) = delete;
};

enum class PollerKind {
    Auto,
    Poll,
    Epoll
};

bool parsePollerKind(const std::string& text, PollerKind& out);
const char* pollerKindName(PollerKind kind);

bool epollAvailable();

std::unique_ptr<Poller> makePoller(PollerKind kind);
}
}
