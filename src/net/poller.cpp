#include "net/poller.h"

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <unordered_map>

#include <poll.h>
#include <unistd.h>

#if defined(__linux__)
#include <sys/epoll.h>
#endif

#include "net/socket.h"

namespace kvstore {
namespace net {
namespace {

class PollPoller final : public Poller {
public:
    bool add(int fd, bool wantRead, bool wantWrite) override {
        if (index_.find(fd) != index_.end()) {
            return false;
        }
        pollfd entry{};
        entry.fd = fd;
        entry.events = eventsFor(wantRead, wantWrite);
        index_[fd] = fds_.size();
        fds_.push_back(entry);
        return true;
    }

    bool modify(int fd, bool wantRead, bool wantWrite) override {
        auto it = index_.find(fd);
        if (it == index_.end()) {
            return false;
        }
        fds_[it->second].events = eventsFor(wantRead, wantWrite);
        return true;
    }

    bool remove(int fd) override {
        auto it = index_.find(fd);
        if (it == index_.end()) {
            return false;
        }
        const std::size_t slot = it->second;
        const std::size_t last = fds_.size() - 1;
        if (slot != last) {
            fds_[slot] = fds_[last];
            index_[fds_[slot].fd] = slot;
        }
        fds_.pop_back();
        index_.erase(it);
        return true;
    }

    int wait(std::vector<ReadyEvent>& out, int timeoutMs) override {
        out.clear();
        if (fds_.empty()) {
            ::poll(nullptr, 0, timeoutMs);
            return 0;
        }

        int ready = ::poll(fds_.data(), static_cast<nfds_t>(fds_.size()), timeoutMs);
        if (ready < 0) {
            if (errno == EINTR) {
                return 0;
            }
            return -1;
        }
        if (ready == 0) {
            return 0;
        }

        out.reserve(static_cast<std::size_t>(ready));
        for (const pollfd& entry : fds_) {
            if (entry.revents == 0) {
                continue;
            }
            ReadyEvent event;
            event.fd = entry.fd;
            event.readable = (entry.revents & POLLIN) != 0;
            event.writable = (entry.revents & POLLOUT) != 0;

            event.closed = (entry.revents & (POLLHUP | POLLERR | POLLNVAL)) != 0;
            out.push_back(event);
            if (out.size() == static_cast<std::size_t>(ready)) {
                break;
            }
        }
        return static_cast<int>(out.size());
    }

    const char* name() const override { return "poll"; }
    std::size_t size() const override { return fds_.size(); }

private:
    static short eventsFor(bool wantRead, bool wantWrite) {
        short events = 0;
        if (wantRead) {
            events = static_cast<short>(events | POLLIN);
        }
        if (wantWrite) {
            events = static_cast<short>(events | POLLOUT);
        }
        return events;
    }

    std::vector<pollfd> fds_;
    std::unordered_map<int, std::size_t> index_;
};

#if defined(__linux__)

std::runtime_error syscallError(const char* what, int err) {
    return std::runtime_error(std::string(what) + " failed: " + std::strerror(err));
}

class EpollPoller final : public Poller {
public:
    EpollPoller() {
        int fd = ::epoll_create1(EPOLL_CLOEXEC);
        if (fd < 0) {
            throw syscallError("epoll_create1", errno);
        }
        epollFd_ = FileDescriptor(fd);
        events_.resize(kMaxEventsPerWait);
    }

    bool add(int fd, bool wantRead, bool wantWrite) override {
        epoll_event event{};
        event.events = eventsFor(wantRead, wantWrite);
        event.data.fd = fd;
        if (::epoll_ctl(epollFd_.get(), EPOLL_CTL_ADD, fd, &event) != 0) {
            return false;
        }
        ++registered_;
        return true;
    }

    bool modify(int fd, bool wantRead, bool wantWrite) override {
        epoll_event event{};
        event.events = eventsFor(wantRead, wantWrite);
        event.data.fd = fd;
        return ::epoll_ctl(epollFd_.get(), EPOLL_CTL_MOD, fd, &event) == 0;
    }

    bool remove(int fd) override {
        if (::epoll_ctl(epollFd_.get(), EPOLL_CTL_DEL, fd, nullptr) != 0) {
            return false;
        }
        if (registered_ > 0) {
            --registered_;
        }
        return true;
    }

    int wait(std::vector<ReadyEvent>& out, int timeoutMs) override {
        out.clear();
        int ready = ::epoll_wait(epollFd_.get(), events_.data(),
                                 static_cast<int>(events_.size()), timeoutMs);
        if (ready < 0) {
            if (errno == EINTR) {
                return 0;
            }
            return -1;
        }

        out.reserve(static_cast<std::size_t>(ready));
        for (int i = 0; i < ready; ++i) {
            const epoll_event& source = events_[static_cast<std::size_t>(i)];
            ReadyEvent event;
            event.fd = source.data.fd;
            event.readable = (source.events & EPOLLIN) != 0;
            event.writable = (source.events & EPOLLOUT) != 0;

            event.closed = (source.events & (EPOLLHUP | EPOLLERR)) != 0;
            if ((source.events & EPOLLRDHUP) != 0) {
                event.readable = true;
            }
            out.push_back(event);
        }
        return ready;
    }

    const char* name() const override { return "epoll"; }
    std::size_t size() const override { return registered_; }

    static constexpr std::size_t kMaxEventsPerWait = 1024;

private:
    static std::uint32_t eventsFor(bool wantRead, bool wantWrite) {
        std::uint32_t events = 0;
        if (wantRead) {
            events |= EPOLLIN | EPOLLRDHUP;
        }
        if (wantWrite) {
            events |= EPOLLOUT;
        }

        return events;
    }

    FileDescriptor epollFd_;
    std::vector<epoll_event> events_;
    std::size_t registered_ = 0;
};

#endif
}

Poller::~Poller() = default;

bool parsePollerKind(const std::string& text, PollerKind& out) {
    if (text == "auto") {
        out = PollerKind::Auto;
    } else if (text == "poll") {
        out = PollerKind::Poll;
    } else if (text == "epoll") {
        out = PollerKind::Epoll;
    } else {
        return false;
    }
    return true;
}

const char* pollerKindName(PollerKind kind) {
    switch (kind) {
        case PollerKind::Auto:  return "auto";
        case PollerKind::Poll:  return "poll";
        case PollerKind::Epoll: return "epoll";
    }
    return "unknown";
}

bool epollAvailable() {
#if defined(__linux__)
    return true;
#else
    return false;
#endif
}

std::unique_ptr<Poller> makePoller(PollerKind kind) {
    if (kind == PollerKind::Epoll && !epollAvailable()) {
        throw std::runtime_error(
            "--poller epoll requires Linux; this build has no epoll backend "
            "(use --poller poll, or run in Docker)");
    }

#if defined(__linux__)
    if (kind == PollerKind::Auto || kind == PollerKind::Epoll) {
        return std::make_unique<EpollPoller>();
    }
#endif
    return std::make_unique<PollPoller>();
}
}
}
