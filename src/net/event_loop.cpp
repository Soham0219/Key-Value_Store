#include "net/event_loop.h"

#include <cerrno>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <utility>

#include <unistd.h>

namespace kvstore {
namespace net {
namespace {

constexpr std::size_t kMaxLineLength = 64 * 1024;

constexpr std::size_t kKeepBufferBytes = 64 * 1024;
}

EventLoop::EventLoop(ListeningSocket& listener,
                     store::Store& store,
                     const std::atomic<bool>& shuttingDown,
                     PollerKind pollerKind)
    : listener_(listener), store_(store), shuttingDown_(shuttingDown) {
    poller_ = makePoller(pollerKind);

    setNonBlocking(listener_.fd());

    if (!poller_->add(listener_.fd(), true, false)) {
        throw std::runtime_error("could not register the listening socket with the poller");
    }
    events_.reserve(256);
}

void EventLoop::run() {
    while (!shuttingDown_.load(std::memory_order_relaxed)) {
        const int ready = poller_->wait(events_, kWaitTimeoutMs);
        if (ready < 0) {
            std::cerr << "event loop: wait failed: " << std::strerror(errno) << '\n';
            break;
        }

        for (const ReadyEvent& event : events_) {
            if (event.fd == listener_.fd()) {
                acceptNew();
                continue;
            }

            auto it = clients_.find(event.fd);
            if (it == clients_.end()) {
                continue;
            }

            bool close = false;
            handleClient(*it->second, event, close);
            if (close) {
                pendingClose_.push_back(event.fd);
            }
        }

        for (int fd : pendingClose_) {
            closeClient(fd);
        }
        pendingClose_.clear();
    }

    std::cerr << "event loop: closing " << clients_.size() << " connection(s)\n";
    clients_.clear();
}

void EventLoop::acceptNew() {
    for (std::size_t i = 0; i < kMaxAcceptsPerEvent; ++i) {
        FileDescriptor socket;
        try {
            socket = listener_.accept();
        } catch (const std::exception& e) {
            std::cerr << "event loop: accept failed: " << e.what() << '\n';
            return;
        }
        if (!socket.valid()) {
            return;
        }

        const int fd = socket.get();
        if (!setNonBlocking(fd)) {
            std::cerr << "event loop: could not set O_NONBLOCK; dropping the connection\n";
            continue;
        }
        setNoDelay(fd);

        auto client = std::make_unique<Client>(std::move(socket), store_);
        if (!poller_->add(fd, true, false)) {
            std::cerr << "event loop: poller rejected fd " << fd << "\n";
            continue;
        }

        clients_.emplace(fd, std::move(client));
        ++accepted_;
    }
}

void EventLoop::handleClient(Client& client, const ReadyEvent& event, bool& close) {
    if (event.writable) {
        if (!flush(client)) {
            close = true;
            return;
        }
    }

    if (event.readable) {
        bool peerClosed = false;
        if (!readFrom(client, peerClosed)) {
            close = true;
            return;
        }
        processInput(client);
        if (!flush(client)) {
            close = true;
            return;
        }
        if (peerClosed) {
            client.closeAfterFlush = true;
            client.readingPaused = true;
        }
    }

    if (event.closed && !event.readable) {
        close = true;
        return;
    }

    if (client.closeAfterFlush && client.outSent >= client.outbuf.size()) {
        close = true;
        return;
    }

    updateInterest(client);
}

bool EventLoop::readFrom(Client& client, bool& peerClosed) {
    char buffer[kReadChunk];

    for (;;) {
        if (client.readingPaused) {
            return true;
        }

        const ssize_t n = ::read(client.fd.get(), buffer, sizeof(buffer));
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return true;
            }
            return false;
        }
        if (n == 0) {
            peerClosed = true;
            return true;
        }

        client.inbuf.append(buffer, static_cast<std::size_t>(n));

        return true;
    }
}

void EventLoop::processInput(Client& client) {
    for (;;) {
        if (client.closeAfterFlush) {
            return;
        }

        const std::size_t newline = client.inbuf.find('\n');
        if (newline == std::string::npos) {
            if (client.inbuf.size() > kMaxLineLength) {
                client.outbuf += "-ERR line too long\n";
                client.closeAfterFlush = true;
            }
            return;
        }

        std::string line = client.inbuf.substr(0, newline);
        client.inbuf.erase(0, newline + 1);

        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }

        bool quit = false;

        client.outbuf += client.session.handleLine(line, quit);
        ++commands_;

        if (quit) {
            client.closeAfterFlush = true;
            return;
        }

        if (client.outbuf.size() - client.outSent > kPauseReadingAboveBytes) {
            client.readingPaused = true;
            return;
        }
    }
}

bool EventLoop::flush(Client& client) {
    while (client.outSent < client.outbuf.size()) {
        const ssize_t n = ::write(client.fd.get(), client.outbuf.data() + client.outSent,
                                  client.outbuf.size() - client.outSent);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                ++shortWrites_;
                break;
            }
            return false;
        }
        client.outSent += static_cast<std::size_t>(n);
    }

    if (client.outSent >= client.outbuf.size()) {
        client.outbuf.clear();
        if (client.outbuf.capacity() > kKeepBufferBytes) {
            client.outbuf.shrink_to_fit();
        }
        client.outSent = 0;

        if (client.readingPaused) {
            client.readingPaused = false;
            processInput(client);
        }
    } else if (client.outSent > 0 && client.outSent > client.outbuf.size() / 2) {
        client.outbuf.erase(0, client.outSent);
        client.outSent = 0;
    }

    if (client.outbuf.size() - client.outSent > kMaxOutputBytes) {
        ++evictedForBacklog_;
        return false;
    }

    updateInterest(client);
    return true;
}

void EventLoop::updateInterest(Client& client) {
    const bool wantWrite = client.outSent < client.outbuf.size();
    const bool wantRead = !client.readingPaused;

    if (wantWrite == client.wantsWrite && wantRead == client.wantsRead) {
        return;
    }
    client.wantsWrite = wantWrite;
    client.wantsRead = wantRead;
    poller_->modify(client.fd.get(), wantRead, wantWrite);
}

void EventLoop::closeClient(int fd) {
    auto it = clients_.find(fd);
    if (it == clients_.end()) {
        return;
    }

    poller_->remove(fd);

    clients_.erase(it);
}
}
}
