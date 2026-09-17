#include "repl/leader.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <utility>

#include <fcntl.h>
#include <poll.h>
#include <unistd.h>
#include <sys/socket.h>

namespace kvstore {
namespace repl {
namespace {

using Clock = std::chrono::steady_clock;

constexpr std::size_t kMaxControlLine = 256;

bool writeAll(int fd, const char* data, std::size_t length) {
    std::size_t sent = 0;
    while (sent < length) {
        ssize_t n = ::write(fd, data + sent, length - sent);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        sent += static_cast<std::size_t>(n);
    }
    return true;
}

void pokeWakePipe(int fd) {
    const char byte = 'x';
    const ssize_t ignored = ::write(fd, &byte, 1);
    (void)ignored;
}

std::uint32_t percentile(const std::vector<std::uint32_t>& sorted, double fraction) {
    if (sorted.empty()) {
        return 0;
    }
    std::size_t index = static_cast<std::size_t>(fraction * static_cast<double>(sorted.size()));
    if (index >= sorted.size()) {
        index = sorted.size() - 1;
    }
    return sorted[index];
}
}

Leader::Leader(store::Store& store, const ReplConfig& config)
    : store_(store), config_(config) {}

Leader::~Leader() {
    stop();
}

void Leader::start() {
    if (!store_.logEnabled()) {
        throw std::runtime_error("replication requires a write-ahead log (--log)");
    }

    listener_ = std::make_unique<net::ListeningSocket>(config_.listenPort, 16);
    acceptThread_ = std::thread([this] { acceptLoop(); });
}

void Leader::stop() {
    std::vector<std::shared_ptr<Session>> sessions;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopping_) {
            return;
        }
        stopping_ = true;
        sessions = sessions_;
    }

    ackCv_.notify_all();

    for (const std::shared_ptr<Session>& session : sessions) {
        session->stopping.store(true, std::memory_order_relaxed);

        pokeWakePipe(session->wakeWrite.get());
        ::shutdown(session->socket.get(), SHUT_RDWR);
    }

    for (const std::shared_ptr<Session>& session : sessions) {
        if (session->thread.joinable()) {
            session->thread.join();
        }
    }

    if (acceptThread_.joinable()) {
        acceptThread_.join();
    }

    std::lock_guard<std::mutex> lock(mutex_);
    sessions_.clear();
}

void Leader::onLogReplaced() {
    std::vector<std::shared_ptr<Session>> sessions;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        sessions = sessions_;
    }
    for (const std::shared_ptr<Session>& session : sessions) {
        session->stopping.store(true, std::memory_order_relaxed);
        pokeWakePipe(session->wakeWrite.get());
        ::shutdown(session->socket.get(), SHUT_RDWR);
    }
}

void Leader::onAppend(std::uint64_t endOffset) {
    const Clock::time_point now = Clock::now();

    {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!sessions_.empty()) {
            pending_.push_back(PendingRecord{endOffset, now});
        }
    }

    wakeSessions();

    if (config_.mode != ReplMode::Sync) {
        return;
    }

    std::unique_lock<std::mutex> lock(mutex_);

    if (sessions_.empty()) {
        syncDegraded_.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    const bool acked = ackCv_.wait_for(
        lock, std::chrono::milliseconds(config_.ackTimeoutMs), [this, endOffset] {
            if (stopping_) {
                return true;
            }
            for (const std::shared_ptr<Session>& session : sessions_) {
                if (session->acked.load(std::memory_order_relaxed) >= endOffset) {
                    return true;
                }
            }
            return false;
        });

    if (!acked) {
        ackTimeouts_.fetch_add(1, std::memory_order_relaxed);
    }
}

void Leader::wakeSessions() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const std::shared_ptr<Session>& session : sessions_) {
        pokeWakePipe(session->wakeWrite.get());
    }
}

void Leader::noteAck(std::uint64_t offset) {
    while (!pending_.empty() && pending_.front().offset <= offset) {
        const PendingRecord record = pending_.front();
        pending_.pop_front();

        const auto lag = std::chrono::duration_cast<std::chrono::microseconds>(
                             Clock::now() - record.at).count();
        const std::uint64_t lagUs = lag < 0 ? 0 : static_cast<std::uint64_t>(lag);

        ++lagCount_;
        lagSumUs_ += lagUs;
        lagMaxUs_ = std::max(lagMaxUs_, lagUs);
        if (lagSamplesUs_.size() < kMaxLagSamples) {
            lagSamplesUs_.push_back(static_cast<std::uint32_t>(lagUs));
        }
    }
}

void Leader::reapFinishedSessions() {
    std::vector<std::shared_ptr<Session>> dead;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (std::size_t i = sessions_.size(); i-- > 0;) {
            if (sessions_[i]->finished.load(std::memory_order_acquire)) {
                dead.push_back(sessions_[i]);
                sessions_.erase(sessions_.begin() + static_cast<std::ptrdiff_t>(i));
            }
        }
    }

    for (const std::shared_ptr<Session>& session : dead) {
        if (session->thread.joinable()) {
            session->thread.join();
        }
    }
    if (!dead.empty()) {
        ackCv_.notify_all();
    }
}

void Leader::acceptLoop() {
    for (;;) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stopping_) {
                return;
            }
        }
        reapFinishedSessions();

        pollfd pfd{};
        pfd.fd = listener_->fd();
        pfd.events = POLLIN;

        int ready = ::poll(&pfd, 1, kPollTimeoutMs);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            std::cerr << "replication: poll() on the replication socket failed\n";
            return;
        }
        if (ready == 0) {
            continue;
        }

        net::FileDescriptor socket;
        try {
            socket = listener_->accept();
        } catch (const std::exception& e) {
            std::cerr << "replication: accept failed: " << e.what() << '\n';
            continue;
        }
        if (!socket.valid()) {
            continue;
        }
        net::setNoDelay(socket.get());

        auto session = std::make_shared<Session>();
        if (!performHandshake(*session, socket)) {
            continue;
        }
        session->socket = std::move(socket);

        int fds[2] = {-1, -1};
        if (::pipe(fds) != 0) {
            std::cerr << "replication: pipe() failed; refusing follower\n";
            continue;
        }
        session->wakeRead = net::FileDescriptor(fds[0]);
        session->wakeWrite = net::FileDescriptor(fds[1]);

        ::fcntl(session->wakeWrite.get(), F_SETFL, O_NONBLOCK);
        ::fcntl(session->wakeRead.get(), F_SETFL, O_NONBLOCK);

        int logFd = ::open(store_.log()->path().c_str(), O_RDONLY);
        if (logFd < 0) {
            std::cerr << "replication: cannot open the log for streaming: "
                      << std::strerror(errno) << '\n';
            continue;
        }
        session->logReader = net::FileDescriptor(logFd);

        connections_.fetch_add(1, std::memory_order_relaxed);

        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stopping_) {
                continue;
            }
            sessions_.push_back(session);
        }

        session->thread = std::thread([this, session] { sessionLoop(session); });
    }
}

bool Leader::performHandshake(Session& session, net::FileDescriptor& socket) {
    std::string line;
    char byte = 0;
    for (;;) {
        ssize_t n = ::read(socket.get(), &byte, 1);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (n == 0) {
            return false;
        }
        if (byte == '\n') {
            break;
        }
        line.push_back(byte);
        if (line.size() > kMaxControlLine) {
            return false;
        }
    }
    if (!line.empty() && line.back() == '\r') {
        line.pop_back();
    }

    std::string verb, token, offsetText;
    std::size_t first = line.find(' ');
    std::size_t second = (first == std::string::npos) ? std::string::npos
                                                      : line.find(' ', first + 1);
    if (first == std::string::npos || second == std::string::npos) {
        const std::string reply = "-ERR expected: SYNC <token> <offset>\n";
        writeAll(socket.get(), reply.data(), reply.size());
        return false;
    }
    verb = line.substr(0, first);
    token = line.substr(first + 1, second - first - 1);
    offsetText = line.substr(second + 1);

    if (verb != "SYNC") {
        const std::string reply = "-ERR unknown replication verb\n";
        writeAll(socket.get(), reply.data(), reply.size());
        return false;
    }

    std::uint64_t requested = 0;
    try {
        requested = std::stoull(offsetText);
    } catch (const std::exception&) {
        const std::string reply = "-ERR bad offset\n";
        writeAll(socket.get(), reply.data(), reply.size());
        return false;
    }

    const std::string leaderToken = store_.replicationToken();
    const std::uint64_t leaderOffset = store_.logOffset();

    const bool canResume = !token.empty() && token == leaderToken && requested <= leaderOffset;

    std::string reply;
    if (canResume) {
        session.sentOffset = requested;
        reply = "+RESUME " + leaderToken + " " + std::to_string(requested) + "\n";
    } else {
        session.sentOffset = 0;
        fullResyncs_.fetch_add(1, std::memory_order_relaxed);
        reply = "+FULL " + leaderToken + " 0\n";
    }

    session.acked.store(session.sentOffset, std::memory_order_relaxed);

    return writeAll(socket.get(), reply.data(), reply.size());
}

void Leader::sessionLoop(const std::shared_ptr<Session>& session) {
    Session& s = *session;

    for (;;) {
        if (s.stopping.load(std::memory_order_relaxed)) {
            break;
        }

        if (!shipPending(s)) {
            break;
        }

        pollfd fds[2]{};
        fds[0].fd = s.socket.get();
        fds[0].events = POLLIN;
        fds[1].fd = s.wakeRead.get();
        fds[1].events = POLLIN;

        int ready = ::poll(fds, 2, kPollTimeoutMs);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }

        if (fds[1].revents != 0) {
            char scratch[64];
            for (;;) {
                ssize_t drained = ::read(s.wakeRead.get(), scratch, sizeof(scratch));
                if (drained <= 0) {
                    break;
                }
                if (static_cast<std::size_t>(drained) < sizeof(scratch)) {
                    break;
                }
            }
        }

        if (fds[0].revents != 0) {
            if (!drainAcks(s)) {
                break;
            }
        }
    }

    s.finished.store(true, std::memory_order_release);

    ackCv_.notify_all();
}

bool Leader::shipPending(Session& s) {
    const std::uint64_t target = store_.logOffset();

    char buffer[kStreamChunkBytes];
    while (s.sentOffset < target) {
        if (s.stopping.load(std::memory_order_relaxed)) {
            return false;
        }
        const std::uint64_t remaining = target - s.sentOffset;
        const std::size_t want = static_cast<std::size_t>(
            remaining < kStreamChunkBytes ? remaining : kStreamChunkBytes);

        ssize_t n = ::pread(s.logReader.get(), buffer, want,
                            static_cast<off_t>(s.sentOffset));
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            std::cerr << "replication: pread on the log failed: " << std::strerror(errno) << '\n';
            return false;
        }
        if (n == 0) {
            return false;
        }

        if (!writeAll(s.socket.get(), buffer, static_cast<std::size_t>(n))) {
            return false;
        }

        s.sentOffset += static_cast<std::uint64_t>(n);
        s.bytesSent.fetch_add(static_cast<std::uint64_t>(n), std::memory_order_relaxed);
    }
    return true;
}

bool Leader::drainAcks(Session& s) {
    char buffer[256];
    ssize_t n = ::read(s.socket.get(), buffer, sizeof(buffer));
    if (n < 0) {
        if (errno == EINTR) {
            return true;
        }
        return false;
    }
    if (n == 0) {
        return false;
    }
    s.ackBuffer.append(buffer, static_cast<std::size_t>(n));

    if (s.ackBuffer.size() > kMaxControlLine) {
        return false;
    }

    std::uint64_t highest = 0;
    bool sawAck = false;

    for (;;) {
        const std::size_t newline = s.ackBuffer.find('\n');
        if (newline == std::string::npos) {
            break;
        }
        std::string line = s.ackBuffer.substr(0, newline);
        s.ackBuffer.erase(0, newline + 1);

        if (line.rfind("ACK ", 0) != 0) {
            continue;
        }
        try {
            const std::uint64_t offset = std::stoull(line.substr(4));

            if (offset > highest) {
                highest = offset;
            }
            sawAck = true;
        } catch (const std::exception&) {
        }
    }

    if (!sawAck) {
        return true;
    }

    if (highest > s.acked.load(std::memory_order_relaxed)) {
        s.acked.store(highest, std::memory_order_relaxed);
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        noteAck(highest);
    }

    ackCv_.notify_all();
    return true;
}

std::size_t Leader::followerCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return sessions_.size();
}

std::uint64_t Leader::ackedOffset() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::uint64_t best = 0;
    for (const std::shared_ptr<Session>& session : sessions_) {
        best = std::max(best, session->acked.load(std::memory_order_relaxed));
    }
    return best;
}

std::uint64_t Leader::fullResyncs() const {
    return fullResyncs_.load(std::memory_order_relaxed);
}

std::uint64_t Leader::ackTimeouts() const {
    return ackTimeouts_.load(std::memory_order_relaxed);
}

std::string Leader::statsLine() const {
    std::vector<std::uint32_t> samples;
    std::uint64_t count = 0;
    std::uint64_t sum = 0;
    std::uint64_t maxUs = 0;
    std::size_t followers = 0;
    std::uint64_t acked = 0;
    std::uint64_t sent = 0;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        samples = lagSamplesUs_;
        count = lagCount_;
        sum = lagSumUs_;
        maxUs = lagMaxUs_;
        followers = sessions_.size();
        for (const std::shared_ptr<Session>& session : sessions_) {
            acked = std::max(acked, session->acked.load(std::memory_order_relaxed));
            sent += session->bytesSent.load(std::memory_order_relaxed);
        }
    }

    std::sort(samples.begin(), samples.end());

    const std::uint64_t offset = store_.logOffset();
    std::string out =
        "repl=leader mode=" + std::string(replModeName(config_.mode)) +
        " followers=" + std::to_string(followers) +
        " repl_port=" + std::to_string(config_.listenPort) +
        " log_offset=" + std::to_string(offset) +
        " acked_offset=" + std::to_string(acked) +

        " behind_bytes=" + std::to_string(offset > acked ? offset - acked : 0) +
        " repl_bytes_sent=" + std::to_string(sent) +
        " connections=" + std::to_string(connections_.load(std::memory_order_relaxed)) +
        " full_resyncs=" + std::to_string(fullResyncs_.load(std::memory_order_relaxed)) +
        " ack_timeouts=" + std::to_string(ackTimeouts_.load(std::memory_order_relaxed)) +
        " sync_no_follower=" + std::to_string(syncDegraded_.load(std::memory_order_relaxed)) +
        " lag_samples=" + std::to_string(count);

    if (count > 0) {
        out += " lag_avg_us=" + std::to_string(sum / count) +
               " lag_p50_us=" + std::to_string(percentile(samples, 0.50)) +
               " lag_p99_us=" + std::to_string(percentile(samples, 0.99)) +
               " lag_max_us=" + std::to_string(maxUs);
    }
    return out;
}
}
}
