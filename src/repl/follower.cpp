#include "repl/follower.h"

#include <cerrno>
#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>
#include <utility>

#include <poll.h>
#include <unistd.h>

#include "protocol/commands.h"
#include "wal/record.h"

namespace kvstore {
namespace repl {
namespace {

bool writeAll(int fd, const std::string& data) {
    std::size_t sent = 0;
    while (sent < data.size()) {
        ssize_t n = ::write(fd, data.data() + sent, data.size() - sent);
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

constexpr std::size_t kMaxControlLine = 256;
constexpr std::size_t kReadChunkBytes = 64 * 1024;
}

Follower::Follower(store::Store& store, const ReplConfig& config)
    : store_(store), config_(config), applier_(store) {}

Follower::~Follower() {
    stop();
}

void Follower::start() {
    linkThread_ = std::thread([this] { linkLoop(); });
}

void Follower::stop() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopping_) {
            return;
        }
        stopping_ = true;
    }

    stopCv_.notify_all();

    if (linkThread_.joinable()) {
        linkThread_.join();
    }
}

void Follower::sleepBeforeRetry() {
    std::unique_lock<std::mutex> lock(mutex_);

    stopCv_.wait_for(lock, std::chrono::milliseconds(kReconnectDelayMs),
                     [this] { return stopping_; });
}

void Follower::linkLoop() {
    for (;;) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stopping_) {
                return;
            }
        }

        connectAttempts_.fetch_add(1, std::memory_order_relaxed);
        net::FileDescriptor socket = net::connectTo(config_.peerHost, config_.peerPort);
        if (!socket.valid()) {
            sleepBeforeRetry();
            continue;
        }

        runOneConnection(std::move(socket));

        linkUp_.store(false, std::memory_order_relaxed);
        disconnects_.fetch_add(1, std::memory_order_relaxed);

        recvBuffer_.clear();
        pending_.clear();
        pendingBytes_ = 0;
        insideTransaction_ = false;
        openTransaction_.store(false, std::memory_order_relaxed);
        applier_.abandonOpenTransaction();

        sleepBeforeRetry();
    }
}

bool Follower::runOneConnection(net::FileDescriptor socket) {
    bool fullResync = false;
    if (!handshake(socket.get(), fullResync)) {
        return false;
    }

    linkUp_.store(true, std::memory_order_relaxed);
    std::cerr << "replication: link up to " << config_.peerHost << ":" << config_.peerPort
              << (fullResync ? " (FULL resync)" : " (resumed)")
              << " at offset " << store_.logOffset() << '\n';

    if (!sendAck(socket.get(), store_.logOffset())) {
        return false;
    }

    return consume(socket.get());
}

bool Follower::handshake(int fd, bool& fullResync) {
    const std::uint64_t offset = store_.logOffset();

    std::string token = loadLeaderToken();
    if (token.empty() || offset == 0) {
        token = "-";
    }

    const std::string request = "SYNC " + token + " " + std::to_string(offset) + "\n";
    if (!writeAll(fd, request)) {
        return false;
    }

    std::string line;
    for (;;) {
        char c = 0;
        ssize_t n = ::read(fd, &c, 1);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (n == 0) {
            return false;
        }
        if (c == '\n') {
            break;
        }
        line.push_back(c);
        if (line.size() > kMaxControlLine) {
            return false;
        }
    }

    if (!line.empty() && line[0] == '-') {
        std::cerr << "replication: leader refused the handshake: " << line << '\n';
        return false;
    }

    const std::size_t first = line.find(' ');
    const std::size_t second = (first == std::string::npos) ? std::string::npos
                                                            : line.find(' ', first + 1);
    if (first == std::string::npos || second == std::string::npos) {
        std::cerr << "replication: unparseable handshake reply: " << line << '\n';
        return false;
    }
    const std::string verb = line.substr(0, first);
    const std::string leaderToken = line.substr(first + 1, second - first - 1);

    if (verb == "+RESUME") {
        fullResync = false;

        return true;
    }
    if (verb != "+FULL") {
        std::cerr << "replication: unexpected handshake verb: " << verb << '\n';
        return false;
    }

    fullResync = true;
    fullResyncs_.fetch_add(1, std::memory_order_relaxed);
    try {
        store_.resetForFullResync();
    } catch (const std::exception& e) {
        std::cerr << "replication: full resync failed: " << e.what() << '\n';
        return false;
    }
    applier_.abandonOpenTransaction();

    saveLeaderToken(leaderToken);
    return true;
}

bool Follower::consume(int fd) {
    for (;;) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stopping_) {
                return true;
            }
        }

        pollfd pfd{};
        pfd.fd = fd;
        pfd.events = POLLIN;

        int ready = ::poll(&pfd, 1, kPollTimeoutMs);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (ready == 0) {
            continue;
        }

        char buffer[kReadChunkBytes];
        ssize_t n = ::read(fd, buffer, sizeof(buffer));
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (n == 0) {
            return false;
        }
        recvBuffer_.append(buffer, static_cast<std::size_t>(n));
        bytesReceived_.fetch_add(static_cast<std::uint64_t>(n), std::memory_order_relaxed);

        std::size_t offset = 0;
        for (;;) {
            std::string payload;
            std::size_t next = 0;
            const wal::ParseResult parsed = wal::parseRecord(recvBuffer_, offset, payload, next);

            if (parsed == wal::ParseResult::Incomplete) {
                break;
            }
            if (parsed == wal::ParseResult::Corrupt) {
                std::cerr << "replication: checksum failure in the replication stream;"
                             " dropping the link\n";
                return false;
            }

            std::uint64_t txnId = 0;
            if (protocol::parseBeginMarker(payload, txnId)) {
                insideTransaction_ = true;
            } else if (protocol::parseCommitMarker(payload, txnId)) {
                insideTransaction_ = false;
            }

            pendingBytes_ += payload.size();
            pending_.push_back(std::move(payload));
            offset = next;

            if (pendingBytes_ > kMaxBufferedTransactionBytes) {
                std::cerr << "replication: a single transaction exceeded the buffer limit;"
                             " dropping the link\n";
                return false;
            }
        }
        recvBuffer_.erase(0, offset);
        openTransaction_.store(insideTransaction_, std::memory_order_relaxed);

        if (!insideTransaction_ && !pending_.empty()) {
            if (!flushPending()) {
                return false;
            }
            if (!sendAck(fd, store_.logOffset())) {
                return false;
            }
        }
    }
}

bool Follower::flushPending() {
    std::uint64_t newOffset = 0;
    try {
        newOffset = store_.appendReplicatedRecords(pending_);
    } catch (const std::exception& e) {
        std::cerr << "replication: could not write to the follower's log: " << e.what() << '\n';
        return false;
    }

    for (const std::string& payload : pending_) {
        if (!applier_.apply(payload)) {
            std::cerr << "replication: unrecognised record from the leader; the two nodes"
                         " are running incompatible versions. Stopping this link.\n";
            return false;
        }
        recordsApplied_.fetch_add(1, std::memory_order_relaxed);
    }

    pending_.clear();
    pendingBytes_ = 0;

    if (newOffset != store_.logOffset()) {
        std::cerr << "replication: internal offset mismatch after applying records\n";
        return false;
    }
    return true;
}

bool Follower::sendAck(int fd, std::uint64_t offset) {
    return writeAll(fd, "ACK " + std::to_string(offset) + "\n");
}

std::string Follower::stateFilePath() const {
    return store_.log() != nullptr ? store_.log()->path() + ".replstate" : std::string();
}

std::string Follower::loadLeaderToken() const {
    const std::string path = stateFilePath();
    if (path.empty()) {
        return std::string();
    }
    std::ifstream in(path);
    if (!in) {
        return std::string();
    }
    std::string token;
    in >> token;
    return token;
}

void Follower::saveLeaderToken(const std::string& token) const {
    const std::string path = stateFilePath();
    if (path.empty()) {
        return;
    }
    std::ofstream out(path, std::ios::trunc);
    if (!out) {
        std::cerr << "replication: could not write " << path << "; a restart will full-resync\n";
        return;
    }
    out << token << '\n';
}

std::string Follower::statsLine() const {
    const std::uint64_t offset = store_.logOffset();
    return "repl=follower mode=" + std::string(replModeName(config_.mode)) +
           " link=" + std::string(linkUp_.load(std::memory_order_relaxed) ? "up" : "down") +
           " leader=" + config_.peerHost + ":" + std::to_string(config_.peerPort) +
           " offset=" + std::to_string(offset) +
           " repl_records=" + std::to_string(recordsApplied_.load(std::memory_order_relaxed)) +
           " repl_bytes=" + std::to_string(bytesReceived_.load(std::memory_order_relaxed)) +
           " connect_attempts=" + std::to_string(connectAttempts_.load(std::memory_order_relaxed)) +
           " disconnects=" + std::to_string(disconnects_.load(std::memory_order_relaxed)) +
           " full_resyncs=" + std::to_string(fullResyncs_.load(std::memory_order_relaxed)) +

           " open_txn=" +
           std::string(openTransaction_.load(std::memory_order_relaxed) ? "yes" : "no");
}
}
}
