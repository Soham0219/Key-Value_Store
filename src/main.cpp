#include <cstdlib>
#include <cstring>
#include <exception>
#include <iostream>
#include <string>

#include <csignal>

#include "net/server.h"
#include "net/poller.h"
#include "repl/replicator.h"
#include "store/sharded_map.h"
#include "store/store.h"
#include "pool/task_queue.h"
#include "wal/wal.h"

namespace {

constexpr std::uint16_t kDefaultPort = 6380;
constexpr std::size_t kDefaultThreads = 4;

void printUsage(const char* program) {
    std::cerr << "usage: " << program
              << " [--port N] [--threads N] [--lock-mode MODE] [--shards N]"
                 " [--max-memory BYTES]\n"
                 "       [--log PATH] [--fsync MODE] [--fsync-interval-ms N]\n"
                 "       [--schedule POLICY] [--aging]\n"
                 "       [--role ROLE] [--repl-port N] [--peer HOST:PORT]\n"
                 "       [--repl-mode MODE] [--repl-ack-timeout-ms N]\n"
                 "       [--io MODE] [--poller BACKEND]\n"
              << "  --port       TCP port to listen on (default " << kDefaultPort << ")\n"
              << "  --threads    worker threads, also the max simultaneous clients (default "
              << kDefaultThreads << ")\n"

              << "  --lock-mode  none | global-mutex | global-shared | sharded (default sharded)\n"
              << "               'none' is UNSAFE and exists only to demonstrate the race\n"
              << "  --shards     shard count, rounded up to a power of two (default "
              << kvstore::store::Store::kDefaultShardCount << "; only used by --lock-mode sharded)\n"

              << "  --max-memory approximate byte budget; 0 = unlimited (default 0)\n"
              << "               non-zero enables exact per-shard LRU eviction, which makes\n"
              << "               every GET take an exclusive lock -- see README\n"

              << "  --log        write-ahead log file; omit for a purely in-memory store\n"
              << "               the file is REPLAYED at startup to rebuild state\n"
              << "  --fsync      always | interval | never (default always)\n"
              << "               always   = durable per write, slowest\n"
              << "               interval = a background thread syncs every N ms; you can\n"
              << "                          lose up to N ms of acknowledged writes to a\n"
              << "                          POWER CUT (not to a process crash)\n"
              << "               never    = syncs only on clean shutdown\n"
              << "  --fsync-interval-ms  interval for --fsync interval (default 1000)\n"

              << "  --schedule   fcfs | priority | rr (default fcfs)\n"
              << "               affects the thread pool's task ordering. NOTE: the server\n"
              << "               runs one long-lived task per connection, so the queue is\n"
              << "               rarely deep enough for this to matter -- see README\n"
              << "  --aging      with --schedule priority, promote tasks that have been\n"
              << "               passed over, so low-priority work cannot starve\n"

              << "  --role       standalone | leader | follower (default standalone)\n"
              << "               leader and follower both REQUIRE --log: replication ships\n"
              << "               the log, so a node without one has nothing to send or store\n"
              << "  --repl-port  leader: the port followers connect to (default 6390).\n"
              << "               A SEPARATE port from --port; replication is not the client\n"
              << "               protocol, it is a raw framed record stream\n"
              << "  --peer       follower: the leader's HOST:PORT (default 127.0.0.1:6390).\n"
              << "               HOST must be a numeric IPv4 address -- no DNS, deliberately\n"
              << "  --repl-mode  async | sync (default async)\n"
              << "               async = reply to the client as soon as the record is in THIS\n"
              << "                       node's log. Losing this machine loses writes the\n"
              << "                       follower had not received yet.\n"
              << "               sync  = wait for a follower to acknowledge before replying.\n"
              << "                       An acknowledged write is on two machines. Costs a\n"
              << "                       network round trip on EVERY write -- see README\n"

              << "  --io         threads | loop (default threads)\n"
              << "               threads = one worker thread per client, blocking reads.\n"
              << "                         Uses every core. Maximum concurrent clients is\n"
              << "                         --threads, and a thread costs a stack plus a\n"
              << "                         context switch per request.\n"
              << "               loop    = ONE thread, non-blocking sockets, a readiness\n"
              << "                         poller. Thousands of connections on one core --\n"
              << "                         but nothing may ever block, and COMPACT, STATS\n"
              << "                         and --repl-mode sync all do. See README.\n"
              << "  --poller     auto | poll | epoll (default auto; only used with --io loop)\n"
              << "               poll  = POSIX, works everywhere, costs O(connections) per\n"
              << "                       wait whether or not anything happened\n"
              << "               epoll = Linux only, costs O(ready). The difference is the\n"
              << "                       whole point of --io loop; run both and compare\n"
              << "  --repl-ack-timeout-ms  how long sync mode waits before giving up on an\n"
              << "               acknowledgement and replying anyway (default 1000). The\n"
              << "               fallback is counted in STATS as ack_timeouts, because a\n"
              << "               leader that has silently degraded to async is giving you a\n"
              << "               guarantee you no longer have\n";
}

void handleTerminationSignal(int ) {
    kvstore::net::Server::requestShutdown();
}

bool installHandler(int signum, void (*handler)(int)) {
    struct sigaction action {};
    action.sa_handler = handler;
    sigemptyset(&action.sa_mask);
    action.sa_flags = 0;

    return ::sigaction(signum, &action, nullptr) == 0;
}

bool parseUnsigned(const char* text, unsigned long maxValue, unsigned long& out) {
    if (text == nullptr || *text == '\0') {
        return false;
    }
    char* end = nullptr;
    unsigned long value = std::strtoul(text, &end, 10);
    if (*end != '\0') {
        return false;
    }
    if (value == 0 || value > maxValue) {
        return false;
    }
    out = value;
    return true;
}
}

int main(int argc, char** argv) {
    unsigned long port = kDefaultPort;
    unsigned long threads = kDefaultThreads;

    kvstore::store::LockMode lockMode = kvstore::store::LockMode::Sharded;
    unsigned long shards = kvstore::store::Store::kDefaultShardCount;
    unsigned long maxMemory = 0;
    std::string logPath;
    kvstore::wal::FsyncMode fsyncMode = kvstore::wal::FsyncMode::Always;
    unsigned long fsyncIntervalMs = 1000;
    kvstore::pool::SchedulingPolicy schedulingPolicy = kvstore::pool::SchedulingPolicy::Fcfs;
    bool aging = false;
    kvstore::repl::ReplConfig replConfig;
    kvstore::net::IoConfig ioConfig;

    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            if (!parseUnsigned(argv[++i], 65535, port)) {
                std::cerr << "invalid --port value\n";
                return EXIT_FAILURE;
            }
        } else if (std::strcmp(argv[i], "--threads") == 0 && i + 1 < argc) {
            if (!parseUnsigned(argv[++i], 1024, threads)) {
                std::cerr << "invalid --threads value\n";
                return EXIT_FAILURE;
            }
        } else if (std::strcmp(argv[i], "--lock-mode") == 0 && i + 1 < argc) {
            if (!kvstore::store::parseLockMode(argv[++i], lockMode)) {
                std::cerr << "invalid --lock-mode value\n";
                printUsage(argv[0]);
                return EXIT_FAILURE;
            }
        } else if (std::strcmp(argv[i], "--shards") == 0 && i + 1 < argc) {
            if (!parseUnsigned(argv[++i], 4096, shards)) {
                std::cerr << "invalid --shards value\n";
                return EXIT_FAILURE;
            }
        } else if (std::strcmp(argv[i], "--max-memory") == 0 && i + 1 < argc) {
            char* end = nullptr;
            maxMemory = std::strtoul(argv[++i], &end, 10);
            if (end == argv[i] || *end != '\0') {
                std::cerr << "invalid --max-memory value (plain bytes, e.g. 1048576)\n";
                return EXIT_FAILURE;
            }
        } else if (std::strcmp(argv[i], "--log") == 0 && i + 1 < argc) {
            logPath = argv[++i];
            if (logPath.empty()) {
                std::cerr << "--log requires a path\n";
                return EXIT_FAILURE;
            }
        } else if (std::strcmp(argv[i], "--fsync") == 0 && i + 1 < argc) {
            if (!kvstore::wal::parseFsyncMode(argv[++i], fsyncMode)) {
                std::cerr << "invalid --fsync value (always | interval | never)\n";
                return EXIT_FAILURE;
            }
        } else if (std::strcmp(argv[i], "--fsync-interval-ms") == 0 && i + 1 < argc) {
            if (!parseUnsigned(argv[++i], 60000, fsyncIntervalMs)) {
                std::cerr << "invalid --fsync-interval-ms value\n";
                return EXIT_FAILURE;
            }
        } else if (std::strcmp(argv[i], "--schedule") == 0 && i + 1 < argc) {
            if (!kvstore::pool::parseSchedulingPolicy(argv[++i], schedulingPolicy)) {
                std::cerr << "invalid --schedule value (fcfs | priority | rr)\n";
                return EXIT_FAILURE;
            }
        } else if (std::strcmp(argv[i], "--aging") == 0) {
            aging = true;
        } else if (std::strcmp(argv[i], "--role") == 0 && i + 1 < argc) {
            if (!kvstore::repl::parseRole(argv[++i], replConfig.role)) {
                std::cerr << "invalid --role value (standalone | leader | follower)\n";
                return EXIT_FAILURE;
            }
        } else if (std::strcmp(argv[i], "--repl-port") == 0 && i + 1 < argc) {
            unsigned long value = 0;
            if (!parseUnsigned(argv[++i], 65535, value)) {
                std::cerr << "invalid --repl-port value\n";
                return EXIT_FAILURE;
            }
            replConfig.listenPort = static_cast<std::uint16_t>(value);
        } else if (std::strcmp(argv[i], "--peer") == 0 && i + 1 < argc) {
            const std::string peer = argv[++i];
            const std::size_t colon = peer.rfind(':');
            if (colon == std::string::npos || colon == 0 || colon + 1 >= peer.size()) {
                std::cerr << "invalid --peer value (expected HOST:PORT, e.g. 127.0.0.1:6390)\n";
                return EXIT_FAILURE;
            }
            unsigned long value = 0;
            if (!parseUnsigned(peer.c_str() + colon + 1, 65535, value)) {
                std::cerr << "invalid port in --peer\n";
                return EXIT_FAILURE;
            }
            replConfig.peerHost = peer.substr(0, colon);
            replConfig.peerPort = static_cast<std::uint16_t>(value);
        } else if (std::strcmp(argv[i], "--repl-mode") == 0 && i + 1 < argc) {
            if (!kvstore::repl::parseReplMode(argv[++i], replConfig.mode)) {
                std::cerr << "invalid --repl-mode value (async | sync)\n";
                return EXIT_FAILURE;
            }
        } else if (std::strcmp(argv[i], "--io") == 0 && i + 1 < argc) {
            if (!kvstore::net::parseIoMode(argv[++i], ioConfig.mode)) {
                std::cerr << "invalid --io value (threads | loop)\n";
                return EXIT_FAILURE;
            }
        } else if (std::strcmp(argv[i], "--poller") == 0 && i + 1 < argc) {
            if (!kvstore::net::parsePollerKind(argv[++i], ioConfig.poller)) {
                std::cerr << "invalid --poller value (auto | poll | epoll)\n";
                return EXIT_FAILURE;
            }
        } else if (std::strcmp(argv[i], "--repl-ack-timeout-ms") == 0 && i + 1 < argc) {
            unsigned long value = 0;
            if (!parseUnsigned(argv[++i], 60000, value)) {
                std::cerr << "invalid --repl-ack-timeout-ms value\n";
                return EXIT_FAILURE;
            }
            replConfig.ackTimeoutMs = static_cast<int>(value);
        } else {
            printUsage(argv[0]);
            return EXIT_FAILURE;
        }
    }

    if (lockMode == kvstore::store::LockMode::None) {
        std::cerr << "WARNING: --lock-mode none disables ALL locking. The store WILL corrupt "
                     "under concurrent writes. This mode exists to demonstrate the race.\n";
    }

    if (!installHandler(SIGPIPE, SIG_IGN)) {
        std::cerr << "failed to ignore SIGPIPE\n";
        return EXIT_FAILURE;
    }
    if (!installHandler(SIGINT, handleTerminationSignal)) {
        std::cerr << "failed to install SIGINT handler\n";
        return EXIT_FAILURE;
    }
    if (!installHandler(SIGTERM, handleTerminationSignal)) {
        std::cerr << "failed to install SIGTERM handler\n";
        return EXIT_FAILURE;
    }

    if (logPath.empty()) {
        std::cerr << "NOTE: no --log given; this store is purely in-memory and all data "
                     "is lost when the process exits.\n";
    }

    if (replConfig.role != kvstore::repl::Role::Standalone && logPath.empty()) {
        std::cerr << "--role " << kvstore::repl::roleName(replConfig.role)
                  << " requires --log: replication works by shipping the write-ahead log\n";
        return EXIT_FAILURE;
    }
    if (replConfig.role == kvstore::repl::Role::Leader &&
        replConfig.listenPort == static_cast<std::uint16_t>(port)) {
        std::cerr << "--repl-port must differ from --port\n";
        return EXIT_FAILURE;
    }
    if (replConfig.role == kvstore::repl::Role::Follower &&
        replConfig.mode == kvstore::repl::ReplMode::Sync) {
        std::cerr << "NOTE: --repl-mode is a leader setting; it has no effect on a follower.\n";
    }

    if (ioConfig.poller == kvstore::net::PollerKind::Epoll && !kvstore::net::epollAvailable()) {
        std::cerr << "--poller epoll requires Linux. Use --poller poll, or run in Docker.\n";
        return EXIT_FAILURE;
    }
    if (ioConfig.mode == kvstore::net::IoMode::EventLoop) {
        if (replConfig.mode == kvstore::repl::ReplMode::Sync &&
            replConfig.role == kvstore::repl::Role::Leader) {
            std::cerr << "WARNING: --io loop with --repl-mode sync serialises the ENTIRE "
                         "server behind every write's replication round trip. One thread "
                         "cannot wait for the network and serve clients at the same time.\n";
        }
        if (threads != kDefaultThreads) {
            std::cerr << "NOTE: --threads is ignored with --io loop; the event loop is one thread.\n";
        }
    }

    kvstore::store::StoreConfig storeConfig;
    storeConfig.lockMode = lockMode;
    storeConfig.shardCount = static_cast<std::size_t>(shards);
    storeConfig.maxMemoryBytes = static_cast<std::size_t>(maxMemory);
    storeConfig.logPath = logPath;
    storeConfig.fsyncMode = fsyncMode;
    storeConfig.fsyncIntervalMs = static_cast<int>(fsyncIntervalMs);

    storeConfig.readOnly = (replConfig.role == kvstore::repl::Role::Follower);

    try {
        kvstore::net::Server server(static_cast<std::uint16_t>(port),
                                    static_cast<std::size_t>(threads),
                                    storeConfig,
                                    schedulingPolicy,
                                    aging,
                                    replConfig,
                                    ioConfig);
        server.run();
    } catch (const std::exception& e) {
        std::cerr << "fatal: " << e.what() << '\n';
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
