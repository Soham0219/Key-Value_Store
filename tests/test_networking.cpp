#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include "net/server.h"
#include "net/socket.h"
#include "pool/thread_pool.h"
#include "protocol/parser.h"
#include "store/store.h"

namespace {

int g_failures = 0;

void check(bool condition, const std::string& what, int line) {
    if (condition) {
        std::cout << "  ok   " << what << '\n';
    } else {
        std::cout << "  FAIL " << what << "  (line " << line << ")\n";
        ++g_failures;
    }
}

#define CHECK(cond) check((cond), #cond, __LINE__)

void testParser() {
    std::cout << "parser\n";
    using kvstore::protocol::CommandParser;
    using kvstore::protocol::CommandType;

    auto set = CommandParser::parse("SET name rahul");
    CHECK(set.type == CommandType::Set);
    CHECK(set.key == "name");
    CHECK(set.value == "rahul");

    auto spaced = CommandParser::parse("SET greeting hello there world");
    CHECK(spaced.value == "hello there world");

    auto lower = CommandParser::parse("get name");
    CHECK(lower.type == CommandType::Get);
    CHECK(lower.key == "name");

    auto del = CommandParser::parse("DEL name");
    CHECK(del.type == CommandType::Del);

    auto quit = CommandParser::parse("QUIT");
    CHECK(quit.type == CommandType::Quit);

    CHECK(CommandParser::parse("").type == CommandType::Invalid);
    CHECK(CommandParser::parse("   ").type == CommandType::Invalid);
    CHECK(CommandParser::parse("FROB x").type == CommandType::Invalid);
    CHECK(CommandParser::parse("GET").type == CommandType::Invalid);
    CHECK(CommandParser::parse("SET onlykey").type == CommandType::Invalid);
}

void testStore() {
    std::cout << "store\n";
    kvstore::store::Store store;
    std::string value;

    CHECK(!store.get("missing", value));

    store.set("a", "1");
    CHECK(store.get("a", value) && value == "1");

    store.set("a", "2");
    CHECK(store.get("a", value) && value == "2");
    CHECK(store.size() == 1);

    CHECK(store.del("a"));
    CHECK(!store.del("a"));
    CHECK(store.size() == 0);

    store.set("empty", "");
    CHECK(store.get("empty", value) && value.empty());
}

void testThreadPool() {
    std::cout << "thread pool\n";
    std::atomic<int> counter{0};
    constexpr int kTasks = 1000;

    {
        kvstore::pool::ThreadPool pool(4);
        bool allAccepted = true;
        for (int i = 0; i < kTasks; ++i) {
            allAccepted &= pool.submit([&counter] { counter.fetch_add(1, std::memory_order_relaxed); });
        }
        CHECK(allAccepted);
        pool.shutdown();
        CHECK(counter.load() == kTasks);
    }

    kvstore::pool::ThreadPool stopped(2);
    stopped.shutdown();
    CHECK(!stopped.submit([] {}));
}

kvstore::net::FileDescriptor connectLocal(std::uint16_t port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return kvstore::net::FileDescriptor{};
    }
    kvstore::net::FileDescriptor owned(fd);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    if (::connect(owned.get(), reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) < 0) {
        return kvstore::net::FileDescriptor{};
    }
    return owned;
}

std::string request(int fd, const std::string& line) {
    std::string out = line + "\n";
    std::size_t sent = 0;
    while (sent < out.size()) {
        ssize_t n = ::write(fd, out.data() + sent, out.size() - sent);
        if (n <= 0) {
            return "<write failed>";
        }
        sent += static_cast<std::size_t>(n);
    }

    std::string reply;
    char c = 0;
    while (reply.size() < 4096) {
        ssize_t n = ::read(fd, &c, 1);
        if (n <= 0) {
            break;
        }
        if (c == '\n') {
            break;
        }
        reply.push_back(c);
    }
    return reply;
}

void testEndToEnd() {
    std::cout << "end-to-end (two simultaneous clients)\n";
    constexpr std::uint16_t kPort = 16380;

    kvstore::net::Server server(kPort, 4);
    std::thread serverThread([&server] { server.run(); });

    kvstore::net::FileDescriptor a = connectLocal(kPort);
    kvstore::net::FileDescriptor b = connectLocal(kPort);
    CHECK(a.valid());
    CHECK(b.valid());

    CHECK(request(a.get(), "SET user:42 Rahul") == "+OK");
    CHECK(request(b.get(), "GET user:42") == "$Rahul");
    CHECK(request(b.get(), "GET nothing") == "$nil");
    CHECK(request(a.get(), "SET note hello there") == "+OK");
    CHECK(request(b.get(), "GET note") == "$hello there");
    CHECK(request(a.get(), "DEL user:42") == "+OK");
    CHECK(request(b.get(), "GET user:42") == "$nil");
    CHECK(request(a.get(), "BOGUS x").rfind("-ERR", 0) == 0);

    std::string pipelined = "SET p 1\nGET p\n";

    const ssize_t pipelinedSent = ::write(b.get(), pipelined.data(), pipelined.size());
    CHECK(pipelinedSent == static_cast<ssize_t>(pipelined.size()));
    std::string first;
    std::string second;
    char c = 0;
    int newlines = 0;
    while (newlines < 2 && ::read(b.get(), &c, 1) == 1) {
        if (c == '\n') {
            ++newlines;
        } else if (newlines == 0) {
            first.push_back(c);
        } else {
            second.push_back(c);
        }
    }
    CHECK(first == "+OK");
    CHECK(second == "$1");

    CHECK(request(a.get(), "QUIT") == "+OK");

    kvstore::net::Server::requestShutdown();
    serverThread.join();
    CHECK(true);
}
}

int main() {
    testParser();
    testStore();
    testThreadPool();
    testEndToEnd();

    std::cout << (g_failures == 0 ? "\nALL TESTS PASSED\n" : "\nFAILURES\n");
    return g_failures == 0 ? 0 : 1;
}
