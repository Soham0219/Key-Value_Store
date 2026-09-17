#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include <sys/socket.h>
#include <unistd.h>

#include "net/event_loop.h"
#include "net/poller.h"
#include "net/socket.h"
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

using kvstore::net::EventLoop;
using kvstore::net::FileDescriptor;
using kvstore::net::ListeningSocket;
using kvstore::net::Poller;
using kvstore::net::PollerKind;
using kvstore::net::ReadyEvent;
using kvstore::store::Store;
using kvstore::store::StoreConfig;

std::uint16_t nextPort() {
    static std::uint16_t next = static_cast<std::uint16_t>(21000 + (::getpid() % 2000));
    return next++;
}

class LoopFixture {
public:

    LoopFixture(Store& store, PollerKind kind, int sendBufBytes = 0)
        : port_(nextPort()),
          listener_(port_, 128),
          loop_(listener_, store, stop_, kind),
          thread_([this] { loop_.run(); }) {
        if (sendBufBytes > 0) {
            ::setsockopt(listener_.fd(), SOL_SOCKET, SO_SNDBUF, &sendBufBytes,
                         sizeof(sendBufBytes));
        }
    }

    ~LoopFixture() {
        stop_.store(true, std::memory_order_relaxed);
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    std::uint16_t port() const { return port_; }
    const EventLoop& loop() const { return loop_; }

private:
    std::uint16_t port_;
    ListeningSocket listener_;
    std::atomic<bool> stop_{false};
    EventLoop loop_;
    std::thread thread_;
};

class TestClient {
public:

    explicit TestClient(std::uint16_t port, int recvBufBytes = 0)
        : fd_(connectWithBuffer(port, recvBufBytes)) {}

    bool valid() const { return fd_.valid(); }

    bool send(const std::string& data) {
        std::size_t sent = 0;
        while (sent < data.size()) {
            const ssize_t n = ::write(fd_.get(), data.data() + sent, data.size() - sent);
            if (n <= 0) {
                return false;
            }
            sent += static_cast<std::size_t>(n);
        }
        return true;
    }

    std::string recvLines(std::size_t lines) {
        char buffer[16384];
        while (newlines_ < lines) {
            const ssize_t n = ::read(fd_.get(), buffer, sizeof(buffer));
            if (n <= 0) {
                break;
            }
            for (ssize_t i = 0; i < n; ++i) {
                if (buffer[i] == '\n') {
                    ++newlines_;
                }
            }
            received_.append(buffer, static_cast<std::size_t>(n));
        }
        std::string out;
        out.swap(received_);
        newlines_ = 0;
        return out;
    }

    void halfClose() { ::shutdown(fd_.get(), SHUT_WR); }
    void close() { fd_.reset(); }

    static std::size_t count(const std::string& text, char c) {
        std::size_t total = 0;
        for (char ch : text) {
            if (ch == c) {
                ++total;
            }
        }
        return total;
    }

private:
    static FileDescriptor connectWithBuffer(std::uint16_t port, int recvBufBytes) {
        FileDescriptor fd = kvstore::net::connectTo("127.0.0.1", port);
        if (fd.valid() && recvBufBytes > 0) {
            ::setsockopt(fd.get(), SOL_SOCKET, SO_RCVBUF, &recvBufBytes, sizeof(recvBufBytes));
        }
        return fd;
    }

    FileDescriptor fd_;
    std::string received_;
    std::size_t newlines_ = 0;
};

std::string ask(TestClient& client, const std::string& command) {
    if (!client.send(command)) {
        return "<send failed>";
    }
    return client.recvLines(1);
}

void testPollerBasics(PollerKind kind) {
    std::unique_ptr<Poller> poller = kvstore::net::makePoller(kind);
    std::cout << "Poller (" << poller->name() << "): registration and readiness\n";

    int fds[2] = {-1, -1};
    CHECK(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);
    FileDescriptor a(fds[0]);
    FileDescriptor b(fds[1]);

    CHECK(poller->add(a.get(), true, false));
    CHECK(poller->size() == 1);

    std::vector<ReadyEvent> events;

    CHECK(poller->wait(events, 20) == 0);

    const char byte = 'x';
    CHECK(::write(b.get(), &byte, 1) == 1);
    CHECK(poller->wait(events, 200) == 1);
    CHECK(!events.empty() && events[0].fd == a.get() && events[0].readable);

    CHECK(poller->modify(a.get(), true, true));
    CHECK(poller->wait(events, 200) >= 1);
    CHECK(!events.empty() && events[0].writable);

    CHECK(poller->remove(a.get()));
    CHECK(poller->size() == 0);
}

void testEventLoopBasics(PollerKind kind) {
    Store store;
    LoopFixture fixture(store, kind);
    std::cout << "Event loop (" << fixture.loop().pollerName() << "): basic commands\n";

    TestClient client(fixture.port());
    CHECK(client.valid());

    CHECK(ask(client, "SET greeting hello\n") == "+OK\n");
    CHECK(ask(client, "GET greeting\n") == "$hello\n");
    CHECK(ask(client, "GET missing\n") == "$nil\n");
    CHECK(ask(client, "DEL greeting\n") == "+OK\n");
    CHECK(ask(client, "GET greeting\n") == "$nil\n");
    CHECK(ask(client, "NONSENSE\n").rfind("-ERR", 0) == 0);

    CHECK(ask(client, "SET note hello there world\n") == "+OK\n");
    CHECK(ask(client, "GET note\n") == "$hello there world\n");
}

void testPipeliningAndSplits() {
    Store store;
    LoopFixture fixture(store, PollerKind::Auto);
    std::cout << "Event loop: pipelining and commands split across reads\n";

    TestClient client(fixture.port());

    std::string burst;
    for (int i = 0; i < 500; ++i) {
        burst += "SET key" + std::to_string(i) + " value" + std::to_string(i) + "\n";
    }
    CHECK(client.send(burst));
    const std::string replies = client.recvLines(500);
    CHECK(TestClient::count(replies, '\n') == 500);
    CHECK(ask(client, "GET key499\n") == "$value499\n");

    TestClient splitter(fixture.port());
    CHECK(splitter.send("SET split hel"));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    CHECK(splitter.send("lo\n"));
    CHECK(splitter.recvLines(1) == "+OK\n");
    CHECK(ask(splitter, "GET split\n") == "$hello\n");
}

void testManyConnections() {
    Store store;
    LoopFixture fixture(store, PollerKind::Auto);
    std::cout << "Event loop: 300 simultaneous connections served by ONE thread\n";

    constexpr int kConnections = 300;
    std::vector<std::unique_ptr<TestClient>> clients;
    clients.reserve(kConnections);

    for (int i = 0; i < kConnections; ++i) {
        clients.push_back(std::make_unique<TestClient>(fixture.port()));
    }
    std::size_t connected = 0;
    for (const auto& client : clients) {
        if (client->valid()) {
            ++connected;
        }
    }
    CHECK(connected == kConnections);

    for (int i = 0; i < kConnections; ++i) {
        clients[static_cast<std::size_t>(i)]->send("SET conn" + std::to_string(i) + " " +
                                                   std::to_string(i) + "\n");
    }
    std::size_t acked = 0;
    for (const auto& client : clients) {
        if (client->recvLines(1) == "+OK\n") {
            ++acked;
        }
    }
    CHECK(acked == kConnections);

    std::size_t correct = 0;
    for (int i = 0; i < kConnections; ++i) {
        const std::string expected = "$" + std::to_string(i) + "\n";
        if (ask(*clients[static_cast<std::size_t>(i)],
                "GET conn" + std::to_string(i) + "\n") == expected) {
            ++correct;
        }
    }
    CHECK(correct == kConnections);
    CHECK(store.size() == kConnections);
}

void testShortWrites() {
    Store store;

    LoopFixture fixture(store, PollerKind::Auto, 8192);
    std::cout << "Event loop: a reply larger than the socket buffer is finished later\n";

    TestClient client(fixture.port());

    const std::string big(60 * 1024, 'x');
    CHECK(ask(client, "SET big " + big + "\n") == "+OK\n");

    std::string requests;
    for (int i = 0; i < 100; ++i) {
        requests += "GET big\n";
    }
    CHECK(client.send(requests));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    TestClient other(fixture.port());
    CHECK(ask(other, "SET other 1\n") == "+OK\n");
    CHECK(ask(other, "GET other\n") == "$1\n");

    const std::string replies = client.recvLines(100);
    CHECK(TestClient::count(replies, '\n') == 100);
    CHECK(replies.size() == 100 * (big.size() + 2));

    CHECK(fixture.loop().shortWrites() > 0);

    CHECK(fixture.loop().evictedForBacklog() == 0);
}

void testTransactions() {
    Store store;
    LoopFixture fixture(store, PollerKind::Auto);
    std::cout << "Event loop: transactions are per connection, and roll back on disconnect\n";

    TestClient writer(fixture.port());
    TestClient reader(fixture.port());

    CHECK(ask(writer, "BEGIN\n") == "+OK\n");
    CHECK(ask(writer, "SET t1 inside\n") == "+OK\n");
    CHECK(ask(writer, "GET t1\n") == "$inside\n");
    CHECK(ask(reader, "GET t1\n") == "$nil\n");
    CHECK(ask(writer, "COMMIT\n") == "$committed=1\n");
    CHECK(ask(reader, "GET t1\n") == "$inside\n");

    {
        TestClient abandoner(fixture.port());
        CHECK(ask(abandoner, "BEGIN\n") == "+OK\n");
        CHECK(ask(abandoner, "SET ghost boo\n") == "+OK\n");
        abandoner.close();
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    CHECK(ask(reader, "GET ghost\n") == "$nil\n");
    CHECK(store.transactionsRolledBack() >= 1);
}

void testQuitAndHalfClose() {
    Store store;
    LoopFixture fixture(store, PollerKind::Auto);
    std::cout << "Event loop: QUIT is answered before closing, half-close is drained\n";

    TestClient quitter(fixture.port());
    CHECK(quitter.send("SET q 1\nQUIT\n"));
    const std::string replies = quitter.recvLines(2);

    CHECK(TestClient::count(replies, '\n') == 2);

    TestClient halfCloser(fixture.port());
    CHECK(halfCloser.send("GET q\n"));
    halfCloser.halfClose();
    CHECK(halfCloser.recvLines(1) == "$1\n");
}

void testReadOnlyInLoopMode() {
    StoreConfig config;
    config.readOnly = true;
    Store store(config);

    LoopFixture fixture(store, PollerKind::Auto);
    std::cout << "Event loop: a read-only node refuses writes here as well\n";

    TestClient client(fixture.port());
    CHECK(ask(client, "SET x 1\n").rfind("-ERR READONLY", 0) == 0);
    CHECK(ask(client, "DEL x\n").rfind("-ERR READONLY", 0) == 0);
    CHECK(ask(client, "GET x\n") == "$nil\n");
    CHECK(store.size() == 0);
}

void testBothPollersAgree() {
    std::cout << "Event loop: poll() and epoll() produce identical answers\n";

    auto script = [](PollerKind kind) {
        Store store;
        LoopFixture fixture(store, kind);
        TestClient client(fixture.port());
        std::string transcript;
        transcript += ask(client, "SET a 1\n");
        transcript += ask(client, "SET b two\n");
        transcript += ask(client, "GET a\n");
        transcript += ask(client, "GET b\n");
        transcript += ask(client, "DEL a\n");
        transcript += ask(client, "GET a\n");
        transcript += ask(client, "BEGIN\n");
        transcript += ask(client, "SET c 3\n");
        transcript += ask(client, "ROLLBACK\n");
        transcript += ask(client, "GET c\n");
        return transcript;
    };

    const std::string viaPoll = script(PollerKind::Poll);
    CHECK(!viaPoll.empty());

    if (kvstore::net::epollAvailable()) {
        const std::string viaEpoll = script(PollerKind::Epoll);

        CHECK(viaPoll == viaEpoll);
    } else {
        std::cout << "  skip epoll comparison (not Linux)\n";
    }
}
}

int main() {
    std::cout << "=== event loop and readiness backends ===\n";
    std::cout << "epoll available: " << (kvstore::net::epollAvailable() ? "yes" : "no") << "\n\n";

    testPollerBasics(PollerKind::Poll);
    if (kvstore::net::epollAvailable()) {
        testPollerBasics(PollerKind::Epoll);
    }
    testEventLoopBasics(PollerKind::Auto);
    testPipeliningAndSplits();
    testManyConnections();
    testShortWrites();
    testTransactions();
    testQuitAndHalfClose();
    testReadOnlyInLoopMode();
    testBothPollersAgree();

    if (g_failures == 0) {
        std::cout << "ALL TESTS PASSED\n";
        return 0;
    }
    std::cout << g_failures << " CHECK(s) FAILED\n";
    return 1;
}
