// ET/LT dual-mode correctness tests
//
// Coverage:
//  1. Both modes echo a small payload correctly (basic smoke)
//  2. ET drains a large payload (> readv extrabuf window ~65 KB) in one wakeup
//  3. ET acceptor drains a burst of simultaneous connections without dropping
//  4. Large write path completes in ET mode (server sends > 64 KB response)

#include <gtest/gtest.h>

#include "runtime/net/buffer.h"
#include "runtime/net/event_loop.h"
#include "runtime/net/inet_address.h"
#include "runtime/net/net_utils.h"
#include "runtime/net/tcp_server.h"

#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <future>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace runtime::net {
namespace {

using namespace std::chrono_literals;

// ── helpers ────────────────────────────────────────────────────────────────

// RAII wrapper for a raw file descriptor.
class ScopedFd {
public:
  explicit ScopedFd(int fd = -1) : fd_(fd) {}
  ~ScopedFd() {
    if (fd_ >= 0) ::close(fd_);
  }
  ScopedFd(ScopedFd&& o) noexcept : fd_(o.fd_) { o.fd_ = -1; }
  ScopedFd& operator=(ScopedFd&& o) noexcept {
    if (this != &o) {
      if (fd_ >= 0) ::close(fd_);
      fd_ = o.fd_;
      o.fd_ = -1;
    }
    return *this;
  }
  ScopedFd(const ScopedFd&) = delete;
  ScopedFd& operator=(const ScopedFd&) = delete;
  int get() const { return fd_; }

private:
  int fd_;
};

// Binds to port 0 and returns the kernel-assigned ephemeral port.
std::uint16_t ReserveLoopbackPort() {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  EXPECT_GE(fd, 0);
  int on = 1;
  ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = 0;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  ::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
  socklen_t len = sizeof(addr);
  ::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len);
  std::uint16_t port = ntohs(addr.sin_port);
  ::close(fd);
  return port;
}

ScopedFd ConnectTo(std::uint16_t port) {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  EXPECT_GE(fd, 0);
  sockaddr_in addr = MakeIPv4Address("127.0.0.1", port);
  EXPECT_EQ(::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
  return ScopedFd(fd);
}

void WriteAll(int fd, const std::string& data) {
  std::size_t sent = 0;
  while (sent < data.size()) {
    ssize_t n = ::write(fd, data.data() + sent, data.size() - sent);
    ASSERT_GT(n, 0);
    sent += static_cast<std::size_t>(n);
  }
}

std::string ReadExactly(int fd, std::size_t bytes) {
  std::string buf(bytes, '\0');
  std::size_t got = 0;
  while (got < bytes) {
    ssize_t n = ::read(fd, buf.data() + got, bytes - got);
    if (n <= 0) {
      ADD_FAILURE() << "short read: wanted=" << bytes << " got=" << got;
      return {};
    }
    got += static_cast<std::size_t>(n);
  }
  return buf;
}

// ── fixture / server factory ────────────────────────────────────────────────

// Starts an echo TcpServer in a background thread.
// Returns [loop ptr, server thread] via promise.  Call loop->Quit() to stop.
struct EchoServer {
  std::thread thread;
  EventLoop*  loop = nullptr;

  EchoServer() = default;
  EchoServer(EchoServer&&) = default;
  EchoServer& operator=(EchoServer&&) = default;
  EchoServer(const EchoServer&) = delete;
  EchoServer& operator=(const EchoServer&) = delete;

  // Blocks until the server is accepting.
  //
  // io_threads=0 keeps everything on the main loop so ~TcpServer() can call
  // ConnectDestroyed() without a cross-thread assert.  ET/LT behaviour is
  // independent of thread count, so 0 is fine for correctness tests.
  static EchoServer Start(std::uint16_t port, bool et_mode,
                          int io_threads = 0) {
    std::promise<EventLoop*> ready;
    auto fut = ready.get_future();

    EchoServer es;
    es.thread = std::thread([port, et_mode, io_threads,
                              p = std::move(ready)]() mutable {
      EventLoop loop;
      TcpServer server(&loop, InetAddress(port, "127.0.0.1"), "EchoServer");
      server.SetThreadNum(io_threads);
      server.SetEdgeTriggered(et_mode);
      server.SetMessageCallback(
          [](const TcpServer::TcpConnectionPtr& conn, Buffer& buf,
             runtime::time::Timestamp) {
            conn->Send(buf.RetrieveAllAsString());
          });
      server.Start();
      p.set_value(&loop);
      loop.Loop();
    });

    es.loop = fut.get();
    return es;
  }

  void Stop() {
    if (loop) loop->Quit();
    if (thread.joinable()) thread.join();
    loop = nullptr;
  }

  ~EchoServer() { Stop(); }
};

// ── parameterized: small payload echo ──────────────────────────────────────

class TriggerModeEchoTest : public ::testing::TestWithParam<bool> {};

INSTANTIATE_TEST_SUITE_P(
    LtAndEt, TriggerModeEchoTest,
    ::testing::Values(false, true),
    [](const ::testing::TestParamInfo<bool>& i) {
      return i.param ? "ET" : "LT";
    });

// Both modes must echo a small payload round-trip correctly.
TEST_P(TriggerModeEchoTest, EchoesSmallPayload) {
  const bool et = GetParam();
  const auto port = ReserveLoopbackPort();
  auto srv = EchoServer::Start(port, et);

  ScopedFd client = ConnectTo(port);
  const std::string msg = "hello ET/LT";
  WriteAll(client.get(), msg);
  EXPECT_EQ(ReadExactly(client.get(), msg.size()), msg);
}

// Pipelining: send two back-to-back messages on the same connection.
TEST_P(TriggerModeEchoTest, EchoesMultipleMessages) {
  const bool et = GetParam();
  const auto port = ReserveLoopbackPort();
  auto srv = EchoServer::Start(port, et);

  ScopedFd client = ConnectTo(port);
  const std::string a(512, 'A');
  const std::string b(512, 'B');
  WriteAll(client.get(), a);
  EXPECT_EQ(ReadExactly(client.get(), a.size()), a);
  WriteAll(client.get(), b);
  EXPECT_EQ(ReadExactly(client.get(), b.size()), b);
}

// ── large payload: forces ET drain loop ────────────────────────────────────

// Send 256 KB in one write.  Buffer::ReadFd reads at most ~65 KB per
// readv call (1 KB writable + 64 KB extrabuf), so ET mode must loop
// ReadFd at least 4 times to drain the kernel buffer.
TEST(TriggerModeTest, ET_DrainLargePayload) {
  const auto port = ReserveLoopbackPort();
  auto srv = EchoServer::Start(port, /*et=*/true);

  ScopedFd client = ConnectTo(port);

  const std::size_t kSize = 256 * 1024;  // 256 KB
  const std::string payload(kSize, 'X');
  WriteAll(client.get(), payload);
  EXPECT_EQ(ReadExactly(client.get(), kSize), payload);
}

// Same test but with LT to confirm the baseline still works.
TEST(TriggerModeTest, LT_DrainLargePayload) {
  const auto port = ReserveLoopbackPort();
  auto srv = EchoServer::Start(port, /*et=*/false);

  ScopedFd client = ConnectTo(port);

  const std::size_t kSize = 256 * 1024;
  const std::string payload(kSize, 'Y');
  WriteAll(client.get(), payload);
  EXPECT_EQ(ReadExactly(client.get(), kSize), payload);
}

// ── ET acceptor: burst connections ─────────────────────────────────────────

// Open N connections in rapid succession.  In ET mode the Acceptor must
// loop accept() until EAGAIN or connections are silently dropped.
// io_threads=0: keeps everything on the main loop so ~TcpServer() is safe.
TEST(TriggerModeTest, ET_AcceptsBurstConnections) {
  const auto port = ReserveLoopbackPort();
  auto srv = EchoServer::Start(port, /*et=*/true, /*io_threads=*/0);

  const int kConns = 16;
  std::vector<ScopedFd> clients;
  clients.reserve(kConns);
  for (int i = 0; i < kConns; ++i) {
    clients.push_back(ConnectTo(port));
  }

  // All connections must be alive and echo correctly.
  const std::string ping = "ping";
  for (auto& fd : clients) {
    WriteAll(fd.get(), ping);
  }
  for (auto& fd : clients) {
    EXPECT_EQ(ReadExactly(fd.get(), ping.size()), ping);
  }

  // Close all clients before stopping the server so TcpServer cleans up
  // connections on the correct loop thread via its normal close path.
  clients.clear();
}

// ── large response write path ───────────────────────────────────────────────

// Server sends a large body back. Verifies the ET write drain loop flushes
// the entire output_buffer_ even when the kernel send buffer fills mid-write.
// io_threads=0: avoids cross-thread assert in ~TcpServer().
TEST(TriggerModeTest, ET_SendsLargeResponse) {
  const auto port = ReserveLoopbackPort();
  const std::size_t kSize = 512 * 1024;  // 512 KB response
  const std::string large_body(kSize, 'Z');

  std::promise<EventLoop*> ready;
  auto fut = ready.get_future();
  std::thread srv_thread([&, port] {
    EventLoop loop;
    TcpServer server(&loop, InetAddress(port, "127.0.0.1"), "BigSendServer");
    server.SetThreadNum(0);  // single-threaded: all I/O on main loop
    server.SetEdgeTriggered(true);
    server.SetMessageCallback(
        [&large_body](const TcpServer::TcpConnectionPtr& conn, Buffer& buf,
                      runtime::time::Timestamp) {
          buf.RetrieveAll();       // discard the request
          conn->Send(large_body);  // send large response
        });
    server.Start();
    ready.set_value(&loop);
    loop.Loop();
  });

  EventLoop* loop = fut.get();
  {
    ScopedFd client = ConnectTo(port);

    // Trigger server by sending a request token.
    WriteAll(client.get(), "go");
    const std::string received = ReadExactly(client.get(), kSize);
    EXPECT_EQ(received, large_body);
    // client closes here; server processes the FIN on the loop thread
    // before we call Quit(), so ~TcpServer() finds an empty connections_ map.
  }

  loop->Quit();
  srv_thread.join();
}

}  // namespace
}  // namespace runtime::net
