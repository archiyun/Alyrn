// SPDX-License-Identifier: MIT
//
// Uring write-half shutdown demo
//
// Build:
//   make uring
//
// Run:
//   ./build/uring/debug/examples/uring/demo_luring_half_close

#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstddef>
#include <print>
#include <span>
#include <stop_token>
#include <string>
#include <string_view>
#include <system_error>

#include "alyrn/coro.h"
#include "alyrn/io.h"
#include "alyrn/net.h"
#include "alyrn/net/native.h"
#include "alyrn/uring.h"

using namespace alyrn;

namespace {

constexpr std::string_view kRequest = "request before half-close";
constexpr std::string_view kReply = "reply after peer observed EOF";

bool ReceiveExactly(int fd, std::span<char> destination) noexcept {
  std::size_t received = 0;
  while (received < destination.size()) {
    const ssize_t result =
        ::recv(fd, destination.data() + received, destination.size() - received, 0);
    if (result > 0) {
      received += static_cast<std::size_t>(result);
      continue;
    }
    if (result < 0 && errno == EINTR) {
      continue;
    }
    return false;
  }
  return true;
}

bool SendExactly(int fd, std::string_view bytes) noexcept {
  std::size_t sent = 0;
  while (sent < bytes.size()) {
    const ssize_t result = ::send(fd, bytes.data() + sent, bytes.size() - sent, MSG_NOSIGNAL);
    if (result > 0) {
      sent += static_cast<std::size_t>(result);
      continue;
    }
    if (result < 0 && errno == EINTR) {
      continue;
    }
    return false;
  }
  return true;
}

auto HalfCloseSession(uring::Loop& loop, uring::Stream stream, int peer_fd, int& exit_code)
    -> alyrn::DetachedTask {
  const auto request = std::as_bytes(std::span<const char>(kRequest.data(), kRequest.size()));
  auto written = co_await stream.Write(request);
  if (!written.HasValue()) {
    std::println(stderr, "write failed: {}", written.Error().message());
    (void)co_await stream.Close();
    loop.RequestStop();
    co_return;
  }

  // Shutdown closes only this endpoint's write half. It is idempotent and
  // does not invalidate reads or close the descriptor.
  auto shutdown = co_await stream.Shutdown();
  auto repeated_shutdown = co_await stream.Shutdown();
  if (!shutdown.HasValue() || !repeated_shutdown.HasValue()) {
    const auto& error = shutdown.HasValue() ? repeated_shutdown.Error() : shutdown.Error();
    std::println(stderr, "shutdown failed: {}", error.message());
    (void)co_await stream.Close();
    loop.RequestStop();
    co_return;
  }

  // Future writes fail locally after the write half has been shut down.
  auto rejected_write = co_await stream.Write(request);
  if (rejected_write.HasValue() || rejected_write.Error() != std::errc::broken_pipe) {
    std::println(stderr, "write after Shutdown did not fail with EPIPE");
    (void)co_await stream.Close();
    loop.RequestStop();
    co_return;
  }

  // The blocking peer is used only to make the OS-visible half-close easy to
  // see: it receives all preceding bytes and then observes EOF.
  std::array<char, kRequest.size()> peer_request{};
  if (!ReceiveExactly(peer_fd, peer_request) || std::string_view(peer_request) != kRequest) {
    std::println(stderr, "peer did not receive the request");
    (void)co_await stream.Close();
    loop.RequestStop();
    co_return;
  }

  std::array<char, 1> eof_probe{};
  if (::recv(peer_fd, eof_probe.data(), eof_probe.size(), 0) != 0) {
    std::println(stderr, "peer did not observe EOF after Shutdown");
    (void)co_await stream.Close();
    loop.RequestStop();
    co_return;
  }

  // Receiving EOF on one direction does not close the reverse direction.
  if (!SendExactly(peer_fd, kReply)) {
    std::println(stderr, "peer reply failed");
    (void)co_await stream.Close();
    loop.RequestStop();
    co_return;
  }

  std::array<std::byte, 64> read_buffer{};
  std::string reply;
  while (reply.size() < kReply.size()) {
    auto read = co_await stream.Read(read_buffer);
    if (!read.HasValue() || *read == 0) {
      std::println(stderr, "read after Shutdown failed");
      (void)co_await stream.Close();
      loop.RequestStop();
      co_return;
    }
    reply.append(reinterpret_cast<const char*>(read_buffer.data()), *read);
  }

  if (reply != kReply) {
    std::println(stderr, "reply mismatch");
    (void)co_await stream.Close();
    loop.RequestStop();
    co_return;
  }

  auto closed = co_await stream.Close();
  if (!closed.HasValue()) {
    std::println(stderr, "close failed: {}", closed.Error().message());
    loop.RequestStop();
    co_return;
  }

  std::println("Shutdown sequence verified:");
  std::println("  write request -> Shutdown -> peer EOF");
  std::println("  peer reply -> Read still succeeds -> Close");
  exit_code = 0;
  loop.RequestStop();
}

}  // namespace

auto main() -> int {
  std::array<int, 2> sockets{-1, -1};
  if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sockets.data()) < 0) {
    std::println(stderr, "socketpair failed: {}", CurrentErrno().message());
    return 1;
  }

  // Keep the Uring endpoint non-blocking, but make the small demonstration
  // peer blocking so its EOF observation is deterministic and readable.
  auto blocking_peer = net::SetNonBlocking(sockets[1], false);
  if (!blocking_peer.HasValue()) {
    std::println(stderr, "failed to configure peer: {}", blocking_peer.Error().message());
    (void)::close(sockets[0]);
    (void)::close(sockets[1]);
    return 1;
  }

  uring::Loop loop;
  uring::Options options;
  options.entries = 64;

  auto initialized = loop.Init(options);
  if (!initialized.HasValue()) {
    std::println(stderr, "loop init failed: {}", initialized.Error().message());
    (void)::close(sockets[0]);
    (void)::close(sockets[1]);
    return 1;
  }

  int exit_code = 1;
  uring::Stream stream(&loop, sockets[0], net::Endpoint::Loopback(0));
  alyrn::SpawnDetach(loop, HalfCloseSession(loop, std::move(stream), sockets[1], exit_code));
  loop.Run(std::stop_token{});

  (void)::close(sockets[1]);
  return exit_code;
}
