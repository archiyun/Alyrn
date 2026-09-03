// SPDX-License-Identifier: MIT
//
// Uring SendZeroCopy demo
//
// Shows the split-release path: co_await returns only after the kernel's
// buffer-release boundary (primary CQE without F_MORE, or the later F_NOTIF).
//
// Build:
//   make uring
//
// Run:
//   ./build/uring/debug/examples/uring/demo_luring_zero_copy_send

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <print>
#include <span>
#include <stop_token>
#include <string_view>
#include <utility>

#include "alyrn/coro.h"
#include "alyrn/io.h"
#include "alyrn/net.h"
#include "alyrn/uring.h"

using namespace alyrn;

namespace {

constexpr std::string_view kPayload = "hello from SendZeroCopy";

const char* UsageName(uring::ZeroCopySendUsage usage) noexcept {
  switch (usage) {
    case uring::ZeroCopySendUsage::kUnknown:
      return "unknown (primary was already terminal; no usage report)";
    case uring::ZeroCopySendUsage::kZeroCopy:
      return "zero-copy";
    case uring::ZeroCopySendUsage::kCopied:
      return "copied (kernel fallback; not a business error)";
  }
  return "unknown";
}

// Blocking TCP client + non-blocking server fd, same shape as the send_zc smoke
// test. AF_UNIX socketpair is less reliable for send_zc across kernels.
Result<std::pair<int, int>> MakeTcpPair() noexcept {
  const int listener = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (listener < 0) {
    return std::unexpected(CurrentErrno());
  }

  int reuse = 1;
  (void)::setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  if (::bind(listener, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) < 0 ||
      ::listen(listener, 1) < 0) {
    const auto error = CurrentErrno();
    (void)::close(listener);
    return std::unexpected(error);
  }

  socklen_t address_length = sizeof(address);
  if (::getsockname(listener, reinterpret_cast<sockaddr*>(&address), &address_length) < 0) {
    const auto error = CurrentErrno();
    (void)::close(listener);
    return std::unexpected(error);
  }

  const int client = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (client < 0 ||
      ::connect(client, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) < 0) {
    const auto error = CurrentErrno();
    if (client >= 0) {
      (void)::close(client);
    }
    (void)::close(listener);
    return std::unexpected(error);
  }

  const int server = ::accept4(listener, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
  (void)::close(listener);
  if (server < 0) {
    const auto error = CurrentErrno();
    (void)::close(client);
    return std::unexpected(error);
  }

  return std::make_pair(server, client);
}

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

auto ZeroCopySession(uring::Loop& loop, uring::Stream stream, int peer_fd, int& exit_code)
    -> alyrn::DetachedTask {
  // Own the send memory until co_await returns. After release authorization,
  // overwriting it is safe even if the peer has not yet read every byte.
  std::array<char, kPayload.size()> owned{};
  std::copy(kPayload.begin(), kPayload.end(), owned.begin());
  auto payload = std::as_bytes(std::span<const char>(owned.data(), owned.size()));

  auto sent = co_await stream.SendZeroCopy(payload);
  if (!sent.HasValue()) {
    std::println(stderr, "SendZeroCopy failed: {}", sent.Error().message());
    std::println(stderr, "hint: older kernels or restricted environments may not support send_zc");
    (void)co_await stream.Close();
    loop.RequestStop();
    co_return;
  }

  // Release boundary crossed: the awaiter promised the kernel no longer needs
  // this buffer. Mutating it demonstrates that ownership returned to the caller.
  owned.fill('*');

  std::array<char, kPayload.size()> peer_bytes{};
  if (!ReceiveExactly(peer_fd, peer_bytes) || std::string_view(peer_bytes) != kPayload) {
    std::println(stderr, "peer payload mismatch");
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

  std::println("SendZeroCopy completed (LRCI split-release):");
  std::println("  bytes                 = {}", sent->bytes);
  std::println("  usage                 = {}", UsageName(sent->usage));
  std::println("  notification_received = {}", sent->notification_received);
  std::println("  buffer reused after await returned (kernel release boundary crossed)");
  exit_code = 0;
  loop.RequestStop();
}

}  // namespace

auto main() -> int {
  auto sockets = MakeTcpPair();
  if (!sockets.HasValue()) {
    std::println(stderr, "tcp pair failed: {}", sockets.Error().message());
    return 1;
  }
  auto [server_fd, client_fd] = *sockets;

  uring::Loop loop;
  uring::Options options;
  options.entries = 64;

  auto initialized = loop.Init(options);
  if (!initialized.HasValue()) {
    std::println(stderr, "loop init failed: {}", initialized.Error().message());
    (void)::close(server_fd);
    (void)::close(client_fd);
    return 1;
  }

  int exit_code = 1;
  uring::Stream stream(&loop, server_fd, net::Endpoint::Loopback(0));
  alyrn::SpawnDetach(loop, ZeroCopySession(loop, std::move(stream), client_fd, exit_code));
  loop.Run(std::stop_token{});

  (void)::close(client_fd);
  return exit_code;
}
