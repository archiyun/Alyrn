// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstddef>
#include <expected>
#include <iostream>
#include <optional>
#include <span>
#include <string_view>
#include <system_error>
#include <utility>

#include "coropact/base/error.h"
#include "coropact/coro/detached_task.h"
#include "coropact/coro/spawn.h"
#include "coropact/luring/loop.h"
#include "coropact/luring/options.h"
#include "coropact/luring/stream.h"
#include "coropact/net/endpoint.h"

namespace {

using coropact::base::Error;
using coropact::base::Result;
using coropact::coro::DetachedTask;
using coropact::luring::LUringLoop;
using coropact::luring::LUringOptions;
using coropact::luring::LUringStream;
using coropact::luring::ZeroCopySendResult;

class UniqueFd final {
public:
  explicit UniqueFd(int fd = -1) noexcept : fd_(fd) {}
  UniqueFd(const UniqueFd&) = delete;
  UniqueFd& operator=(const UniqueFd&) = delete;
  UniqueFd(UniqueFd&& other) noexcept
      : fd_(std::exchange(other.fd_, -1)) {}

  ~UniqueFd() { Reset(); }

  [[nodiscard]]
  int Get() const noexcept { return fd_; }

  int Release() noexcept { return std::exchange(fd_, -1); }

  void Reset() noexcept {
    if (fd_ >= 0) {
      ::close(fd_);
      fd_ = -1;
    }
  }

private:
  int fd_{-1};
};

bool Check(bool condition, const char* message) {
  if (!condition) {
    std::cout << "FAIL: " << message << '\n';
    return false;
  }
  return true;
}

bool IsEnvironmentSkip(Error error) {
  return error == std::errc::operation_not_supported ||
         error == std::errc::operation_not_permitted ||
         error == std::errc::permission_denied ||
         error == std::errc::function_not_supported ||
         error.value() == EINVAL || error.value() == EOPNOTSUPP;
}

bool InitLoop(LUringLoop& loop) {
  LUringOptions options;
  options.entries = 32;
  options.submit_batch = 1;
  auto initialized = loop.Init(options);
  if (initialized.has_value()) {
    return true;
  }
  if (IsEnvironmentSkip(initialized.error())) {
    std::cout << "SKIP: io_uring unavailable: "
              << initialized.error().message() << '\n';
    return false;
  }
  std::cout << "FAIL: io_uring initialization failed: "
            << initialized.error().message() << '\n';
  return false;
}

Result<std::pair<UniqueFd, UniqueFd>> MakeTcpPair() {
  UniqueFd listener(::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0));
  if (listener.Get() < 0) {
    return std::unexpected(coropact::base::CurrentErrno());
  }

  int reuse = 1;
  (void)::setsockopt(listener.Get(), SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  if (::bind(listener.Get(), reinterpret_cast<const sockaddr*>(&address),
             sizeof(address)) < 0 ||
      ::listen(listener.Get(), 1) < 0) {
    return std::unexpected(coropact::base::CurrentErrno());
  }

  socklen_t address_length = sizeof(address);
  if (::getsockname(listener.Get(), reinterpret_cast<sockaddr*>(&address),
                    &address_length) < 0) {
    return std::unexpected(coropact::base::CurrentErrno());
  }

  UniqueFd client(::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0));
  if (client.Get() < 0 ||
      ::connect(client.Get(), reinterpret_cast<const sockaddr*>(&address),
                sizeof(address)) < 0) {
    return std::unexpected(coropact::base::CurrentErrno());
  }

  UniqueFd server(::accept4(
      listener.Get(), nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC));
  if (server.Get() < 0) {
    return std::unexpected(coropact::base::CurrentErrno());
  }

  const int flags = ::fcntl(client.Get(), F_GETFL, 0);
  if (flags < 0 || ::fcntl(client.Get(), F_SETFL, flags | O_NONBLOCK) < 0) {
    return std::unexpected(coropact::base::CurrentErrno());
  }

  return std::make_pair(std::move(client), std::move(server));
}

coropact::net::Endpoint EmptyPeerAddress() {
  sockaddr_in address{};
  address.sin_family = AF_INET;
  return coropact::net::Endpoint(address);
}

DetachedTask SendOnce(
    LUringStream* stream,
    std::span<const std::byte> payload,
    std::optional<Result<ZeroCopySendResult>>* result) {
  result->emplace(co_await stream->SendZeroCopy(payload));
}

bool CheckSendZeroCopy() {
  LUringLoop loop;
  if (!InitLoop(loop)) {
    return true;
  }

  auto pair = MakeTcpPair();
  if (!pair.has_value()) {
    std::cout << "FAIL: TCP pair failed: "
              << pair.error().message() << '\n';
    return false;
  }
  auto local = std::move(pair->first);
  auto peer = std::move(pair->second);

  LUringStream stream(&loop, local.Release(), EmptyPeerAddress());
  constexpr std::string_view text = "io_uring-send-zero-copy";
  const auto payload = std::as_bytes(
      std::span<const char>(text.data(), text.size()));

  std::optional<Result<ZeroCopySendResult>> result;
  coropact::coro::SpawnDetach(loop, SendOnce(&stream, payload, &result));
  loop.RunReady();

  for (int i = 0; i < 8 && !result.has_value(); ++i) {
    auto completions = loop.WaitCompletions();
    if (!completions.has_value()) {
      std::cout << "FAIL: waiting for send zerocopy CQE failed: "
                << completions.error().message() << '\n';
      return false;
    }
    loop.RunReady();
  }

  if (!result.has_value()) {
    std::cout << "FAIL: send zerocopy did not reach terminal CQE\n";
    return false;
  }
  if (!result->has_value() && IsEnvironmentSkip(result->error())) {
    std::cout << "SKIP: send zerocopy unsupported: "
              << result->error().message() << '\n';
    return true;
  }
  if (!Check(result->has_value(), "send zerocopy returned an error")) {
    std::cout << "send zerocopy error: " << result->error().message() << '\n';
    return false;
  }

  std::array<char, 128> received{};
  const ssize_t count = ::read(peer.Get(), received.data(), received.size());
  return Check(count == static_cast<ssize_t>(text.size()),
               "send zerocopy peer byte count mismatch") &&
         Check(std::string_view(received.data(), static_cast<std::size_t>(count)) == text,
               "send zerocopy payload mismatch") &&
         Check(result->value().bytes == text.size(),
               "send zerocopy result byte count mismatch") &&
         Check(result->value().notification_received,
               "send zerocopy notification CQE was not observed");
}

}  // namespace

int main() {
  if (!CheckSendZeroCopy()) {
    return 1;
  }
  std::cout << "luring send zerocopy smoke: PASS\n";
  return 0;
}
