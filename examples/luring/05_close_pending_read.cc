// SPDX-License-Identifier: MIT
//
// LUring close-convergence demo
//
// Build:
//   cmake --build build-uring --target demo_luring_close_pending_read -j"$(nproc)"
//
// Run:
//   ./build-uring/examples/luring/demo_luring_close_pending_read

#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstddef>
#include <print>
#include <stop_token>

#include "coropact/coro.h"
#include "coropact/luring.h"
#include "coropact/net.h"

using namespace coropact;

namespace {

struct DemoState {
  luring::LUringLoop* loop;
  int completed{0};
  int exit_code{0};

  void Fail() noexcept { exit_code = 1; }

  void FinishOne() noexcept {
    ++completed;
    if (completed == 2) {
      loop->RequestStop();
    }
  }
};

auto WaitForCancellation(luring::LUringStream& stream, DemoState& state) -> coro::DetachedTask {
  std::array<std::byte, 64> buffer{};

  std::println("submitting ReadSome; the peer intentionally sends no data");
  auto read = co_await stream.ReadSome(buffer);

  if (read.has_value() || read.error().value() != ECANCELED) {
    std::println(stderr, "pending read did not finish with ECANCELED");
    state.Fail();
  } else {
    std::println("pending ReadSome resumed once with ECANCELED");
  }

  state.FinishOne();
}

auto CloseAfterReadSuspends(luring::LUringLoop& loop, luring::LUringStream& stream,
                            DemoState& state) -> coro::DetachedTask {
  // The read task is scheduled first. This timer gives the loop a separate
  // completion boundary before Close starts its cancel-and-drain protocol.
  auto delayed = co_await luring::SleepFor(loop, time::Milliseconds(10));
  if (!delayed.has_value()) {
    std::println(stderr, "delay failed: {}", delayed.error().message());
    state.Fail();
  }

  std::println("calling Close while ReadSome is pending");
  auto closed = co_await stream.Close();
  if (!closed.has_value()) {
    std::println(stderr, "close failed: {}", closed.error().message());
    state.Fail();
  } else {
    std::println("Close returned after cancel and original read CQEs converged");
  }

  state.FinishOne();
}

}  // namespace

auto main() -> int {
  std::array<int, 2> sockets{-1, -1};
  if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sockets.data()) < 0) {
    std::println(stderr, "socketpair failed: {}", CurrentErrno().message());
    return 1;
  }

  luring::LUringLoop loop;
  luring::LUringOptions options;
  options.entries = 64;

  auto initialized = loop.Init(options);
  if (!initialized.has_value()) {
    std::println(stderr, "loop init failed: {}", initialized.error().message());
    (void)::close(sockets[0]);
    (void)::close(sockets[1]);
    return 1;
  }

  luring::LUringStream stream(&loop, sockets[0], net::Endpoint::Loopback(0));
  DemoState state{.loop = &loop};

  coro::SpawnDetach(loop, WaitForCancellation(stream, state));
  coro::SpawnDetach(loop, CloseAfterReadSuspends(loop, stream, state));
  loop.Run(std::stop_token{});

  (void)::close(sockets[1]);
  return state.exit_code;
}
