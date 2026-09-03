// SPDX-License-Identifier: MIT
//
// Uring close-convergence demo
//
// Build:
//   make uring
//
// Run:
//   ./build/uring/debug/examples/uring/demo_luring_close_pending_read

#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstddef>
#include <print>
#include <stop_token>

#include "alyrn/coro.h"
#include "alyrn/net.h"
#include "alyrn/uring.h"

using namespace alyrn;

namespace {

struct DemoState {
  uring::Loop* loop;
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

auto WaitForCancellation(uring::Stream& stream, DemoState& state) -> alyrn::DetachedTask {
  std::array<std::byte, 64> buffer{};

  std::println("submitting Read; the peer intentionally sends no data");
  auto read = co_await stream.Read(buffer);

  if (read.HasValue() || read.Error().value() != ECANCELED) {
    std::println(stderr, "pending read did not finish with ECANCELED");
    state.Fail();
  } else {
    std::println("pending Read resumed once with ECANCELED");
  }

  state.FinishOne();
}

auto CloseAfterReadSuspends(uring::Loop& loop, uring::Stream& stream, DemoState& state)
    -> alyrn::DetachedTask {
  // The read task is scheduled first. This timer gives the loop a separate
  // completion boundary before Close starts its cancel-and-drain protocol.
  auto delayed = co_await uring::SleepFor(loop, time::Milliseconds(10));
  if (!delayed.HasValue()) {
    std::println(stderr, "delay failed: {}", delayed.Error().message());
    state.Fail();
  }

  std::println("calling Close while Read is pending");
  auto closed = co_await stream.Close();
  if (!closed.HasValue()) {
    std::println(stderr, "close failed: {}", closed.Error().message());
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

  uring::Stream stream(&loop, sockets[0], net::Endpoint::Loopback(0));
  DemoState state{.loop = &loop};

  alyrn::SpawnDetach(loop, WaitForCancellation(stream, state));
  alyrn::SpawnDetach(loop, CloseAfterReadSuspends(loop, stream, state));
  loop.Run(std::stop_token{});

  (void)::close(sockets[1]);
  return state.exit_code;
}
