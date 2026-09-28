// SPDX-License-Identifier: MIT
#include "alyrn/uring/detail/worker.h"

#include <pthread.h>
#include <sched.h>

#include <algorithm>
#include <cerrno>
#include <expected>
#include <optional>
#include <stop_token>
#include <thread>
#include <utility>

#include "alyrn/result.h"
#include "alyrn/coro/spawn.h"
#include "alyrn/net/detail/socket.h"
#include "alyrn/time/clock.h"
#include "alyrn/uring/detail/loop_access.h"
#include "alyrn/uring/listener.h"
#include "alyrn/uring/loop.h"
#include "alyrn/uring/timer.h"

namespace alyrn::uring::detail {

namespace {

// Backoff before retrying accept after a transient resource error, so the loop
// can service existing connections and free descriptors instead of spinning.
constexpr time::Duration kAcceptBackoff = time::Milliseconds(10);

// Runs when a connection handler finishes. A drain stops the loop once the
// last handler is gone.
void OnConnectionDone(void* context) noexcept {
  auto& worker = *static_cast<WorkerContext*>(context);
  --worker.live_connections;
  if (worker.draining && worker.live_connections == 0) {
    worker.loop.RequestStop();
  }
}

void SpawnHandler(WorkerContext& context, Worker::ConnectionCallback& callback, Stream stream) {
  auto handler = callback(context, std::move(stream));
  ++context.live_connections;
  handler.OnComplete(&OnConnectionDone, &context);
  coro::SpawnDetach(context.loop, std::move(handler));
}

coro::DetachedTask AcceptLoop(WorkerContext& context,
                               Worker::ConnectionCallback* callback) {
  while (true) {
    auto accepted = co_await context.listener.Accept();
    if (!accepted.HasValue()) {
      const int error = accepted.Error().value();
      if (error == ECANCELED || error == EBADF) {
        co_return;
      }
      if (net::detail::IsTransientAcceptResourceError(error)) {
        // Descriptors are exhausted; back off so the loop can drain and close
        // connections before retrying. A loop stop ends the wait with ECANCELED.
        auto waited = co_await SleepFor(context.loop, kAcceptBackoff);
        if (!waited.HasValue()) {
          co_return;
        }
      }
      continue;
    }
    if (*callback) {
      SpawnHandler(context, *callback, std::move(*accepted));
    }
  }
}

coro::DetachedTask MultishotAcceptLoop(
    WorkerContext& context,
    Worker::ConnectionCallback* callback) {
  for (;;) {
    // A terminal EMFILE/ENFILE CQE stops the multishot source (it cannot be
    // re-armed in place, unlike a per-connection error). The source surfaces
    // that errno through Next(); recreate the source after a backoff so the
    // listener keeps accepting instead of stopping for good after one EMFILE.
    bool retry_after_backoff = false;
    {
      auto source_result = context.listener.CreateAcceptSource({
          .pending_depth = 1,
          .event_capacity = 1024,
      });
      if (!source_result.HasValue()) {
        co_return;
      }

      auto source = std::move(*source_result);
      for (;;) {
        auto accepted = co_await source.Next();
        if (!accepted.HasValue()) {
          retry_after_backoff =
              net::detail::IsTransientAcceptResourceError(accepted.Error().value());
          break;
        }
        if (!*accepted) {
          break;
        }

        if (*callback) {
          SpawnHandler(context, *callback, std::move(**accepted));
        }
      }

      // Stop() is idempotent and also covers an error or listener-close path. It
      // keeps the source's operation/cancel state converged before its frame is
      // destroyed at the end of this scope, which releases the listener's
      // accept-source slot for the next CreateAcceptSource.
      auto stopped = co_await source.Stop();
      (void)stopped;
    }

    if (!retry_after_backoff) {
      co_return;
    }
    auto waited = co_await SleepFor(context.loop, kAcceptBackoff);
    if (!waited.HasValue()) {
      co_return;
    }
  }
}

coro::DetachedTask CloseListener(Listener* listener,
                                 std::optional<Result<void>>* result) {
  result->emplace(co_await listener->Close());
}

// Stops accepting during a drain; the accept loops observe the close.
coro::DetachedTask CloseListenerForDrain(Listener& listener) { (void)co_await listener.Close(); }

void CloseListenerAfterLoopDrain(Loop& loop, Listener& listener) noexcept {
  std::optional<Result<void>> close_result;
  coro::SpawnDetach(loop, CloseListener(&listener, &close_result));
  LoopAccess::RunReady(loop);
  ALYRN_CHECK(close_result.has_value(),
                 "Loop drain left listener Close coroutine pending");
}

Result<void> SetCurrentThreadAffinity(unsigned cpu) noexcept {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(cpu, &cpuset);
  const int result = pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
  if (result != 0) {
    return std::unexpected(Errno(result));
  }
  return {};
}

}  // namespace

Worker::Worker(std::size_t index, net::Endpoint listen_addr, WorkerOptions options,
               ThreadInitCallback init_callback, ConnectionCallback connection_callback,
               ThreadExitCallback exit_callback, ThreadDrainCallback drain_callback)
    : index_(index),
      listen_addr_(listen_addr),
      options_(std::move(options)),
      init_callback_(std::move(init_callback)),
      connection_callback_(std::move(connection_callback)),
      exit_callback_(std::move(exit_callback)),
      drain_callback_(std::move(drain_callback)) {}

Worker::~Worker() noexcept { Stop(); }

Result<void> Worker::Start() {
  if (thread_.joinable()) {
    return std::unexpected(Errno(EALREADY));
  }

  {
    std::lock_guard lock{mutex_};
    init_done_ = false;
    start_result_ = Result<void>{};
    exit_result_ = ExitResult{};
  }
  drain_source_ = std::stop_source{};

  thread_ = std::jthread([this](std::stop_token token) { WorkLoop(std::move(token)); });

  std::unique_lock lock{mutex_};
  cv_.wait(lock, thread_.get_stop_token(), [this] { return init_done_; });

  if (!init_done_) {
    return std::unexpected(Errno(ECANCELED));
  }
  return start_result_;
}

void Worker::Stop() noexcept {
  // The stop source, unlike joinable(), may be used while another thread
  // joins; a worker that never started has no stop state and ignores it.
  (void)thread_.get_stop_source().request_stop();
}

void Worker::RequestDrain() noexcept { (void)drain_source_.request_stop(); }

void Worker::BeginDrain(WorkerContext& context) noexcept {
  Loop& loop = context.loop;
  if (context.draining || loop.State() != backend::LoopState::kRunning) {
    return;
  }
  context.draining = true;
  // Stop accepting: pending accepts complete and the accept loops end.
  // Connections the kernel queued but nobody accepted are reset.
  coro::SpawnDetach(loop, CloseListenerForDrain(context.listener));
  if (drain_callback_) {
    try {
      drain_callback_(context);
    } catch (...) {
      exit_result_ = std::unexpected(std::current_exception());
    }
  }
  if (context.live_connections == 0) {
    loop.RequestStop();
    return;
  }
  auto grace = loop.RunAfter(options_.shutdown_grace, [&loop] { loop.RequestStop(); });
  if (!grace.HasValue()) {
    // Without a timer the grace period cannot end: stop now.
    loop.RequestStop();
  }
}

Worker::ExitResult Worker::Join() noexcept {
  if (thread_.joinable()) {
    thread_.join();
  }
  // Joining synchronizes with the worker's publication of the exit result.
  return exit_result_;
}

void Worker::WorkLoop(std::stop_token token) noexcept {
  auto PublishStart = [this](Result<void> result) noexcept {
    {
      std::lock_guard lock{mutex_};
      start_result_ = std::move(result);
      init_done_ = true;
    }
    cv_.notify_one();
  };

  if (options_.cpu_affinity.has_value()) {
    auto affinity = SetCurrentThreadAffinity(*options_.cpu_affinity);
    if (!affinity.HasValue()) {
      PublishStart(std::unexpected(affinity.Error()));
      return;
    }
  }

  coro::FrameAllocatorScope frame_scope{options_.frame_resource};
  Loop loop(options_.frame_resource);

  auto loop_init = loop.Init(options_.loop_options);
  if (!loop_init.HasValue()) {
    PublishStart(std::unexpected(loop_init.Error()));
    return;
  }

  auto listener = Listener::Create(&loop, listen_addr_, options_.listen_options);

  if (!listener.HasValue()) {
    PublishStart(std::unexpected(listener.Error()));
    return;
  }

  auto connector = Connector::Create(&loop);
  if (!connector.HasValue()) {
    PublishStart(std::unexpected(connector.Error()));
    return;
  }

  WorkerContext context{index_, loop, *listener, *connector};

  auto init_result = Result<void>{};
  if (init_callback_) {
    loop.RunOnOwner([&] {
      try {
        init_callback_(context);
      } catch (...) {
        init_result = std::unexpected(Errno(EFAULT));
      }
    });
    if (init_result.HasValue() && !context.start_result.HasValue()) {
      init_result = context.start_result;
    }
    if (!init_result.HasValue()) {
      // The callback may already have scheduled work. Drain it while the
      // context is alive, then run exit cleanup for partial initialization.
      loop.RequestStop();
    }
  }

  if (init_result.HasValue() && connection_callback_) {
    if (options_.accept_mode == AcceptMode::kMultishot) {
      coro::SpawnDetach(loop, MultishotAcceptLoop(context, &connection_callback_));
    } else {
      const std::size_t accept_depth =
          std::max<std::size_t>(1, options_.listen_options.accept_depth);
      for (std::size_t i = 0; i < accept_depth; ++i) {
        coro::SpawnDetach(loop, AcceptLoop(context, &connection_callback_));
      }
    }
  }

  PublishStart(init_result);
  // A drain request runs BeginDrain on this loop. The callback is destroyed
  // before the loop and waits for a concurrent RequestDrain() to return; a
  // request that came first runs it here, at registration.
  auto post_drain = [this, &loop, &context] {
    (void)loop.Post([this, &context] { BeginDrain(context); });
  };
  std::stop_callback on_drain{drain_source_.get_token(), post_drain};
  loop.Run(token);
  CloseListenerAfterLoopDrain(loop, *listener);

  if (exit_callback_) {
    loop.RunOnOwner([&] {
      try {
        exit_callback_(context);
      } catch (...) {
        exit_result_ = std::unexpected(std::current_exception());
      }
    });
    // Waiters the callback woke, for example through Channel::Close(), must
    // finish while the loop-bound resources they may reference are alive.
    while (LoopAccess::HasReadyWork(loop)) {
      LoopAccess::RunReady(loop);
    }
  }
}

}  // namespace alyrn::uring::detail
