#include "vexo/luring/worker.h"

#include <cerrno>
#include <algorithm>
#include <expected>
#include <memory>
#include <optional>
#include <utility>

#include "vexo/base/error.h"
#include "vexo/coro/spawn.h"
#include "vexo/coro/task.h"
#include "vexo/luring/listener.h"
#include "vexo/luring/loop.h"

namespace vexo::luring {

namespace {

coro::Task<void> AcceptLoop(LUringLoop& loop, LUringListener* listener,
                            LUringWorker::ConnectionCallback* callback) {
  while (true) {
    auto accepted = co_await listener->Accept();
    if (!accepted.has_value()) {
      const int error = accepted.error().value();
      if (error == ECANCELED || error == EBADF) {
        co_return;
      }
      continue;
    }
    if (*callback) {
      (*callback)(loop, std::move(*accepted));
    }
  }
}

coro::Task<void> CloseListener(LUringListener* listener,
                               std::optional<base::Result<void>>* result) {
  result->emplace(co_await listener->Close());
}

void CloseListenerAndDrain(LUringLoop& loop, LUringListener& listener) noexcept {
  std::optional<base::Result<void>> close_result;
  coro::Spawn(loop, CloseListener(&listener, &close_result)).Detach();

  while (!close_result.has_value()) {
    loop.RunReady();
    if (close_result.has_value()) {
      break;
    }

    if (loop.PendingSubmitCount() == 0 && loop.InflightCount() == 0) {
      break;
    }

    auto completed = loop.WaitCompletions();
    if (!completed.has_value()) {
      break;
    }
  }

  loop.RunReady();
}

}  // namespace

LUringWorker::LUringWorker(net::InetAddress listen_addr, LUringWorkerOptions options,
                           ThreadInitCallback init_callback, ConnectionCallback connection_callback)
    : listen_addr_(listen_addr),
      options_(std::move(options)),
      init_callback_(std::move(init_callback)),
      connection_callback_(std::move(connection_callback)) {}

LUringWorker::~LUringWorker() { Stop(); }

base::Result<void> LUringWorker::Start() {
  if (thread_.joinable()) {
    return std::unexpected(base::make_errno(EALREADY));
  }

  thread_ = std::jthread([this](std::stop_token token) { WorkLoop(std::move(token)); });

  std::unique_lock lk{mutex_};
  cv_.wait(lk, thread_.get_stop_token(), [this] { return started_; });

  if (!start_result_.has_value()) {
    return std::unexpected(start_result_.error());
  }
  return {};
}

void LUringWorker::Stop() noexcept {
  if (thread_.joinable()) {
    thread_.request_stop();
    cv_.notify_all();
  }
}

void LUringWorker::WorkLoop(std::stop_token token) noexcept {
  LUringLoop loop(options_.frame_resource);
  coro::FrameAllocatorScope frame_scope{options_.frame_resource};

  auto init = loop.Init(options_.loop_options);
  if (!init.has_value()) {
    {
      std::lock_guard lk{mutex_};
      start_result_ = std::unexpected(init.error());
      started_ = true;
    }
    cv_.notify_one();
    return;
  }

  auto listener = LUringListener::Create(&loop, listen_addr_, options_.listen_options);
  if (!listener.has_value()) {
    {
      std::lock_guard lk{mutex_};
      start_result_ = std::unexpected(listener.error());
      started_ = true;
    }
    cv_.notify_one();
    return;
  }

  if (init_callback_) {
    init_callback_(&loop, listener->get());
  }

  {
    std::lock_guard lk{mutex_};
    loop_ = &loop;
    listener_ = listener->get();
    start_result_ = base::Result<void>{};
    started_ = true;
  }
  cv_.notify_one();

  if (connection_callback_) {
    const std::size_t accept_depth = std::max<std::size_t>(1, options_.listen_options.accept_depth);
    for (std::size_t i = 0; i < accept_depth; ++i) {
      coro::Spawn(loop, AcceptLoop(loop, listener->get(), &connection_callback_)).Detach();
    }
  }

  std::stop_callback on_stop{token, [&loop] { loop.Quit(); }};

  loop.Loop(token);

  CloseListenerAndDrain(loop, *listener->get());
  listener->reset();

  {
    std::lock_guard lk{mutex_};
    loop_ = nullptr;
    listener_ = nullptr;
  }
}

}  // namespace vexo::luring
