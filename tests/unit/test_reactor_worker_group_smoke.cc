// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <expected>
#include <iostream>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>
#include <vector>

#include "coropact/base/error.h"
#include "coropact/coro/scheduler.h"
#include "coropact/coro/task.h"
#include "coropact/net/endpoint.h"
#include "coropact/reactor/detail/reactor_worker.h"
#include "coropact/reactor/detail/reactor_worker_group.h"

namespace {

class UniqueFd {
public:
  explicit UniqueFd(int fd) noexcept : fd_(fd) {}
  UniqueFd(const UniqueFd&) = delete;
  UniqueFd& operator=(const UniqueFd&) = delete;

  ~UniqueFd() {
    if (fd_ >= 0) {
      ::close(fd_);
    }
  }

  int get() const noexcept { return fd_; }

private:
  int fd_;
};

struct WorkerState {
  std::condition_variable cv;
  std::mutex mutex;
  std::optional<coropact::net::Endpoint> listen_address;
  bool init_failed{false};
  bool connection_finished{false};
  bool scheduler_is_current{false};
  bool init_thread_is_worker{false};
};

struct GroupState {
  std::condition_variable cv;
  std::mutex mutex;
  std::vector<coropact::net::Endpoint> listen_addresses;
  std::vector<std::thread::id> init_threads;
};

bool Check(bool condition, const char* message) {
  if (!condition) {
    std::cout << "FAIL: " << message << '\n';
    return false;
  }
  return true;
}

coropact::base::Result<std::uint16_t> PickFreePort() {
  UniqueFd socket(::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP));
  if (socket.get() < 0) {
    return std::unexpected(coropact::base::CurrentErrno());
  }

  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = htons(0);
  if (::bind(socket.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
    return std::unexpected(coropact::base::CurrentErrno());
  }

  socklen_t length = sizeof(address);
  if (::getsockname(socket.get(), reinterpret_cast<sockaddr*>(&address), &length) < 0) {
    return std::unexpected(coropact::base::CurrentErrno());
  }
  return ntohs(address.sin_port);
}

coropact::base::Result<int> ConnectClient(const coropact::net::Endpoint& address) {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
  if (fd < 0) {
    return std::unexpected(coropact::base::CurrentErrno());
  }

  if (::connect(fd, address.sock_addr(), address.sock_addr_len()) < 0 && errno != EINPROGRESS) {
    const auto error = coropact::base::CurrentErrno();
    ::close(fd);
    return std::unexpected(error);
  }
  return fd;
}

coropact::coro::DetachedTask HandleConnection(
    coropact::reactor::detail::ReactorWorkerContext& context,
    coropact::reactor::ReactorStream stream, WorkerState* state) {
  {
    std::lock_guard lock{state->mutex};
    state->scheduler_is_current = coropact::coro::Scheduler::TryCurrent() == &context.loop;
  }
  state->cv.notify_all();

  auto close_result = co_await stream.Close();
  (void)close_result;
  {
    std::lock_guard lock{state->mutex};
    state->connection_finished = true;
  }
  state->cv.notify_all();
}

bool CheckWorkerAcceptAndStop() {
  WorkerState state;
  coropact::reactor::detail::ReactorWorker worker(
      0, coropact::net::Endpoint(0), {},
      [&state](coropact::reactor::detail::ReactorWorkerContext& context) {
        auto address = context.listener.LocalAddress();
        std::lock_guard lock{state.mutex};
        if (!address.has_value()) {
          state.init_failed = true;
        } else {
          state.listen_address = *address;
        }
        state.init_thread_is_worker = !context.loop.IsInLoopThread();
      },
      [&state](coropact::reactor::detail::ReactorWorkerContext& context,
               coropact::reactor::ReactorStream stream) {
        return HandleConnection(context, std::move(stream), &state);
      });

  auto started = worker.Start();
  if (!started.has_value()) {
    std::cout << "FAIL: ReactorWorker::Start failed: " << started.error().message() << '\n';
    return false;
  }

  {
    std::lock_guard lock{state.mutex};
    if (!Check(!state.init_failed, "worker init could not read the local address") ||
        !Check(!state.init_thread_is_worker, "init callback should run in the loop thread") ||
        !Check(state.listen_address.has_value(), "worker did not publish a local address")) {
      worker.Stop();
      return false;
    }
  }

  auto client_fd = ConnectClient(*state.listen_address);
  if (!client_fd.has_value()) {
    std::cout << "FAIL: client connect failed: " << client_fd.error().message() << '\n';
    worker.Stop();
    return false;
  }
  UniqueFd client(*client_fd);

  std::unique_lock lock{state.mutex};
  const bool finished = state.cv.wait_for(lock, std::chrono::seconds(2),
                                          [&state] { return state.connection_finished; });
  const bool scheduler_is_current = state.scheduler_is_current;
  lock.unlock();

  const bool ok = Check(finished, "connection callback did not finish") &&
                  Check(scheduler_is_current, "connection callback lost its worker scheduler");
  worker.Stop();
  return ok;
}

bool CheckWorkerGroupStartAndStop() {
  auto port = PickFreePort();
  if (!port.has_value()) {
    std::cout << "FAIL: PickFreePort failed: " << port.error().message() << '\n';
    return false;
  }

  GroupState state;
  coropact::reactor::detail::ReactorWorkerGroupOptions options;
  options.worker_num = 2;
  options.worker_options.listener_options.reuse_port = true;

  coropact::reactor::detail::ReactorWorkerGroup group(
      coropact::net::Endpoint(*port), options,
      [&state](coropact::reactor::detail::ReactorWorkerContext& context) {
        auto address = context.listener.LocalAddress();
        std::lock_guard lock{state.mutex};
        if (address.has_value()) {
          state.listen_addresses.push_back(*address);
        }
        state.init_threads.push_back(std::this_thread::get_id());
        state.cv.notify_all();
      });

  auto started = group.Start();
  if (!started.has_value()) {
    std::cout << "FAIL: ReactorWorkerGroup::Start failed: " << started.error().message() << '\n';
    return false;
  }

  bool all_initialized = false;
  {
    std::unique_lock lock{state.mutex};
    all_initialized = state.cv.wait_for(lock, std::chrono::seconds(2), [&state] {
      return state.listen_addresses.size() == 2 && state.init_threads.size() == 2;
    });
  }

  const auto main_thread = std::this_thread::get_id();
  bool worker_threads_are_distinct = false;
  bool ports_are_shared = false;
  {
    std::lock_guard lock{state.mutex};
    if (state.init_threads.size() == 2 && state.listen_addresses.size() == 2) {
      worker_threads_are_distinct = state.init_threads[0] != main_thread &&
                                    state.init_threads[1] != main_thread &&
                                    state.init_threads[0] != state.init_threads[1];
      ports_are_shared = state.listen_addresses[0].ToPort() == *port &&
                         state.listen_addresses[1].ToPort() == *port;
    }
  }

  const bool ok = Check(group.Started(), "group should be started") &&
                  Check(group.Size() == 2, "group should own two workers") &&
                  Check(all_initialized, "init callback should run for each worker") &&
                  Check(worker_threads_are_distinct, "workers should use distinct loop threads") &&
                  Check(ports_are_shared, "reuse_port workers should share the listen port") &&
                  Check(group.Worker(0) != nullptr && group.Worker(1) != nullptr,
                        "group worker accessors should return both workers");

  group.Stop();
  return ok && Check(!group.Started(), "group should be stopped") &&
         Check(group.Size() == 0, "group should release all workers");
}

bool CheckZeroWorkersRejected() {
  coropact::reactor::detail::ReactorWorkerGroupOptions options;
  options.worker_num = 0;
  coropact::reactor::detail::ReactorWorkerGroup group(coropact::net::Endpoint(0), options);
  auto result = group.Start();
  return Check(!result.has_value(), "zero-worker group should be rejected") &&
         Check(result.error() == std::errc::invalid_argument,
               "zero-worker group should return EINVAL");
}

}  // namespace

int main() {
  if (!CheckZeroWorkersRejected()) return 1;
  if (!CheckWorkerAcceptAndStop()) return 1;
  if (!CheckWorkerGroupStartAndStop()) return 1;

  std::cout << "reactor worker group smoke: PASS\n";
  return 0;
}
