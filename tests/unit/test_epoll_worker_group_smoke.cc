// SPDX-License-Identifier: MIT

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

#include "alyrn/result.h"
#include "alyrn/coro/scheduler.h"
#include "alyrn/net/endpoint.h"
#include "alyrn/epoll/detail/worker.h"
#include "alyrn/epoll/detail/worker_group.h"

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
  std::optional<alyrn::net::Endpoint> listen_address;
  bool init_failed{false};
  bool connection_finished{false};
  bool scheduler_is_current{false};
  bool init_thread_is_worker{false};
};

struct GroupState {
  std::condition_variable cv;
  std::mutex mutex;
  std::vector<alyrn::net::Endpoint> listen_addresses;
  std::vector<std::thread::id> init_threads;
};

bool Check(bool condition, const char* message) {
  if (!condition) {
    std::cout << "FAIL: " << message << '\n';
    return false;
  }
  return true;
}

alyrn::Result<std::uint16_t> PickFreePort() {
  UniqueFd socket(::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP));
  if (socket.get() < 0) {
    return std::unexpected(alyrn::CurrentErrno());
  }

  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = htons(0);
  if (::bind(socket.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
    return std::unexpected(alyrn::CurrentErrno());
  }

  socklen_t length = sizeof(address);
  if (::getsockname(socket.get(), reinterpret_cast<sockaddr*>(&address), &length) < 0) {
    return std::unexpected(alyrn::CurrentErrno());
  }
  return ntohs(address.sin_port);
}

alyrn::Result<int> ConnectClient(const alyrn::net::Endpoint& address) {
  const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
  if (fd < 0) {
    return std::unexpected(alyrn::CurrentErrno());
  }

  if (::connect(fd, address.SockAddr(), address.SockAddrLen()) < 0 && errno != EINPROGRESS) {
    const auto error = alyrn::CurrentErrno();
    ::close(fd);
    return std::unexpected(error);
  }
  return fd;
}

alyrn::coro::DetachedTask HandleConnection(
    alyrn::epoll::detail::WorkerContext& context,
    alyrn::epoll::Stream stream, WorkerState* state) {
  {
    std::lock_guard lock{state->mutex};
    state->scheduler_is_current = alyrn::coro::Scheduler::TryCurrent() == &context.loop;
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
  alyrn::epoll::detail::Worker worker(
      0, alyrn::net::Endpoint(0), {},
      [&state](alyrn::epoll::detail::WorkerContext& context) {
        auto address = context.listener.LocalAddress();
        std::lock_guard lock{state.mutex};
        if (!address.HasValue()) {
          state.init_failed = true;
        } else {
          state.listen_address = *address;
        }
        state.init_thread_is_worker = !context.loop.IsInLoopThread();
      },
      [&state](alyrn::epoll::detail::WorkerContext& context,
               alyrn::epoll::Stream stream) {
        return HandleConnection(context, std::move(stream), &state);
      });

  auto started = worker.Start();
  if (!started.HasValue()) {
    std::cout << "FAIL: Worker::Start failed: " << started.Error().message() << '\n';
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
  if (!client_fd.HasValue()) {
    std::cout << "FAIL: client connect failed: " << client_fd.Error().message() << '\n';
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
  if (!port.HasValue()) {
    std::cout << "FAIL: PickFreePort failed: " << port.Error().message() << '\n';
    return false;
  }

  GroupState state;
  alyrn::epoll::detail::WorkerGroupOptions options;
  options.worker_num = 2;
  options.worker_options.listener_options.reuse_port = true;

  alyrn::epoll::detail::WorkerGroup group(
      alyrn::net::Endpoint(*port), options,
      [&state](alyrn::epoll::detail::WorkerContext& context) {
        auto address = context.listener.LocalAddress();
        std::lock_guard lock{state.mutex};
        if (address.HasValue()) {
          state.listen_addresses.push_back(*address);
        }
        state.init_threads.push_back(std::this_thread::get_id());
        state.cv.notify_all();
      });

  auto started = group.Start();
  if (!started.HasValue()) {
    std::cout << "FAIL: WorkerGroup::Start failed: " << started.Error().message() << '\n';
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
                  Check(group.At(0) != nullptr && group.At(1) != nullptr,
                        "group worker accessors should return both workers");

  group.Stop();
  return ok && Check(!group.Started(), "group should be stopped") &&
         Check(group.Size() == 0, "group should release all workers");
}

bool CheckZeroWorkersRejected() {
  alyrn::epoll::detail::WorkerGroupOptions options;
  options.worker_num = 0;
  alyrn::epoll::detail::WorkerGroup group(alyrn::net::Endpoint(0), options);
  auto result = group.Start();
  return Check(!result.HasValue(), "zero-worker group should be rejected") &&
         Check(result.Error() == std::errc::invalid_argument,
               "zero-worker group should return EINVAL");
}

}  // namespace

int main() {
  if (!CheckZeroWorkersRejected()) return 1;
  if (!CheckWorkerAcceptAndStop()) return 1;
  if (!CheckWorkerGroupStartAndStop()) return 1;

  std::cout << "epoll worker group smoke: PASS\n";
  return 0;
}
