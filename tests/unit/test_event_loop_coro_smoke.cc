// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
//
// Acceptance coverage for EventLoop's coroutine work source:
//   1. cross-thread Schedule wakes the poller;
//   2. every coroutine resumes on the owning loop thread;
//   3. in-loop Schedule appends to the local ready queue;
//   4. the coroutine budget does not starve ready fd events.

#include <sys/eventfd.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <coroutine>
#include <cstdint>
#include <functional>
#include <future>
#include <iostream>
#include <mutex>
#include <thread>

#include "vexo/net/channel.h"
#include "vexo/net/event_loop.h"

using vexo::net::EventLoop;

namespace {

// Non-owning handle carrier. The frame self-destructs at final suspend.
struct Probe {
  struct promise_type {
    Probe get_return_object() noexcept {
      return Probe{std::coroutine_handle<promise_type>::from_promise(*this)};
    }
    std::suspend_always initial_suspend() noexcept { return {}; }
    std::suspend_never final_suspend() noexcept { return {}; }
    void return_void() noexcept {}
    void unhandled_exception() noexcept { std::terminate(); }
  };

  std::coroutine_handle<promise_type> h{};
};

Probe Invoke(std::function<void()> fn) {
  fn();
  co_return;
}

bool CheckScheduleAndAffinity() {
  std::mutex mutex;
  std::condition_variable cv;
  EventLoop* loop = nullptr;
  std::thread::id loop_thread_id;
  std::atomic<int> ran{0};
  std::atomic<int> ran_on_loop{0};
  std::promise<void> done;

  std::jthread worker([&] {
    EventLoop owned_loop;
    {
      std::lock_guard lock{mutex};
      loop = &owned_loop;
      loop_thread_id = std::this_thread::get_id();
    }
    cv.notify_one();
    owned_loop.Loop();
  });

  {
    std::unique_lock lock{mutex};
    cv.wait(lock, [&] { return loop != nullptr; });
  }

  auto record = [&] {
    if (std::this_thread::get_id() == loop_thread_id) {
      ran_on_loop.fetch_add(1, std::memory_order_relaxed);
    }
    ran.fetch_add(1, std::memory_order_relaxed);
  };

  loop->Schedule({});

  constexpr int kSimple = 8;
  for (int i = 0; i < kSimple; ++i) {
    Probe probe = Invoke(record);
    loop->Schedule(probe.h);
  }

  Probe chain = Invoke([&] {
    record();
    Probe finisher = Invoke([&] {
      record();
      loop->Quit();
      done.set_value();
    });
    loop->Schedule(finisher.h);
  });
  loop->Schedule(chain.h);

  auto future = done.get_future();
  if (future.wait_for(std::chrono::seconds(3)) != std::future_status::ready) {
    std::cout << "FAIL: scheduled coroutines did not complete\n";
    loop->Quit();
    return false;
  }
  worker.join();

  constexpr int kExpected = kSimple + 2;
  if (ran.load() != kExpected || ran_on_loop.load() != kExpected) {
    std::cout << "FAIL: ran=" << ran.load() << " on_loop=" << ran_on_loop.load()
              << " expected=" << kExpected << '\n';
    return false;
  }
  return true;
}

bool CheckCoroutineBudgetPreservesIoFairness() {
  std::mutex mutex;
  std::condition_variable cv;
  bool setup_done = false;
  bool start = false;
  EventLoop* loop = nullptr;
  int io_fd = -1;
  std::atomic<int> ran{0};
  std::atomic<int> ran_when_io_fired{-1};
  std::promise<void> all_coroutines_done;

  std::jthread worker([&] {
    EventLoop owned_loop;
    const int owned_io_fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (owned_io_fd < 0) {
      {
        std::lock_guard lock{mutex};
        setup_done = true;
      }
      cv.notify_one();
      return;
    }

    vexo::net::Channel io_channel(&owned_loop, owned_io_fd);
    io_channel.set_read_callback([&](vexo::time::Timestamp) {
      uint64_t value = 0;
      (void)::read(owned_io_fd, &value, sizeof(value));
      ran_when_io_fired.store(ran.load(std::memory_order_relaxed), std::memory_order_relaxed);
    });
    io_channel.EnableReading();

    {
      std::unique_lock lock{mutex};
      loop = &owned_loop;
      io_fd = owned_io_fd;
      setup_done = true;
      cv.notify_one();
      cv.wait(lock, [&] { return start; });
    }

    owned_loop.Loop();
    io_channel.DisableAll();
    io_channel.Remove();
    ::close(owned_io_fd);
  });

  {
    std::unique_lock lock{mutex};
    cv.wait(lock, [&] { return setup_done; });
  }
  if (loop == nullptr || io_fd < 0) {
    std::cout << "FAIL: eventfd setup failed\n";
    return false;
  }

  constexpr int kTotal = 1024;
  std::function<void()> produce;
  produce = [&] {
    const int completed = ran.fetch_add(1, std::memory_order_relaxed) + 1;
    if (completed == kTotal) {
      all_coroutines_done.set_value();
      loop->Quit();
      return;
    }

    Probe next = Invoke(produce);
    loop->Schedule(next.h);
  };

  // Arm both work sources before the loop starts. A fair loop resumes one
  // budget of coroutines, performs a non-blocking poll, then continues.
  const uint64_t one = 1;
  if (::write(io_fd, &one, sizeof(one)) != static_cast<ssize_t>(sizeof(one))) {
    std::cout << "FAIL: eventfd write failed\n";
    {
      std::lock_guard lock{mutex};
      start = true;
    }
    cv.notify_one();
    loop->Quit();
    return false;
  }

  Probe first = Invoke(produce);
  loop->Schedule(first.h);

  {
    std::lock_guard lock{mutex};
    start = true;
  }
  cv.notify_one();

  auto future = all_coroutines_done.get_future();
  if (future.wait_for(std::chrono::seconds(3)) != std::future_status::ready) {
    std::cout << "FAIL: coroutine budget test timed out\n";
    loop->Quit();
    return false;
  }
  worker.join();

  const int io_position = ran_when_io_fired.load(std::memory_order_relaxed);
  if (io_position < 0 || io_position >= kTotal) {
    std::cout << "FAIL: fd event was starved by " << kTotal << " runnable coroutines\n";
    return false;
  }
  return true;
}

}  // namespace

int main() {
  if (!CheckScheduleAndAffinity()) return 1;
  if (!CheckCoroutineBudgetPreservesIoFairness()) return 1;

  std::cout << "event loop coro smoke: PASS\n";
  return 0;
}
