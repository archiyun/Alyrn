// SPDX-License-Identifier: MIT
//
// Frame-allocator churn on a real io_uring Loop. HTTP keep-alive hides this
// cost in I/O; this program does almost no syscalls so malloc vs the worker
// frame pool shows up in wall time.
//
// Build:
//   make uring
// Run:
//   ITERATIONS=1000000 FRAME_POOL=0 ./build/uring/debug/examples/uring/demo_bench_frame_churn
//   ITERATIONS=1000000 FRAME_POOL=1 ./build/uring/debug/examples/uring/demo_bench_frame_churn

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <optional>

#include "alyrn/coro/frame_allocator.h"
#include "alyrn/coro/spawn.h"
#include "alyrn/coro/work.h"
#include "alyrn/uring/loop.h"
#include "alyrn/uring/options.h"
#include "alyrn/spawn.h"

namespace {

using alyrn::DetachedTask;
using alyrn::coro::ResumeWork;
using alyrn::coro::Scheduler;
using alyrn::SpawnDetach;
using alyrn::uring::Loop;

std::uint64_t EnvU64(const char* key, std::uint64_t fallback) {
  const char* value = std::getenv(key);
  if (value == nullptr) {
    return fallback;
  }
  char* end = nullptr;
  const auto parsed = std::strtoull(value, &end, 10);
  return end != value && *end == '\0' ? static_cast<std::uint64_t>(parsed) : fallback;
}

class Yield {
public:
  explicit Yield(Scheduler& scheduler) noexcept : scheduler_(&scheduler) {}

  bool await_ready() const noexcept { return false; }

  void await_suspend(std::coroutine_handle<> handle) noexcept {
    resume_.SetHandle(handle);
    scheduler_->Schedule(&resume_);
  }

  void await_resume() const noexcept {}

private:
  Scheduler* scheduler_;
  ResumeWork resume_{};
};

DetachedTask Tiny(std::uint64_t* completed) {
  ++*completed;
  co_return;
}

DetachedTask Pump(Loop& loop, std::uint64_t iterations, std::uint64_t batch_size) {
  std::uint64_t completed = 0;
  const auto start = std::chrono::steady_clock::now();

  for (std::uint64_t begin = 0; begin < iterations; begin += batch_size) {
    const std::uint64_t batch = std::min(batch_size, iterations - begin);
    const std::uint64_t target = completed + batch;
    for (std::uint64_t i = 0; i < batch; ++i) {
      SpawnDetach(loop, Tiny(&completed));
    }
    while (completed < target) {
      co_await Yield{loop};
    }
  }

  const double elapsed_ms =
      std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count();
  std::printf("iterations=%llu total_ms=%.3f\n", static_cast<unsigned long long>(iterations),
              elapsed_ms);
  std::fflush(stdout);
  loop.RequestStop();
  co_return;
}

}  // namespace

int main() {
  const std::uint64_t iterations = EnvU64("ITERATIONS", 1'000'000);
  const std::uint64_t batch_size = EnvU64("BATCH", 1024);
  const bool use_pool = EnvU64("FRAME_POOL", 0) != 0;
  if (iterations == 0 || batch_size == 0) {
    return 2;
  }

  std::optional<alyrn::coro::CoroFramePoolResource> pool;
  if (use_pool) {
    pool.emplace();
  }

  Loop loop{use_pool ? &*pool : nullptr};
  alyrn::uring::Options options;
  options.entries = 256;
  options.shared_buffer_capacity = 0;
  auto initialized = loop.Init(options);
  if (!initialized.HasValue()) {
    std::fprintf(stderr, "Loop::Init failed: %s\n", initialized.Error().message().c_str());
    return 1;
  }

  std::printf("frame_pool=%s batch=%llu\n", use_pool ? "on" : "off",
              static_cast<unsigned long long>(batch_size));
  std::fflush(stdout);

  SpawnDetach(loop, Pump(loop, iterations, batch_size));
  loop.Run();
  return 0;
}
