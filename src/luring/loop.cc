// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include "vexo/luring/loop.h"

#include <liburing.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <expected>
#include <stop_token>
#include <utility>

#include "vexo/base/ctrack.h"
#include "vexo/base/current_thread.h"
#include "vexo/base/error.h"
#include "vexo/coro/scheduler.h"
#include "vexo/luring/op.h"
#include "vexo/luring/options.h"
#include "vexo/luring/ring.h"

namespace vexo::luring {

namespace {

constexpr std::chrono::milliseconds kStopPollInterval{100};

[[nodiscard]] std::uint64_t NowNs() noexcept {
  return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                        std::chrono::steady_clock::now().time_since_epoch())
                                        .count());
}

void RecordLatency(std::array<std::uint64_t, kLatencyHistogramBucketCount>& histogram,
                   std::uint64_t latency_ns) noexcept {
  std::size_t bucket = 0;
  for (std::uint64_t value = latency_ns; value > 1 && bucket + 1 < kLatencyHistogramBucketCount;
       value >>= 1) {
    ++bucket;
  }
  ++histogram[bucket];
}

[[nodiscard]] std::uint64_t HistogramPercentileNs(
    const std::array<std::uint64_t, kLatencyHistogramBucketCount>& histogram, std::uint64_t samples,
    std::uint64_t percentile) noexcept {
  if (samples == 0) {
    return 0;
  }

  const std::uint64_t quotient = samples / 100;
  const std::uint64_t remainder = samples % 100;
  const std::uint64_t rank =
      std::max<std::uint64_t>(1, quotient * percentile + (remainder * percentile + 99) / 100);
  std::uint64_t cumulative = 0;
  for (std::size_t bucket = 0; bucket < histogram.size(); ++bucket) {
    cumulative += histogram[bucket];
    if (cumulative >= rank) {
      return bucket >= 63 ? UINT64_MAX : (std::uint64_t{1} << (bucket + 1));
    }
  }
  return UINT64_MAX;
}

[[nodiscard]] LUringOp* DecodeOp(io_uring_cqe* cqe) noexcept {
  return reinterpret_cast<LUringOp*>(io_uring_cqe_get_data(cqe));
}

}  // namespace

LUringLoop::LUringLoop(std::pmr::memory_resource* frame_resource)
    : Scheduler(frame_resource), thread_id_(base::tid()), timers_(this) {}

LUringLoop::~LUringLoop() noexcept {
  if (wake_fd_ >= 0) {
    ::close(wake_fd_);
  }
}

base::Result<void> LUringLoop::Init(const LUringOptions& options) noexcept {
  assert(IsInLoopThread());

  if (initialized_) {
    return std::unexpected(base::make_errno(EALREADY));
  }

  auto ring = LUringRing::Create(options);
  if (!ring.has_value()) {
    return std::unexpected(ring.error());
  }

  ring_ = std::move(*ring);
  wake_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (wake_fd_ < 0) {
    return std::unexpected(base::CurrentErrno());
  }
  submit_batch_ = options.submit_batch == 0 ? 1 : options.submit_batch;
  max_ready_work_per_turn_ = options.max_ready_work_per_turn;
  max_cqe_per_turn_ = options.max_cqe_per_turn;
  max_ready_time_per_turn_ = options.max_ready_time_per_turn;
  max_completion_work_per_turn_ = options.max_completion_work_per_turn;
  completion_queue_age_threshold_ =
      options.completion_queue_age_threshold > std::chrono::microseconds::zero()
          ? options.completion_queue_age_threshold
          : std::chrono::microseconds::zero();
  max_urgent_completion_work_per_turn_ = options.max_urgent_completion_work_per_turn;
  normal_queue_age_threshold_ =
      options.normal_queue_age_threshold > std::chrono::microseconds::zero()
          ? options.normal_queue_age_threshold
          : std::chrono::microseconds::zero();
  stats_enabled_ = options.collect_stats || options.dump_stats_on_exit;
  dump_stats_on_exit_ = options.dump_stats_on_exit;
  ready_depth_ = 0;
  completion_ready_depth_ = 0;
  ready_nonempty_since_ns_ = 0;
  completion_ready_nonempty_since_ns_ = 0;
  ready_samples_.clear();
  completion_ready_samples_.clear();
  stats_ = {};
  pending_submit_ = 0;
  inflight_ = 0;
  wake_pending_ = false;
  wake_inflight_ = false;
  quit_.store(false, std::memory_order_relaxed);
  initialized_ = true;
  auto armed = ArmWakePoll();
  if (!armed.has_value()) {
    initialized_ = false;
    ::close(std::exchange(wake_fd_, -1));
    return std::unexpected(armed.error());
  }
  return {};
}

void LUringLoop::Loop(std::stop_token token) noexcept {
  assert(IsInLoopThread());

  if (!initialized_) {
    return;
  }

  while (!token.stop_requested() && !quit_.load(std::memory_order_relaxed)) {
    // Observe already available completions before spending the turn on
    // ready work. This prevents a ready backlog from delaying CQE handling.
    auto completed = PollCompletions();
    if (!completed.has_value()) {
      break;
    }

    if (token.stop_requested() || quit_.load(std::memory_order_relaxed)) {
      break;
    }

    RunReady();

    if (*completed == 0 && !HasReadyWork() && inflight_ > 0) {
      completed = WaitCompletionsFor(kStopPollInterval);
      if (!completed.has_value() && completed.error().value() != ETIME) {
        break;
      }
    }
  }

  if (dump_stats_on_exit_) {
    DumpStats();
  }
}

void LUringLoop::Quit() noexcept {
  if (!quit_.exchange(true, std::memory_order_acq_rel)) {
    Wake();
  }
}

void LUringLoop::Schedule(coro::Work* work) noexcept {
  assert(IsInLoopThread());
  const std::uint64_t enqueued_ns = stats_enabled_ ? NowNs() : 0;
  if (ready_depth_ == 0) {
    ready_nonempty_since_ns_ = stats_enabled_ ? enqueued_ns : NowNs();
  }
  ++ready_depth_;
  if (stats_enabled_) {
    ++stats_.normal_work_enqueued;
    stats_.max_normal_ready_depth =
        std::max(stats_.max_normal_ready_depth, static_cast<std::uint64_t>(ready_depth_));
    ready_samples_.push_back(ReadySample{.enqueued_ns = enqueued_ns});
  }
  ready_.PushBack(work);
}

void LUringLoop::ScheduleCompletion(coro::Work* work) noexcept { ScheduleCompletionAt(work, 0); }

void LUringLoop::ScheduleCompletionAt(coro::Work* work, std::uint64_t event_ns) noexcept {
  assert(IsInLoopThread());
  const std::uint64_t enqueued_ns = stats_enabled_ ? NowNs() : 0;
  if (event_ns == 0) {
    event_ns = enqueued_ns;
  }
  if (completion_ready_depth_ == 0) {
    completion_ready_nonempty_since_ns_ = stats_enabled_ ? enqueued_ns : NowNs();
  }
  ++completion_ready_depth_;
  if (stats_enabled_) {
    ++stats_.completion_work_enqueued;
    stats_.max_completion_ready_depth = std::max(
        stats_.max_completion_ready_depth, static_cast<std::uint64_t>(completion_ready_depth_));
    const std::uint64_t event_to_enqueue_ns = enqueued_ns - event_ns;
    ++stats_.completion_event_to_enqueue_samples;
    stats_.completion_event_to_enqueue_sum_ns += event_to_enqueue_ns;
    stats_.completion_event_to_enqueue_max_ns =
        std::max(stats_.completion_event_to_enqueue_max_ns, event_to_enqueue_ns);
    completion_ready_samples_.push_back(
        ReadySample{.enqueued_ns = enqueued_ns, .event_ns = event_ns});
  }
  completion_ready_.PushBack(work);
}

void LUringLoop::RunReady() noexcept {
  assert(IsInLoopThread());

  coro::Scheduler* previous = coro::Scheduler::Current();
  coro::Scheduler::SetCurrent(this);

  const std::uint64_t turn_start_ns = NowNs();
  const auto configured_time_budget = max_ready_time_per_turn_ > std::chrono::microseconds::zero()
                                          ? max_ready_time_per_turn_
                                          : std::chrono::microseconds::zero();
  const std::uint64_t time_budget_ns = static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(configured_time_budget).count());
  const auto configured_age_threshold =
      completion_queue_age_threshold_ > std::chrono::microseconds::zero()
          ? completion_queue_age_threshold_
          : std::chrono::microseconds::zero();
  const std::uint64_t completion_age_threshold_ns = static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(configured_age_threshold).count());
  const auto configured_normal_age_threshold =
      normal_queue_age_threshold_ > std::chrono::microseconds::zero()
          ? normal_queue_age_threshold_
          : std::chrono::microseconds::zero();
  const std::uint64_t normal_age_threshold_ns = static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(configured_normal_age_threshold)
          .count());
  const bool completion_is_urgent =
      completion_ready_depth_ > 0 && completion_age_threshold_ns != 0 &&
      turn_start_ns - completion_ready_nonempty_since_ns_ >= completion_age_threshold_ns;
  const bool normal_is_overdue =
      ready_depth_ > 0 && normal_age_threshold_ns != 0 &&
      turn_start_ns - ready_nonempty_since_ns_ >= normal_age_threshold_ns;
  const bool use_urgent_completion_budget = completion_is_urgent && !normal_is_overdue;
  const std::size_t completion_budget =
      use_urgent_completion_budget && max_urgent_completion_work_per_turn_ != 0
          ? max_urgent_completion_work_per_turn_
          : max_completion_work_per_turn_;
  if (stats_enabled_ && use_urgent_completion_budget) {
    ++stats_.urgent_completion_turn_count;
  }
  if (stats_enabled_ && normal_is_overdue) {
    ++stats_.normal_priority_turn_count;
  }
  if (stats_enabled_) {
    if (ready_depth_ > 0) {
      const std::uint64_t age_ns = turn_start_ns - ready_nonempty_since_ns_;
      ++stats_.normal_queue_age_samples;
      stats_.normal_queue_age_sum_ns += age_ns;
      stats_.normal_queue_age_max_ns = std::max(stats_.normal_queue_age_max_ns, age_ns);
    }
    if (completion_ready_depth_ > 0) {
      const std::uint64_t age_ns = turn_start_ns - completion_ready_nonempty_since_ns_;
      ++stats_.completion_queue_age_samples;
      stats_.completion_queue_age_sum_ns += age_ns;
      stats_.completion_queue_age_max_ns = std::max(stats_.completion_queue_age_max_ns, age_ns);
    }
  }

  std::size_t resumed = 0;
  std::size_t completion_resumed = 0;
  while (HasReadyWork() && (max_ready_work_per_turn_ == 0 || resumed < max_ready_work_per_turn_)) {
    coro::Work* work = nullptr;
    ReadySample sample;
    const bool run_completion =
        !completion_ready_.empty() &&
        (ready_.empty() || (!normal_is_overdue &&
                            (completion_budget == 0 || completion_resumed < completion_budget)));
    if (run_completion) {
      work = completion_ready_.PopFront();
      assert(completion_ready_depth_ > 0);
      --completion_ready_depth_;
      if (completion_ready_depth_ == 0) {
        completion_ready_nonempty_since_ns_ = 0;
      }
      ++completion_resumed;
      if (stats_enabled_) {
        assert(!completion_ready_samples_.empty());
        sample = completion_ready_samples_.front();
        completion_ready_samples_.pop_front();
        ++stats_.completion_work_resumed;
        if (use_urgent_completion_budget) {
          ++stats_.urgent_completion_work_resumed;
        }
      }
    } else {
      work = ready_.PopFront();
      assert(ready_depth_ > 0);
      --ready_depth_;
      if (ready_depth_ == 0) {
        ready_nonempty_since_ns_ = 0;
      }
      if (stats_enabled_) {
        assert(!ready_samples_.empty());
        sample = ready_samples_.front();
        ready_samples_.pop_front();
        ++stats_.normal_work_resumed;
      }
    }

    const std::uint64_t work_start_ns = stats_enabled_ ? NowNs() : 0;
    if (stats_enabled_) {
      const std::uint64_t queue_wait_ns = work_start_ns - sample.enqueued_ns;
      if (run_completion) {
        ++stats_.completion_queue_wait_samples;
        stats_.completion_queue_wait_sum_ns += queue_wait_ns;
        stats_.completion_queue_wait_max_ns =
            std::max(stats_.completion_queue_wait_max_ns, queue_wait_ns);
        RecordLatency(stats_.completion_queue_wait_histogram, queue_wait_ns);

        const std::uint64_t event_to_resume_ns = work_start_ns - sample.event_ns;
        ++stats_.completion_event_to_resume_samples;
        stats_.completion_event_to_resume_sum_ns += event_to_resume_ns;
        stats_.completion_event_to_resume_max_ns =
            std::max(stats_.completion_event_to_resume_max_ns, event_to_resume_ns);
        RecordLatency(stats_.completion_event_to_resume_histogram, event_to_resume_ns);
      } else {
        ++stats_.normal_queue_wait_samples;
        stats_.normal_queue_wait_sum_ns += queue_wait_ns;
        stats_.normal_queue_wait_max_ns = std::max(stats_.normal_queue_wait_max_ns, queue_wait_ns);
        RecordLatency(stats_.normal_queue_wait_histogram, queue_wait_ns);
      }
    }
    {
      VEXO_CTRACK_SCOPE("luring.ready.work");
      Run(work);
    }
    if (stats_enabled_) {
      const std::uint64_t work_time_ns = NowNs() - work_start_ns;
      ++stats_.work_run_count;
      stats_.work_run_time_sum_ns += work_time_ns;
      stats_.work_run_time_max_ns = std::max(stats_.work_run_time_max_ns, work_time_ns);
    }
    ++resumed;

    if (time_budget_ns != 0 && NowNs() - turn_start_ns >= time_budget_ns) {
      break;
    }
  }

  if (stats_enabled_) {
    const std::uint64_t turn_time_ns = NowNs() - turn_start_ns;
    ++stats_.ready_turn_count;
    stats_.ready_turn_time_sum_ns += turn_time_ns;
    stats_.ready_turn_time_max_ns = std::max(stats_.ready_turn_time_max_ns, turn_time_ns);
  }

  coro::Scheduler::SetCurrent(previous);
}

void LUringLoop::RunUntilIdle() {
  assert(IsInLoopThread());

  if (!initialized_) {
    return;
  }

  while (HasReadyWork() || PendingSubmitCount() > 0 || InflightCount() > 0) {
    RunReady();

    if (PendingSubmitCount() == 0 && InflightCount() == 0) {
      continue;
    }

    auto completed = WaitCompletions();
    if (!completed.has_value()) {
      break;
    }
  }

  RunReady();
}

base::Result<void> LUringLoop::FlushSubmit() noexcept {
  assert(IsInLoopThread());

  while (pending_submit_ > 0) {
    base::Result<std::size_t> submitted;
    {
      VEXO_CTRACK_SCOPE("luring.ring.submit");
      submitted = ring_.Submit();
    }
    if (!submitted.has_value()) {
      return std::unexpected(submitted.error());
    }
    if (*submitted == 0) {
      return std::unexpected(base::make_errno(EAGAIN));
    }

    const std::size_t n = std::min(*submitted, pending_submit_);
    pending_submit_ -= n;
    inflight_ += n;
    if (wake_pending_ && n > 0) {
      wake_pending_ = false;
      wake_inflight_ = true;
    }
  }

  return {};
}

base::Result<std::size_t> LUringLoop::PollCompletions() noexcept {
  assert(IsInLoopThread());

  auto flushed = FlushSubmit();
  if (!flushed.has_value()) {
    return std::unexpected(flushed.error());
  }

  {
    VEXO_CTRACK_SCOPE("luring.ring.reap");
    auto reaped = ring_.Reap([this](io_uring_cqe* cqe) { HandleCqe(cqe); }, max_cqe_per_turn_);
    if (stats_enabled_) {
      ++stats_.poll_count;
      if (reaped.has_value()) {
        stats_.cqe_count += *reaped;
        if (*reaped > 0) {
          ++stats_.cqe_batch_count;
          stats_.max_cqe_batch =
              std::max(stats_.max_cqe_batch, static_cast<std::uint64_t>(*reaped));
        }
      }
    }
    return reaped;
  }
}

base::Result<std::size_t> LUringLoop::WaitCompletions() noexcept {
  return WaitCompletionsFor(std::chrono::nanoseconds::max());
}

base::Result<std::size_t> LUringLoop::WaitCompletionsFor(
    std::chrono::nanoseconds timeout) noexcept {
  assert(IsInLoopThread());

  auto flushed = FlushSubmit();
  if (!flushed.has_value()) {
    return std::unexpected(flushed.error());
  }

  io_uring_cqe* cqe = nullptr;
  int r = 0;
  {
    VEXO_CTRACK_SCOPE("luring.ring.wait");
    if (timeout == std::chrono::nanoseconds::max()) {
      r = io_uring_wait_cqe(ring_.native(), &cqe);
    } else {
      constexpr std::int64_t kNanosecondsPerSecond = 1'000'000'000;
      const std::int64_t count = timeout.count();
      __kernel_timespec timeout_spec{};
      timeout_spec.tv_sec = count / kNanosecondsPerSecond;
      timeout_spec.tv_nsec = count % kNanosecondsPerSecond;
      r = io_uring_wait_cqe_timeout(ring_.native(), &cqe, &timeout_spec);
    }
  }
  if (r < 0) {
    return std::unexpected(base::make_neg_errno(r));
  }

  {
    VEXO_CTRACK_SCOPE("luring.ring.reap");
    auto reaped = ring_.Reap([this](io_uring_cqe* completed_cqe) { HandleCqe(completed_cqe); },
                             max_cqe_per_turn_);
    if (stats_enabled_) {
      ++stats_.wait_count;
      if (reaped.has_value()) {
        stats_.cqe_count += *reaped;
        if (*reaped > 0) {
          ++stats_.cqe_batch_count;
          stats_.max_cqe_batch =
              std::max(stats_.max_cqe_batch, static_cast<std::uint64_t>(*reaped));
        }
      }
    }
    return reaped;
  }
}

void LUringLoop::HandleCqe(io_uring_cqe* cqe) noexcept {
  assert(IsInLoopThread());

  if (cqe->user_data == kMsgRingNotificationUserData) {
    HandleMailbox();
    return;
  }

  LUringOp* op = DecodeOp(cqe);
  if (op == nullptr) {
    if (inflight_ > 0) {
      --inflight_;
    }
    return;
  }

  assert(inflight_ > 0);
  if (inflight_ > 0) {
    --inflight_;
  }

  if (op == &wake_op_) {
    wake_inflight_ = false;
    DrainWakeFd();
    if (!quit_.load(std::memory_order_acquire)) {
      wake_op_.completed = false;
      wake_op_.result = {};
      auto armed = ArmWakePoll();
      if (!armed.has_value()) {
        quit_.store(true, std::memory_order_release);
      }
    }
    return;
  }

  const std::uint64_t event_ns = stats_enabled_ ? NowNs() : 0;
  {
    VEXO_CTRACK_SCOPE("luring.cqe.complete");
    op->Complete(cqe->res);
  }
  if (op->resume_work.handle) {
    ScheduleCompletionAt(&op->resume_work, event_ns);
  }
}

base::Result<void> LUringLoop::ArmWakePoll() noexcept {
  assert(IsInLoopThread());
  if (wake_fd_ < 0) {
    return std::unexpected(base::make_errno(EBADF));
  }

  wake_op_.kind = LUringOpKind::kWake;
  wake_op_.continuation_ = {};
  wake_op_.resume_work.handle = {};
  wake_op_.owner = this;
  wake_op_.on_complete = nullptr;
  auto submitted = SubmitOp(&wake_op_, [fd = wake_fd_](io_uring_sqe* sqe) noexcept {
    io_uring_prep_poll_add(sqe, fd, POLLIN);
  });
  if (submitted.has_value()) {
    wake_pending_ = true;
  }
  return submitted;
}

void LUringLoop::DrainWakeFd() noexcept {
  if (wake_fd_ < 0) {
    return;
  }

  std::uint64_t value = 0;
  while (::read(wake_fd_, &value, sizeof(value)) == sizeof(value)) {
  }
}

void LUringLoop::Wake() noexcept {
  if (wake_fd_ < 0) {
    return;
  }

  constexpr std::uint64_t kWakeValue = 1;
  const ssize_t written = ::write(wake_fd_, &kWakeValue, sizeof(kWakeValue));
  if (written < 0 && errno != EAGAIN && errno != EINTR) {
    quit_.store(true, std::memory_order_release);
  }
}

void LUringLoop::HandleMailbox() noexcept {
  assert(IsInLoopThread());

  DrainMessages([this](const LUringMessage& message) noexcept {
    switch (message.type) {
      case LUringMessage::Type::kResume: {
        auto* work = reinterpret_cast<coro::Work*>(static_cast<std::uintptr_t>(message.data));
        if (work == nullptr) {
          assert(false && "mailbox resume message contains a null work pointer");
          return;
        }
        ScheduleCompletion(work);
        return;
      }
      case LUringMessage::Type::kFunction:
        assert(false && "mailbox function messages are not implemented");
        return;
    }

    assert(false && "unknown mailbox message type");
  });
}

void LUringLoop::DumpStats() const noexcept {
  const auto& stats = stats_;
  const auto normal_wait_p95_ns =
      HistogramPercentileNs(stats.normal_queue_wait_histogram, stats.normal_queue_wait_samples, 95);
  const auto normal_wait_p99_ns =
      HistogramPercentileNs(stats.normal_queue_wait_histogram, stats.normal_queue_wait_samples, 99);
  const auto completion_wait_p95_ns = HistogramPercentileNs(
      stats.completion_queue_wait_histogram, stats.completion_queue_wait_samples, 95);
  const auto completion_wait_p99_ns = HistogramPercentileNs(
      stats.completion_queue_wait_histogram, stats.completion_queue_wait_samples, 99);
  const auto completion_event_resume_p95_ns = HistogramPercentileNs(
      stats.completion_event_to_resume_histogram, stats.completion_event_to_resume_samples, 95);
  const auto completion_event_resume_p99_ns = HistogramPercentileNs(
      stats.completion_event_to_resume_histogram, stats.completion_event_to_resume_samples, 99);
  std::fprintf(
      stderr,
      "[luring.stats] tid=%d cqe=%llu cqe_batches=%llu max_cqe_batch=%llu normal_enqueued=%llu "
      "completion_enqueued=%llu normal_resumed=%llu completion_resumed=%llu "
      "urgent_completion_turns=%llu urgent_completion_resumed=%llu "
      "normal_priority_turns=%llu max_normal_depth=%llu max_completion_depth=%llu "
      "normal_age_max_ms=%.3f "
      "completion_age_max_ms=%.3f ready_turns=%llu ready_turn_max_ms=%.3f "
      "normal_wait_p95_ms=%.3f normal_wait_p99_ms=%.3f "
      "completion_wait_p95_ms=%.3f completion_wait_p99_ms=%.3f "
      "normal_wait_samples=%llu completion_wait_samples=%llu "
      "event_enqueue_max_us=%.3f event_resume_p95_ms=%.3f event_resume_p99_ms=%.3f "
      "event_resume_samples=%llu "
      "work_runs=%llu work_max_us=%.3f\n",
      thread_id_, static_cast<unsigned long long>(stats.cqe_count),
      static_cast<unsigned long long>(stats.cqe_batch_count),
      static_cast<unsigned long long>(stats.max_cqe_batch),
      static_cast<unsigned long long>(stats.normal_work_enqueued),
      static_cast<unsigned long long>(stats.completion_work_enqueued),
      static_cast<unsigned long long>(stats.normal_work_resumed),
      static_cast<unsigned long long>(stats.completion_work_resumed),
      static_cast<unsigned long long>(stats.urgent_completion_turn_count),
      static_cast<unsigned long long>(stats.urgent_completion_work_resumed),
      static_cast<unsigned long long>(stats.normal_priority_turn_count),
      static_cast<unsigned long long>(stats.max_normal_ready_depth),
      static_cast<unsigned long long>(stats.max_completion_ready_depth),
      static_cast<double>(stats.normal_queue_age_max_ns) / 1'000'000.0,
      static_cast<double>(stats.completion_queue_age_max_ns) / 1'000'000.0,
      static_cast<unsigned long long>(stats.ready_turn_count),
      static_cast<double>(stats.ready_turn_time_max_ns) / 1'000'000.0,
      static_cast<double>(normal_wait_p95_ns) / 1'000'000.0,
      static_cast<double>(normal_wait_p99_ns) / 1'000'000.0,
      static_cast<double>(completion_wait_p95_ns) / 1'000'000.0,
      static_cast<double>(completion_wait_p99_ns) / 1'000'000.0,
      static_cast<unsigned long long>(stats.normal_queue_wait_samples),
      static_cast<unsigned long long>(stats.completion_queue_wait_samples),
      static_cast<double>(stats.completion_event_to_enqueue_max_ns) / 1'000.0,
      static_cast<double>(completion_event_resume_p95_ns) / 1'000'000.0,
      static_cast<double>(completion_event_resume_p99_ns) / 1'000'000.0,
      static_cast<unsigned long long>(stats.completion_event_to_resume_samples),
      static_cast<unsigned long long>(stats.work_run_count),
      static_cast<double>(stats.work_run_time_max_ns) / 1'000.0);
}

}  // namespace vexo::luring
