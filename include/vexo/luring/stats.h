// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

namespace vexo::luring {

inline constexpr std::size_t kLatencyHistogramBucketCount = 64;

// Per-loop counters for low-overhead scheduler observation. The loop owns the
// counters and updates them on its event-loop thread; callers must read a
// snapshot from that thread or after the loop has stopped.
struct LUringLoopStats {
  std::uint64_t poll_count{0};
  std::uint64_t wait_count{0};
  std::uint64_t cqe_count{0};
  std::uint64_t cqe_batch_count{0};
  std::uint64_t max_cqe_batch{0};

  std::uint64_t normal_work_enqueued{0};
  std::uint64_t completion_work_enqueued{0};
  std::uint64_t normal_work_resumed{0};
  std::uint64_t completion_work_resumed{0};
  std::uint64_t urgent_completion_turn_count{0};
  std::uint64_t urgent_completion_work_resumed{0};
  std::uint64_t normal_priority_turn_count{0};
  std::uint64_t max_normal_ready_depth{0};
  std::uint64_t max_completion_ready_depth{0};

  std::uint64_t normal_queue_wait_samples{0};
  std::uint64_t normal_queue_wait_sum_ns{0};
  std::uint64_t normal_queue_wait_max_ns{0};
  std::uint64_t completion_queue_wait_samples{0};
  std::uint64_t completion_queue_wait_sum_ns{0};
  std::uint64_t completion_queue_wait_max_ns{0};

  std::uint64_t completion_event_to_enqueue_samples{0};
  std::uint64_t completion_event_to_enqueue_sum_ns{0};
  std::uint64_t completion_event_to_enqueue_max_ns{0};
  std::uint64_t completion_event_to_resume_samples{0};
  std::uint64_t completion_event_to_resume_sum_ns{0};
  std::uint64_t completion_event_to_resume_max_ns{0};

  std::array<std::uint64_t, kLatencyHistogramBucketCount> normal_queue_wait_histogram{};
  std::array<std::uint64_t, kLatencyHistogramBucketCount> completion_queue_wait_histogram{};
  std::array<std::uint64_t, kLatencyHistogramBucketCount> completion_event_to_resume_histogram{};
  // Histogram percentiles are upper bounds of power-of-two nanosecond buckets.

  // These ages measure how long a queue has remained non-empty. They are a
  // backlog signal, not a per-work enqueue-to-resume latency measurement.
  std::uint64_t normal_queue_age_samples{0};
  std::uint64_t normal_queue_age_sum_ns{0};
  std::uint64_t normal_queue_age_max_ns{0};
  std::uint64_t completion_queue_age_samples{0};
  std::uint64_t completion_queue_age_sum_ns{0};
  std::uint64_t completion_queue_age_max_ns{0};

  std::uint64_t ready_turn_count{0};
  std::uint64_t ready_turn_time_sum_ns{0};
  std::uint64_t ready_turn_time_max_ns{0};
  std::uint64_t work_run_count{0};
  std::uint64_t work_run_time_sum_ns{0};
  std::uint64_t work_run_time_max_ns{0};
};

}  // namespace vexo::luring
