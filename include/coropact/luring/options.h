// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>

#include "coropact/luring/capability.h"

namespace coropact::luring {

// LUringOptions configures the ownership, queue sizing, polling mode,
// and submission batching policy of one io_uring-backed event loop.
struct LUringOptions {
  std::uint32_t entries{4096};
  std::uint32_t cq_entries{0};

  // The loop binds this native profile during Init(). Core is the safe
  // default; optional io_uring mechanisms must be opted into explicitly.
  RuntimeProfile active_profile{RuntimeProfile::Core()};

  // SQPOLL creates a kernel submission thread per ring and is opt-in.
  bool setup_sqpoll{false};
  std::uint32_t sqpoll_idle_ms{1000};

  // Defers io_uring task work until an enter(GETEVENTS) transition. This is
  // valid only with SINGLE_ISSUER; enabling it also enables cooperative task
  // running and the SQ task-run hint used by liburing's peek path.
  bool setup_defer_taskrun{false};

  bool setup_iopoll{false};  // don't set it true.
  bool setup_submit_all{true};
  bool setup_single_issuer{true};

  std::size_t submit_batch{32};

  // Maximum number of ready coroutine works resumed before polling CQEs
  // again. This bounds scheduler monopolization under a completion burst.
  // Zero disables the budget and drains the ready queue completely.
  std::size_t max_ready_work_per_turn{256};

  // Maximum number of CQEs consumed by one poll or wait turn. This bounds
  // completion processing when the kernel returns a large CQE batch.
  // Zero disables the budget and consumes all currently available CQEs.
  std::size_t max_cqe_per_turn{256};

  // Maximum wall-clock time spent resuming ready work in one turn. The count
  // budget above still applies. Zero disables the time budget.
  std::chrono::microseconds max_ready_time_per_turn{50};

  // Maximum number of completion-ready works selected before normal ready
  // work receives service in one turn. Zero disables this sub-budget; the
  // total ready-work and time budgets still apply.
  std::size_t max_completion_work_per_turn{64};

  // Promotes a completion-ready queue whose oldest item reaches this age to
  // the urgent completion budget. Zero disables age-based promotion.
  std::chrono::microseconds completion_queue_age_threshold{0};

  // Completion budget used while age-based promotion is active. It remains
  // bounded so an urgent completion backlog cannot permanently starve normal
  // ready work. Zero falls back to max_completion_work_per_turn.
  std::size_t max_urgent_completion_work_per_turn{80};

  // Age at which normal ready work suppresses completion promotion for the
  // current turn. Zero disables this protection.
  std::chrono::microseconds normal_queue_age_threshold{5000};

  // Loop-wide provided-buffer ring used by RecvSources. This is an aggregate
  // per-worker capacity, shared by all sources on the loop. Set it to zero
  // to disable RecvSource creation.
  std::size_t shared_buffer_capacity{64};
  std::size_t shared_buffer_size{16 * 1024};

};

}  // namespace coropact::luring
