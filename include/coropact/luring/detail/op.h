// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <liburing/io_uring.h>

#include <cstdint>
#include <limits>
#include <memory>

#include "coropact/base/check.h"
#include "coropact/base/error.h"
#include "coropact/coro/work.h"
#include "coropact/luring/detail/reusable_completion_slot.h"
#include "coropact/operation/detail/single_result_lifecycle.h"

namespace coropact::luring::detail {

// Raw completion data passed from the loop to an operation-specific handler.
// Keeping the CQE result and flags together prevents a handler from silently
// interpreting a result without the completion flags that qualify it.
struct CompletionEvent {
  int result{0};
  std::uint32_t flags{0};

  [[nodiscard]]
  bool More() const noexcept {
    return (flags & IORING_CQE_F_MORE) != 0;
  }

  [[nodiscard]]
  bool Notification() const noexcept {
    return (flags & IORING_CQE_F_NOTIF) != 0;
  }

  [[nodiscard]]
  bool BufferMore() const noexcept {
    return (flags & IORING_CQE_F_BUF_MORE) != 0;
  }
};

// Completion handling is selected by the operation protocol, not by the loop's
// CQE path.  An event stream may produce multiple CQEs for one physical
// request; a split-release operation has separate kernel and logical release
// boundaries; all other operations are one-shot.
enum class LUringCompletionModel : std::uint8_t {
  kSingleShot,
  kEventStream,
  kSplitRelease,
};

// A completion handler returns physical bookkeeping decisions to the loop.
// It deliberately does not describe source state, queue draining, or buffer
// lease ownership; those remain in the operation-specific adapter.
struct CompletionDisposition {
  bool kernel_request_terminal{false};
  bool decrement_inflight{false};
  bool resume_continuation{false};
};

enum class LUringOpKind : std::uint8_t {
  kNone = 0,

  kAcceptComplete,
  // AcceptSource uses this kind for both native multishot accept and its
  // single-shot fallback. The CQE flags determine whether a request remains
  // active; the operation kind identifies the source completion handler.
  kAcceptSourceComplete,
  kAcceptSourceCancelComplete,
  kRecvSourceComplete,
  kRecvSourceCancelComplete,
  kSendZeroCopyComplete,
  kListenerCloseComplete,

  kReadComplete,
  kReadIntoComplete,
  kTimedReadComplete,
  kTimedReadTimeoutComplete,

  kWriteComplete,
  kStreamCloseComplete,

  kTimerDriverComplete,
  kTimerControlComplete,

  kConnect,
  kMsgRing,
  kWake,
  kCancelAll,
  kNop,

  // Count/placeholder sentinel. It is not a submit-able operation kind.
  kCount,
};

[[nodiscard]]
constexpr LUringCompletionModel CompletionModelFor(LUringOpKind kind) noexcept {
  switch (kind) {
    case LUringOpKind::kAcceptSourceComplete:
    case LUringOpKind::kRecvSourceComplete:
      return LUringCompletionModel::kEventStream;
    case LUringOpKind::kSendZeroCopyComplete:
      return LUringCompletionModel::kSplitRelease;
    case LUringOpKind::kNone:
    case LUringOpKind::kAcceptComplete:
    case LUringOpKind::kAcceptSourceCancelComplete:
    case LUringOpKind::kRecvSourceCancelComplete:
    case LUringOpKind::kListenerCloseComplete:
    case LUringOpKind::kReadComplete:
    case LUringOpKind::kReadIntoComplete:
    case LUringOpKind::kTimedReadComplete:
    case LUringOpKind::kTimedReadTimeoutComplete:
    case LUringOpKind::kWriteComplete:
    case LUringOpKind::kStreamCloseComplete:
    case LUringOpKind::kTimerDriverComplete:
    case LUringOpKind::kTimerControlComplete:
    case LUringOpKind::kConnect:
    case LUringOpKind::kMsgRing:
    case LUringOpKind::kWake:
    case LUringOpKind::kCancelAll:
    case LUringOpKind::kNop:
    case LUringOpKind::kCount:
      return LUringCompletionModel::kSingleShot;
  }

  return LUringCompletionModel::kSingleShot;
}

// These awaiters have one coupled logical result and release their
// operation-owned resource before their continuation runs. Some can publish
// the CQE result directly, while others first refine it into a richer value
// such as a connected stream.
[[nodiscard]]
constexpr bool UsesCoupledSingleResultLifecycle(LUringOpKind kind) noexcept {
  switch (kind) {
    case LUringOpKind::kReadComplete:
    case LUringOpKind::kReadIntoComplete:
    case LUringOpKind::kWriteComplete:
    case LUringOpKind::kAcceptComplete:
    case LUringOpKind::kConnect:
      return true;
    case LUringOpKind::kNone:
    case LUringOpKind::kAcceptSourceComplete:
    case LUringOpKind::kAcceptSourceCancelComplete:
    case LUringOpKind::kRecvSourceComplete:
    case LUringOpKind::kRecvSourceCancelComplete:
    case LUringOpKind::kSendZeroCopyComplete:
    case LUringOpKind::kListenerCloseComplete:
    case LUringOpKind::kTimedReadComplete:
    case LUringOpKind::kTimedReadTimeoutComplete:
    case LUringOpKind::kStreamCloseComplete:
    case LUringOpKind::kTimerDriverComplete:
    case LUringOpKind::kTimerControlComplete:
    case LUringOpKind::kMsgRing:
    case LUringOpKind::kWake:
    case LUringOpKind::kCancelAll:
    case LUringOpKind::kNop:
    case LUringOpKind::kCount:
      return false;
  }

  return false;
}

// Returns whether the raw CQE result is already the logical result exposed by
// await_resume(). Accept and Connect first convert a successful CQE into
// LUringStream, so their adapters authorize result readiness after that
// construction.
[[nodiscard]]
constexpr bool CqeResultDirectlyPublishesLogicalResult(LUringOpKind kind) noexcept {
  switch (kind) {
    case LUringOpKind::kReadComplete:
    case LUringOpKind::kReadIntoComplete:
    case LUringOpKind::kWriteComplete:
      return true;
    case LUringOpKind::kNone:
    case LUringOpKind::kAcceptComplete:
    case LUringOpKind::kAcceptSourceComplete:
    case LUringOpKind::kAcceptSourceCancelComplete:
    case LUringOpKind::kRecvSourceComplete:
    case LUringOpKind::kRecvSourceCancelComplete:
    case LUringOpKind::kSendZeroCopyComplete:
    case LUringOpKind::kListenerCloseComplete:
    case LUringOpKind::kTimedReadComplete:
    case LUringOpKind::kTimedReadTimeoutComplete:
    case LUringOpKind::kStreamCloseComplete:
    case LUringOpKind::kTimerDriverComplete:
    case LUringOpKind::kTimerControlComplete:
    case LUringOpKind::kConnect:
    case LUringOpKind::kMsgRing:
    case LUringOpKind::kWake:
    case LUringOpKind::kCancelAll:
    case LUringOpKind::kNop:
    case LUringOpKind::kCount:
      return false;
  }

  return false;
}

// A CQE result is always an integer. Kernel errors are represented by a
// negative cqe_res and converted to base::Error by the awaiter, so storing a
// full std::expected<int, Error> here needlessly adds the error union and its
// alignment padding to every operation. This compact representation relies on
// the Linux io_uring contract that cqe_res contains a non-INT_MIN result (a
// non-negative return value or a negative -errno). If a backend can return an
// arbitrary int, replace the sentinel encoding with a wider/tagged form.
class LUringCqeResult {
public:
  constexpr LUringCqeResult() noexcept = default;

  LUringCqeResult& operator=(int value) noexcept {
    COROPACT_CHECK(value != kEmpty, "INT_MIN is reserved for an empty CQE result");
    COROPACT_CHECK(!HasValue(), "LUringCqeResult was assigned twice");
    encoded_ = static_cast<std::int32_t>(value);
    return *this;
  }

  [[nodiscard]]
  bool HasValue() const noexcept {
    return encoded_ != kEmpty;
  }

  [[nodiscard]]
  int operator*() const noexcept {
    COROPACT_CHECK(HasValue(), "LUringCqeResult was read before completion");
    return static_cast<int>(encoded_);
  }

  void Clear() noexcept { encoded_ = kEmpty; }

private:
  static constexpr std::int32_t kEmpty = std::numeric_limits<std::int32_t>::min();
  std::int32_t encoded_{kEmpty};
};

static_assert(sizeof(int) == sizeof(std::int32_t));
static_assert(sizeof(LUringCqeResult) == 4);

class LUringOp {
public:
  LUringOp() noexcept = default;
  explicit LUringOp(LUringOpKind operation_kind) noexcept : kind(operation_kind) {}

  coro::ResumeWork resume_work;
  LUringCqeResult result;
  LUringOpKind kind{};

  // Records one physical CQE result. When the CQE is itself the logical
  // result, this also enters the coupled result-ready phase. Adapters such as
  // Connect first refine the CQE into a richer result, then authorize it.
  [[nodiscard]]
  bool TryRecordCqeCompletion(int cqe_res) noexcept {
    if (!completion_slot_.TryComplete()) {
      return false;
    }

    result = cqe_res;
    if (CqeResultDirectlyPublishesLogicalResult(kind)) {
      COROPACT_CHECK(single_result_lifecycle_.TryAuthorizeResult(),
                     "coupled single-result operation recorded a CQE twice");
    }
    return true;
  }

  // Some operation protocols have more than one CQE and keep their primary
  // result outside LUringOp. They mark the operation terminal only after the
  // final CQE has been interpreted by the operation-specific handler.
  [[nodiscard]]
  bool TryMarkCompletionWithoutCqeResult() noexcept {
    return completion_slot_.TryComplete();
  }

  void SetImmediateSuccess() noexcept { result = 0; }

  void SetImmediateError(base::Error error) noexcept {
    COROPACT_CHECK(error.value() > 0, "LUringOp immediate error must have a positive errno");
    result = -error.value();
  }

  [[nodiscard]]
  LUringOpKind DispatchKind() const noexcept {
    return kind;
  }

  [[nodiscard]]
  bool CqeCompletionRecorded() const noexcept {
    return completion_slot_.Completed();
  }

  // The coupled lifecycle is intentionally separate from the reusable
  // physical completion slot. It does not govern composites, sources, close,
  // or split-release operations.
  [[nodiscard]]
  bool TryAuthorizeCoupledResult() noexcept {
    COROPACT_CHECK(UsesCoupledSingleResultLifecycle(kind),
                   "result authorization requested for a non-coupled LUring operation");
    return single_result_lifecycle_.TryAuthorizeResult();
  }

  [[nodiscard]]
  bool TryAuthorizeCoupledRelease() noexcept {
    COROPACT_CHECK(UsesCoupledSingleResultLifecycle(kind),
                   "release authorization requested for a non-coupled LUring operation");
    return single_result_lifecycle_.TryAuthorizeRelease();
  }

  [[nodiscard]]
  bool TryAuthorizeCoupledContinuation() noexcept {
    COROPACT_CHECK(UsesCoupledSingleResultLifecycle(kind),
                   "continuation authorization requested for a non-coupled LUring operation");
    return single_result_lifecycle_.TryAuthorizeContinuation();
  }

  [[nodiscard]]
  bool CoupledResultReady() const noexcept {
    return single_result_lifecycle_.ResultReady();
  }

  [[nodiscard]]
  bool CoupledReleaseAuthorized() const noexcept {
    return single_result_lifecycle_.ReleaseAuthorized();
  }

  [[nodiscard]]
  bool CoupledContinuationAuthorized() const noexcept {
    return single_result_lifecycle_.ContinuationAuthorized();
  }

  // Starts the next request in a reusable physical slot. Call only after the
  // previous request reached its release point. This clears
  // the prior CQE result and continuation so stale state cannot leak across
  // physical requests.
  void BeginNextRequest() noexcept {
    completion_slot_.BeginNextRequest();
    std::destroy_at(std::addressof(single_result_lifecycle_));
    std::construct_at(std::addressof(single_result_lifecycle_));
    result.Clear();
    resume_work.ClearHandle();
  }

private:
  detail::ReusableCompletionSlot completion_slot_;
  operation::detail::SingleResultLifecycle single_result_lifecycle_;
};

static_assert(sizeof(LUringOp) == 24);

}  // namespace coropact::luring::detail
