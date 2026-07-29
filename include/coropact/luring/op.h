// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <liburing/io_uring.h>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>

#include "coropact/base/error.h"
#include "coropact/coro/work.h"
#include "coropact/operation/detail/completion_gate.h"

namespace coropact::luring {

// Raw completion data passed from the loop to an operation-family handler.
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

// A split-release operation can have separate kernel and user-visible
// completion boundaries.  The loop owns inflight accounting, while the
// operation family decides when its continuation may resume.
struct CompletionDisposition {
  bool kernel_operation_done{false};
  bool logical_completion_ready{false};
};

enum class LUringOpKind : std::uint8_t {
  kNone = 0,

  kAcceptComplete,
  // AcceptSource uses this family for both native multishot accept and its
  // single-shot fallback. The CQE flags determine whether a request remains
  // active; the operation kind identifies the source completion handler.
  kAcceptSourceComplete,
  kAcceptSourceCancelComplete,
  kRecvSourceComplete,
  kRecvSourceCancelComplete,
  kSendZeroCopyComplete,
  kListenerCloseComplete,

  kReadComplete,
  kTimedReadComplete,
  kTimedReadTimeoutComplete,

  kWriteComplete,
  kWritePartsComplete,
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
    assert(value != kEmpty && "INT_MIN is reserved for an empty CQE result");
    encoded_ = static_cast<std::int32_t>(value);
    return *this;
  }

  [[nodiscard]]
  bool HasValue() const noexcept {
    return encoded_ != kEmpty;
  }

  [[nodiscard]]
  int operator*() const noexcept {
    assert(HasValue());
    return static_cast<int>(encoded_);
  }

  // There is no stored error object: CQE failures are the negative integer
  // itself and are converted by the awaiter. Calling Error() for an empty
  // result is only a defensive fallback for the invalid pre-completion path.
  [[nodiscard]]
  base::Error Error() const noexcept {
    assert(!HasValue());
    return base::MakeErrno(EIO);
  }

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

  [[nodiscard]]
  bool Complete(int cqe_res) noexcept {
    if (!completion_gate_.TryComplete()) {
      return false;
    }

    result = cqe_res;
    return true;
  }

  // Some operation families have more than one CQE and keep their primary
  // result outside LUringOp. They mark the operation terminal only after the
  // final CQE has been interpreted by the family handler.
  bool CompleteWithoutResult() noexcept {
    if (!completion_gate_.TryComplete()) {
      return false;
    }
    return true;
  }

  void SetImmediateSuccess() noexcept { result = 0; }

  void SetImmediateError(base::Error error) noexcept {
    assert(error.value() > 0);
    result = -error.value();
  }

  [[nodiscard]]
  LUringOpKind DispatchKind() const noexcept { return kind; }

  [[nodiscard]]
  bool IsCompleted() const noexcept { return completion_gate_.Completed(); }

  void ResetCompletion() noexcept { completion_gate_.Reset(); }

private:
  operation::detail::CompletionGate completion_gate_;
};

static_assert(sizeof(LUringOp) == 24);

}  // namespace coropact::luring
