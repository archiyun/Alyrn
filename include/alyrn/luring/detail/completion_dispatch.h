// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>

#include "alyrn/luring/detail/op.h"

namespace alyrn::luring::detail {

void DispatchAcceptComplete(Op* op) noexcept;
void DispatchConnectComplete(Op* op) noexcept;
CompletionDisposition DispatchAcceptSourceComplete(Op* op, CompletionEvent event) noexcept;
void DispatchAcceptSourceCancelComplete(Op* op) noexcept;
CompletionDisposition DispatchRecvSourceComplete(Op* op, CompletionEvent event) noexcept;
void DispatchRecvSourceCancelComplete(Op* op) noexcept;
CompletionDisposition DispatchSendZeroCopyComplete(Op* op, CompletionEvent event) noexcept;
void DispatchListenerCloseComplete(Op* op) noexcept;
void DispatchStreamReadComplete(Op* op) noexcept;
void DispatchStreamReadIntoComplete(Op* op) noexcept;
void DispatchTimedReadComplete(Op* op) noexcept;
void DispatchTimedReadTimeoutComplete(Op* op) noexcept;
void DispatchStreamWriteComplete(Op* op) noexcept;
void DispatchStreamCloseComplete(Op* op) noexcept;
void DispatchTimerDriverComplete(Op* op) noexcept;
void DispatchTimerControlComplete(Op* op) noexcept;

CompletionDisposition DispatchCompletion(Op* op, CompletionEvent event) noexcept;

}  // namespace alyrn::luring::detail
