// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>

#include "coropact/luring/op.h"

namespace coropact::luring::detail {

void DispatchAcceptComplete(LUringOp* op) noexcept;
CompletionDisposition DispatchAcceptSourceComplete(
    LUringOp* op,
    CompletionEvent event) noexcept;
void DispatchAcceptSourceCancelComplete(LUringOp* op) noexcept;
CompletionDisposition DispatchRecvSourceComplete(
    LUringOp* op,
    CompletionEvent event) noexcept;
void DispatchRecvSourceCancelComplete(LUringOp* op) noexcept;
CompletionDisposition DispatchSendZeroCopyComplete(
    LUringOp* op,
    CompletionEvent event) noexcept;
void DispatchListenerCloseComplete(LUringOp* op) noexcept;
void DispatchStreamReadComplete(LUringOp* op) noexcept;
void DispatchStreamReadIntoComplete(LUringOp* op) noexcept;
void DispatchTimedReadComplete(LUringOp* op) noexcept;
void DispatchTimedReadTimeoutComplete(LUringOp* op) noexcept;
void DispatchStreamWriteComplete(LUringOp* op) noexcept;
void DispatchStreamCloseComplete(LUringOp* op) noexcept;
void DispatchTimerDriverComplete(LUringOp* op) noexcept;
void DispatchTimerControlComplete(LUringOp* op) noexcept;

CompletionDisposition DispatchCompletion(LUringOp* op, CompletionEvent event) noexcept;

}  // namespace coropact::luring::detail
