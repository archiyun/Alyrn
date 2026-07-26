// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include "coropact/luring/op.h"

namespace coropact::luring::detail {

void DispatchAcceptComplete(LUringOp* op) noexcept;
void DispatchListenerCloseComplete(LUringOp* op) noexcept;
void DispatchStreamReadComplete(LUringOp* op) noexcept;
void DispatchTimedReadComplete(LUringOp* op) noexcept;
void DispatchTimedReadTimeoutComplete(LUringOp* op) noexcept;
void DispatchStreamWriteComplete(LUringOp* op) noexcept;
void DispatchStreamWritePartsComplete(LUringOp* op) noexcept;
void DispatchStreamCloseComplete(LUringOp* op) noexcept;
void DispatchTimerDriverComplete(LUringOp* op) noexcept;
void DispatchTimerControlComplete(LUringOp* op) noexcept;

void DispatchCompletion(LUringOp* op) noexcept;

}  // namespace coropact::luring::detail
