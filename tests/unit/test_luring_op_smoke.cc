// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <cstdio>

#include "coropact/luring/op.h"

namespace {

bool Expect(bool condition, const char* message) {
  if (condition) {
    return true;
  }
  std::fprintf(stderr, "FAIL: %s\n", message);
  return false;
}

bool TestSingleResultCompletion() {
  coropact::luring::LUringOp op;
  op.kind = coropact::luring::LUringOpKind::kReadComplete;

  bool ok = true;
  ok &= Expect(op.Complete(17), "the first CQE must complete the operation");
  ok &= Expect(op.IsCompleted(), "the operation must become terminal after its first CQE");
  ok &= Expect(*op.result == 17, "the winning CQE result must be retained");
  ok &= Expect(!op.Complete(-5), "a duplicate CQE must not overwrite the result");
  ok &= Expect(*op.result == 17, "a duplicate CQE must preserve the original result");
  ok &= Expect(op.DispatchKind() == coropact::luring::LUringOpKind::kReadComplete,
               "completion state must not alter dispatch kind");
  return ok;
}

bool TestReusablePhysicalSlot() {
  coropact::luring::LUringOp op;
  op.kind = coropact::luring::LUringOpKind::kWake;
  static_cast<void>(op.Complete(0));
  op.ResetCompletion();
  op.result = {};

  return Expect(!op.IsCompleted(), "reset must reopen a reusable operation slot") &&
         Expect(op.Complete(0), "a reopened operation slot must accept a CQE") &&
         Expect(op.DispatchKind() == coropact::luring::LUringOpKind::kWake,
                "reset must preserve dispatch kind");
}

}  // namespace

int main() {
  const bool ok = TestSingleResultCompletion() && TestReusablePhysicalSlot();
  if (ok) {
    std::puts("luring op smoke: PASS");
    return 0;
  }
  return 1;
}
