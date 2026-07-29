// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <cstdio>

#include "coropact/operation/detail/completion_gate.h"

namespace {

bool Expect(bool condition, const char* message) {
  if (condition) {
    return true;
  }
  std::fprintf(stderr, "FAIL: %s\n", message);
  return false;
}

bool TestOneShotTransition() {
  coropact::operation::detail::CompletionGate gate;

  bool ok = true;
  ok &= Expect(!gate.Completed(), "a new completion gate must be open");
  ok &= Expect(gate.TryComplete(), "the first completion must win");
  ok &= Expect(gate.Completed(), "a winning completion must become terminal");
  ok &= Expect(!gate.TryComplete(), "a duplicate completion must be rejected");
  return ok;
}

bool TestResetForReusablePhysicalSlot() {
  coropact::operation::detail::CompletionGate gate;
  static_cast<void>(gate.TryComplete());
  gate.Reset();

  return Expect(!gate.Completed(), "reset must reopen a reusable physical slot") &&
         Expect(gate.TryComplete(), "a reopened slot must accept its next completion");
}

}  // namespace

int main() {
  const bool ok = TestOneShotTransition() && TestResetForReusablePhysicalSlot();
  if (ok) {
    std::puts("completion gate smoke: PASS");
    return 0;
  }
  return 1;
}
