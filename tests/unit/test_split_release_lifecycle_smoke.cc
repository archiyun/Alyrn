// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <cstdio>

#include "coropact/operation/detail/composite_lifecycle.h"
#include "coropact/operation/detail/split_release_lifecycle.h"

namespace {

bool Expect(bool condition, const char* message) {
  if (condition) {
    return true;
  }
  std::fprintf(stderr, "FAIL: %s\n", message);
  return false;
}

bool TestPrimaryThenRelease() {
  coropact::operation::detail::SplitReleaseLifecycle lifecycle;

  bool ok = true;
  ok &= Expect(lifecycle.RecordLogicalResult(), "the first logical result must win");
  ok &= Expect(!lifecycle.TryAuthorizeRelease(),
               "logical result alone must not authorize resource release");
  ok &= Expect(lifecycle.MarkPhysicalTerminal(), "the first physical terminal event must win");
  ok &= Expect(lifecycle.TryAuthorizeRelease(),
               "logical result plus physical terminal must authorize release");
  ok &= Expect(!lifecycle.TryAuthorizeRelease(), "release authorization must be one-shot");
  ok &= Expect(lifecycle.TryAuthorizeContinuation(),
               "release authorization must permit one continuation resume");
  ok &=
      Expect(!lifecycle.TryAuthorizeContinuation(), "continuation authorization must be one-shot");
  return ok;
}

bool TestPhysicalTerminalBeforeLogicalResult() {
  coropact::operation::detail::SplitReleaseLifecycle lifecycle;

  return Expect(lifecycle.MarkPhysicalTerminal(), "physical terminal event must be recorded") &&
         Expect(!lifecycle.TryAuthorizeRelease(),
                "physical terminal alone must not authorize release") &&
         Expect(lifecycle.RecordLogicalResult(), "a late logical result must be accepted") &&
         Expect(lifecycle.TryAuthorizeRelease(),
                "late logical result must authorize the pending release") &&
         Expect(lifecycle.TryAuthorizeContinuation(),
                "late logical result must authorize the pending continuation");
}

bool TestCompositeMembersAuthorizeOneContinuation() {
  using coropact::operation::detail::CompositeLifecycle;
  using coropact::operation::detail::CompositeMember;

  CompositeLifecycle lifecycle;
  bool ok = true;
  ok &= Expect(lifecycle.RecordMemberCompletion(CompositeMember::kFirst),
               "the first member must complete once");
  ok &= Expect(!lifecycle.RecordMemberCompletion(CompositeMember::kFirst),
               "a duplicate first member completion must be rejected");
  ok &= Expect(!lifecycle.TryAuthorizeLogicalResult(),
               "one composite member must not determine a logical result");
  ok &= Expect(lifecycle.RecordMemberCompletion(CompositeMember::kSecond),
               "the second member must complete once");
  ok &= Expect(lifecycle.TryAuthorizeLogicalResult(),
               "both composite members must authorize one logical result");
  ok &= Expect(!lifecycle.TryAuthorizeLogicalResult(),
               "composite logical authorization must be one-shot");
  ok &= Expect(lifecycle.TryAuthorizeContinuation(),
               "logical result must authorize one continuation");
  ok &= Expect(!lifecycle.TryAuthorizeContinuation(),
               "composite continuation authorization must be one-shot");
  return ok;
}

}  // namespace

int main() {
  const bool ok = TestPrimaryThenRelease() && TestPhysicalTerminalBeforeLogicalResult() &&
                  TestCompositeMembersAuthorizeOneContinuation();
  if (ok) {
    std::puts("split release lifecycle smoke: PASS");
    return 0;
  }
  return 1;
}
