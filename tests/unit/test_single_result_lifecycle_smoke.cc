// SPDX-License-Identifier: MIT

#include <cstdio>

#include "alyrn/operation/detail/single_result_lifecycle.h"

namespace {

bool Expect(bool condition, const char* message) {
  if (condition) {
    return true;
  }
  std::fprintf(stderr, "FAIL: %s\n", message);
  return false;
}

bool TestRequiresResultThenReleaseThenContinuation() {
  alyrn::operation::detail::SingleResultLifecycle lifecycle;

  bool ok = true;
  ok &= Expect(!lifecycle.TryAuthorizeRelease(),
               "release must not be authorized before the result is ready");
  ok &= Expect(!lifecycle.TryAuthorizeContinuation(),
               "continuation must not be authorized before release");
  ok &= Expect(lifecycle.TryAuthorizeResult(), "the first result authorization must win");
  ok &= Expect(!lifecycle.TryAuthorizeResult(), "result authorization must be one-shot");
  ok &= Expect(lifecycle.ResultReady(), "result readiness was not recorded");
  ok &= Expect(!lifecycle.ContinuationAuthorized(),
               "result readiness must not authorize the continuation");
  ok &= Expect(lifecycle.TryAuthorizeRelease(), "result readiness must authorize one release");
  ok &= Expect(!lifecycle.TryAuthorizeRelease(), "release authorization must be one-shot");
  ok &= Expect(lifecycle.ReleaseAuthorized(), "release authorization was not recorded");
  ok &= Expect(lifecycle.TryAuthorizeContinuation(),
               "release authorization must authorize one continuation");
  ok &=
      Expect(!lifecycle.TryAuthorizeContinuation(), "continuation authorization must be one-shot");
  ok &= Expect(lifecycle.ContinuationAuthorized(), "continuation authorization was not recorded");
  return ok;
}

}  // namespace

int main() {
  if (!TestRequiresResultThenReleaseThenContinuation()) {
    return 1;
  }
  std::puts("single result lifecycle smoke: PASS");
  return 0;
}
