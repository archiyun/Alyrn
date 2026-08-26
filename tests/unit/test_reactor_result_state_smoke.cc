// SPDX-License-Identifier: MIT

#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <csignal>
#include <cstdio>
#include <expected>
#include <utility>

#include "coropact/backend/detail/value_result_state.h"
#include "coropact/result.h"
#include "coropact/reactor/detail/result_state.h"

namespace {

bool Expect(bool condition, const char* message) {
  if (condition) {
    return true;
  }
  std::fprintf(stderr, "FAIL: %s\n", message);
  return false;
}

bool ExpectChildAbort(void (*entry)(), const char* message) {
  const pid_t child = ::fork();
  if (child < 0) {
    return Expect(false, "fork failed for result-state invariant test");
  }
  if (child == 0) {
    (void)::freopen("/dev/null", "w", stderr);
    entry();
    ::_exit(0);
  }

  int status = 0;
  while (::waitpid(child, &status, 0) < 0 && errno == EINTR) {
  }
  return Expect(WIFSIGNALED(status), message) &&
         Expect(WTERMSIG(status) == SIGABRT, "result-state invariant must terminate with SIGABRT");
}

void TakePendingIoState() {
  coropact::reactor::detail::IoResultState state;
  (void)state.Take();
}

void SetIoStateTwice() {
  coropact::reactor::detail::IoResultState state;
  state.SetSuccess(1);
  state.SetSuccess(2);
}

bool TestIoStateTakeConsumesResult() {
  coropact::reactor::detail::IoResultState state;

  state.SetSuccess(42);
  auto success = state.Take();
  bool ok = Expect(success.has_value(), "packed success must decode") &&
            Expect(*success == 42, "packed success must preserve byte count") &&
            Expect(!state.HasResult(), "packed Take must restore pending state");

  state.SetError(coropact::Errno(EPIPE));
  auto error = state.Take();
  ok &= Expect(!error.has_value(), "packed errno must decode as an error") &&
        Expect(error.error().value() == EPIPE, "packed errno must preserve its value") &&
        Expect(error.error().category() == std::system_category(),
               "packed errno must preserve the system category") &&
        Expect(!state.HasResult(), "packed error Take must restore pending state");
  return ok;
}

struct LifetimeProbe {
  explicit LifetimeProbe(int value) noexcept : value(value) { ++live_count; }

  LifetimeProbe(const LifetimeProbe&) = delete;
  LifetimeProbe& operator=(const LifetimeProbe&) = delete;

  LifetimeProbe(LifetimeProbe&& other) noexcept : value(other.value) { ++live_count; }
  LifetimeProbe& operator=(LifetimeProbe&&) = delete;

  ~LifetimeProbe() { --live_count; }

  int value;
  static inline int live_count{0};
};

void TakePendingValueState() {
  coropact::backend::detail::ValueResultState<LifetimeProbe> state;
  (void)state.Take();
}

bool TestResultStatesRejectInvalidTransitions() {
  return ExpectChildAbort(&TakePendingIoState,
                          "pending I/O result Take must terminate in Release") &&
         ExpectChildAbort(&SetIoStateTwice,
                          "duplicate I/O result completion must terminate in Release") &&
         ExpectChildAbort(&TakePendingValueState,
                          "pending value result Take must terminate in Release");
}

bool TestValueStateTakeDestroysActiveMember() {
  using State = coropact::backend::detail::ValueResultState<LifetimeProbe>;

  LifetimeProbe::live_count = 0;
  State state;
  {
    coropact::Result<LifetimeProbe> input(std::in_place, 7);
    state.SetResult(std::move(input));
  }

  bool ok = Expect(LifetimeProbe::live_count == 1,
                   "value state must own one probe after source destruction");
  {
    auto result = state.Take();
    ok &= Expect(result.has_value(), "value state must return its stored value") &&
          Expect(result->value == 7, "value state must preserve the stored value") &&
          Expect(!state.HasResult(), "value Take must restore pending state") &&
          Expect(LifetimeProbe::live_count == 1,
                 "value Take must destroy storage after moving its value out");
  }

  return ok && Expect(LifetimeProbe::live_count == 0,
                      "returned value destruction must leave no live probes");
}

bool TestValueStateErrorCanBeReused() {
  coropact::backend::detail::ValueResultState<LifetimeProbe> state;

  state.SetError(coropact::Errno(ECANCELED));
  auto error = state.Take();
  bool ok = Expect(!error.has_value(), "value state error must decode") &&
            Expect(error.error().value() == ECANCELED, "value state error must preserve errno") &&
            Expect(!state.HasResult(), "value error Take must restore pending state");

  coropact::Result<LifetimeProbe> next(std::in_place, 9);
  state.SetResult(std::move(next));
  auto value = state.Take();
  return ok && Expect(value.has_value(), "value state must be reusable after error Take") &&
         Expect(value->value == 9, "reused value state must preserve its value");
}

}  // namespace

int main() {
  const bool ok = TestIoStateTakeConsumesResult() && TestValueStateTakeDestroysActiveMember() &&
                  TestValueStateErrorCanBeReused() && TestResultStatesRejectInvalidTransitions();
  if (ok) {
    std::puts("reactor result state smoke: PASS");
    return 0;
  }
  return 1;
}
