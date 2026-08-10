// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <cerrno>
#include <cstdio>
#include <expected>
#include <utility>

#include "coropact/base/error.h"
#include "coropact/reactor/detail/result_state.h"

namespace {

bool Expect(bool condition, const char* message) {
  if (condition) {
    return true;
  }
  std::fprintf(stderr, "FAIL: %s\n", message);
  return false;
}

bool TestIoStateTakeConsumesResult() {
  coropact::reactor::detail::ReactorIoResultState state;

  state.SetSuccess(42);
  auto success = state.Take();
  bool ok = Expect(success.has_value(), "packed success must decode") &&
            Expect(*success == 42, "packed success must preserve byte count") &&
            Expect(!state.HasResult(), "packed Take must restore pending state");

  state.SetError(coropact::base::MakeErrno(EPIPE));
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

bool TestValueStateTakeDestroysActiveMember() {
  using State = coropact::reactor::detail::ReactorValueResultState<LifetimeProbe>;

  LifetimeProbe::live_count = 0;
  State state;
  {
    coropact::base::Result<LifetimeProbe> input(std::in_place, 7);
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
  coropact::reactor::detail::ReactorValueResultState<LifetimeProbe> state;

  state.SetError(coropact::base::MakeErrno(ECANCELED));
  auto error = state.Take();
  bool ok = Expect(!error.has_value(), "value state error must decode") &&
            Expect(error.error().value() == ECANCELED, "value state error must preserve errno") &&
            Expect(!state.HasResult(), "value error Take must restore pending state");

  coropact::base::Result<LifetimeProbe> next(std::in_place, 9);
  state.SetResult(std::move(next));
  auto value = state.Take();
  return ok && Expect(value.has_value(), "value state must be reusable after error Take") &&
         Expect(value->value == 9, "reused value state must preserve its value");
}

}  // namespace

int main() {
  const bool ok = TestIoStateTakeConsumesResult() && TestValueStateTakeDestroysActiveMember() &&
                  TestValueStateErrorCanBeReused();
  if (ok) {
    std::puts("reactor result state smoke: PASS");
    return 0;
  }
  return 1;
}
