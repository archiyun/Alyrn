// SPDX-License-Identifier: MIT
// Result composition and the ALYRN_TRY family of early-return macros.

#include <iostream>
#include <memory>
#include <string>
#include <system_error>
#include <utility>

#include "alyrn/coro/sync_wait.h"
#include "alyrn/result.h"
#include "alyrn/task.h"

namespace {

using alyrn::Error;
using alyrn::Result;

bool Check(bool condition, const char* message) {
  if (!condition) std::cerr << "FAIL: " << message << '\n';
  return condition;
}

const Error kBusy = std::make_error_code(std::errc::device_or_resource_busy);

Result<int> Parse(bool ok) {
  if (!ok) {
    return std::unexpected(kBusy);
  }
  return 21;
}

Result<void> Touch(bool ok) {
  if (!ok) {
    return std::unexpected(kBusy);
  }
  return {};
}

Result<std::unique_ptr<int>> Make(bool ok) {
  if (!ok) {
    return std::unexpected(kBusy);
  }
  return std::make_unique<int>(7);
}

Result<int> Doubled(bool parse_ok, bool touch_ok) {
  ALYRN_TRY(Touch(touch_ok));
  ALYRN_TRY_ASSIGN(auto value, Parse(parse_ok));
  return value * 2;
}

Result<int> Owned(bool ok) {
  ALYRN_TRY_ASSIGN(std::unique_ptr<int> owned, Make(ok));
  return *owned;
}

alyrn::Task<Result<int>> ParseTask(bool ok) { co_return Parse(ok); }
alyrn::Task<Result<void>> TouchTask(bool ok) { co_return Touch(ok); }

alyrn::Task<Result<int>> CoDoubled(bool parse_ok, bool touch_ok) {
  ALYRN_CO_TRY(co_await TouchTask(touch_ok));
  ALYRN_CO_TRY_ASSIGN(auto value, co_await ParseTask(parse_ok));
  co_return value * 2;
}

bool MacrosPropagate() {
  auto ok = Doubled(true, true);
  auto parse_failed = Doubled(false, true);
  auto touch_failed = Doubled(true, false);
  auto owned = Owned(true);
  auto owned_failed = Owned(false);
  return Check(ok.HasValue() && *ok == 42, "ALYRN_TRY_ASSIGN must yield the value") &&
         Check(!parse_failed.HasValue() && parse_failed.Error() == kBusy,
               "ALYRN_TRY_ASSIGN must return the error") &&
         Check(!touch_failed.HasValue() && touch_failed.Error() == kBusy,
               "ALYRN_TRY must return the error") &&
         Check(owned.HasValue() && *owned == 7, "ALYRN_TRY_ASSIGN must move a move-only value") &&
         Check(!owned_failed.HasValue() && owned_failed.Error() == kBusy,
               "ALYRN_TRY_ASSIGN must return a move-only result's error");
}

bool CoroutineMacrosPropagate() {
  auto ok = alyrn::coro::SyncWait(CoDoubled(true, true));
  auto parse_failed = alyrn::coro::SyncWait(CoDoubled(false, true));
  auto touch_failed = alyrn::coro::SyncWait(CoDoubled(true, false));
  return Check(ok.HasValue() && *ok == 42, "ALYRN_CO_TRY_ASSIGN must yield the value") &&
         Check(!parse_failed.HasValue() && parse_failed.Error() == kBusy,
               "ALYRN_CO_TRY_ASSIGN must co_return the error") &&
         Check(!touch_failed.HasValue() && touch_failed.Error() == kBusy,
               "ALYRN_CO_TRY must co_return the error");
}

bool ValueComposition() {
  auto to_text = [](int value) -> Result<std::string> { return std::to_string(value); };
  auto and_then = Parse(true).AndThen(to_text);
  auto and_then_error = Parse(false).AndThen(to_text);
  auto transform = Parse(true).Transform([](int value) { return value + 1; });
  auto transform_void = Parse(true).Transform([](int) {});
  auto recovered = Parse(false).OrElse([](const Error&) -> Result<int> { return 0; });
  auto kept = Parse(true).OrElse([](const Error&) -> Result<int> { return 0; });
  auto mapped = Parse(false).TransformError([](const Error& error) { return error.value(); });

  const Result<int> constant = 5;
  auto from_const = constant.Transform([](const int& value) { return value * 3; });
  Result<int> mutable_result = 4;
  auto from_lvalue = mutable_result.AndThen([](int& value) -> Result<int> { return ++value; });

  auto owned = Make(true);
  auto moved = std::move(owned).Transform([](std::unique_ptr<int> pointer) { return *pointer; });

  return Check(and_then.HasValue() && *and_then == "21", "AndThen must chain a value") &&
         Check(!and_then_error.HasValue() && and_then_error.Error() == kBusy,
               "AndThen must pass an error through") &&
         Check(transform.HasValue() && *transform == 22, "Transform must map a value") &&
         Check(transform_void.HasValue(), "Transform to void must keep success") &&
         Check(recovered.HasValue() && *recovered == 0, "OrElse must recover an error") &&
         Check(kept.HasValue() && *kept == 21, "OrElse must pass a value through") &&
         Check(!mapped.HasValue() && mapped.Error() == kBusy.value(),
               "TransformError must map the error type") &&
         Check(from_const.HasValue() && *from_const == 15, "Transform must accept a const Result") &&
         Check(from_lvalue.HasValue() && *from_lvalue == 5 && *mutable_result == 5,
               "AndThen on an lvalue must pass the value by reference") &&
         Check(moved.HasValue() && *moved == 7, "Transform on an rvalue must move the value");
}

bool VoidComposition() {
  auto chained = Touch(true).AndThen([] { return Parse(true); });
  auto chained_error = Touch(false).AndThen([] { return Parse(true); });
  auto transformed = Touch(true).Transform([] { return 3; });
  auto transform_error = Touch(false).Transform([] { return 3; });
  auto recovered = Touch(false).OrElse([](const Error&) -> Result<void> { return {}; });
  auto mapped = Touch(false).TransformError([](Error error) { return error.value(); });

  return Check(chained.HasValue() && *chained == 21, "void AndThen must chain") &&
         Check(!chained_error.HasValue() && chained_error.Error() == kBusy,
               "void AndThen must pass an error through") &&
         Check(transformed.HasValue() && *transformed == 3, "void Transform must produce a value") &&
         Check(!transform_error.HasValue(), "void Transform must pass an error through") &&
         Check(recovered.HasValue(), "void OrElse must recover") &&
         Check(!mapped.HasValue() && mapped.Error() == kBusy.value(),
               "void TransformError must map the error type");
}

}  // namespace

int main() {
  bool ok = MacrosPropagate();
  ok = CoroutineMacrosPropagate() && ok;
  ok = ValueComposition() && ok;
  ok = VoidComposition() && ok;
  if (!ok) {
    return 1;
  }
  std::cout << "result smoke: PASS\n";
  return 0;
}
