#include <cerrno>
#include <cstdint>
#include <iostream>
#include <memory>
#include <system_error>

#include "coropact/result.h"
#include "coropact/base/try.h"
#include "coropact/coro/sync_wait.h"
#include "coropact/coro/task.h"

namespace {

using coropact::Error;
using coropact::Result;

Result<int> ReadValue(bool fail) {
  if (fail) return std::unexpected(coropact::Errno(EINVAL));
  return 41;
}

Result<int> AddValues(bool fail) {
  COROPACT_TRY_VALUE(first, ReadValue(false));
  COROPACT_TRY_VALUE(second, ReadValue(fail));
  return first + second;
}

Result<std::unique_ptr<int>> ReadMoveOnlyValue(bool fail) {
  if (fail) return std::unexpected(coropact::Errno(EINVAL));
  return std::make_unique<int>(99);
}

Result<int> TakeMoveOnlyValue(bool fail) {
  COROPACT_TRY_VALUE(value, ReadMoveOnlyValue(fail));
  return *value;
}

Result<void> Validate(bool fail) {
  if (fail) return std::unexpected(coropact::Errno(EINVAL));
  return {};
}

Result<void> PropagateVoid(bool fail) {
  COROPACT_TRY(Validate(fail));
  return {};
}

enum class ParseError : std::uint8_t {
  kInvalidInput,
};

Result<int, ParseError> ParseNumber(bool fail) {
  if (fail) return std::unexpected(ParseError::kInvalidInput);
  return 41;
}

Result<int, ParseError> IncrementParsedNumber(bool fail) {
  COROPACT_TRY_VALUE(value, ParseNumber(fail));
  return value + 1;
}

coropact::coro::Task<Result<int>> ReadValueAsync(bool fail) {
  if (fail) {
    co_return std::unexpected(coropact::Errno(EINVAL));
  }
  co_return 1;
}

coropact::coro::Task<Result<int>> AddValuesAsync(bool fail) {
  COROPACT_CO_TRY(first, co_await ReadValueAsync(false));
  COROPACT_CO_TRY(second, co_await ReadValueAsync(fail));
  co_return first + second;
}

bool TestTryReturnsValue() {
  const auto result = AddValues(false);
  return result.has_value() && *result == 82;
}

bool TestTryPropagatesError() {
  const auto result = AddValues(true);
  return !result.has_value() && result.error() == Error(EINVAL, std::system_category());
}

bool TestTryMovesValue() {
  const auto result = TakeMoveOnlyValue(false);
  return result.has_value() && *result == 99;
}

bool TestTryPropagatesVoidError() {
  const auto result = PropagateVoid(true);
  return !result.has_value() && result.error() == Error(EINVAL, std::system_category());
}

bool TestCustomErrorType() {
  const auto success = IncrementParsedNumber(false);
  const auto failure = IncrementParsedNumber(true);
  return success.has_value() && *success == 42 && !failure.has_value() &&
         failure.error() == ParseError::kInvalidInput;
}

bool TestCoTryReturnsValue() {
  const auto result = coropact::coro::SyncWait(AddValuesAsync(false));
  return result.has_value() && *result == 2;
}

bool TestCoTryPropagatesError() {
  const auto result = coropact::coro::SyncWait(AddValuesAsync(true));
  return !result.has_value() && result.error() == Error(EINVAL, std::system_category());
}

}  // namespace

int main() {
  if (!TestTryReturnsValue()) {
    std::cerr << "[FAIL] COROPACT_TRY_VALUE should return the expected value\n";
    return 1;
  }
  if (!TestTryPropagatesError()) {
    std::cerr << "[FAIL] COROPACT_TRY_VALUE should propagate the expected error\n";
    return 1;
  }
  if (!TestTryMovesValue()) {
    std::cerr << "[FAIL] COROPACT_TRY_VALUE should move a successful value\n";
    return 1;
  }
  if (!TestTryPropagatesVoidError()) {
    std::cerr << "[FAIL] COROPACT_TRY should propagate a void expected error\n";
    return 1;
  }
  if (!TestCustomErrorType()) {
    std::cerr << "[FAIL] Result should preserve a custom error type\n";
    return 1;
  }
  if (!TestCoTryReturnsValue()) {
    std::cerr << "[FAIL] COROPACT_CO_TRY should return the expected value\n";
    return 1;
  }
  if (!TestCoTryPropagatesError()) {
    std::cerr << "[FAIL] COROPACT_CO_TRY should propagate the expected error\n";
    return 1;
  }

  std::cout << "[PASS] try_smoke_test\n";
  return 0;
}
