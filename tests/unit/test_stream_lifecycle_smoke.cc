// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <iostream>
#include <system_error>
#include <utility>

#include "coropact/net/detail/stream_lifecycle.h"

namespace {

using coropact::net::detail::StreamLifecycle;

bool Check(bool condition, const char* message) {
  if (!condition) {
    std::cout << "FAIL: " << message << '\n';
    return false;
  }
  return true;
}

bool CheckShutdownLifecycle() {
  StreamLifecycle lifecycle;

  auto pending = lifecycle.PrepareShutdown(true);
  if (!Check(!pending.has_value() && pending.error() == std::errc::device_or_resource_busy,
             "pending write did not block Shutdown")) {
    return false;
  }

  auto prepared = lifecycle.PrepareShutdown(false);
  if (!Check(prepared.has_value() && *prepared, "first Shutdown did not require a syscall")) {
    return false;
  }
  lifecycle.CommitShutdown();

  auto repeated = lifecycle.PrepareShutdown(false);
  return Check(lifecycle.ValidateRead().has_value(), "Shutdown disabled the read direction") &&
         Check(!lifecycle.ValidateWrite().has_value() &&
                   lifecycle.ValidateWrite().error() == std::errc::broken_pipe,
               "Shutdown did not reject writes with EPIPE") &&
         Check(repeated.has_value() && !*repeated, "repeated Shutdown was not idempotent");
}

bool CheckCloseLifecycle() {
  StreamLifecycle lifecycle;
  auto prepared = lifecycle.PrepareClose();
  if (!Check(prepared.has_value() && *prepared, "Open did not enter close preparation")) {
    return false;
  }

  auto duplicate = lifecycle.PrepareClose();
  if (!Check(!duplicate.has_value() && duplicate.error() == std::errc::device_or_resource_busy,
             "duplicate close did not return EBUSY") ||
      !Check(!lifecycle.ValidateRead().has_value() &&
                 lifecycle.ValidateRead().error() == std::errc::operation_canceled,
             "Closing did not reject reads with ECANCELED")) {
    return false;
  }

  lifecycle.AbortClosePreparation();
  if (!Check(lifecycle.ValidateRead().has_value(),
             "close preparation abort did not restore Open")) {
    return false;
  }

  prepared = lifecycle.PrepareClose();
  lifecycle.MarkClosed();
  auto closed_again = lifecycle.PrepareClose();
  return Check(prepared.has_value() && *prepared, "second close did not enter preparation") &&
         Check(closed_again.has_value() && !*closed_again, "Closed close was not idempotent") &&
         Check(!lifecycle.ValidateRead().has_value() &&
                   lifecycle.ValidateRead().error() == std::errc::bad_file_descriptor,
               "Closed did not reject reads with EBADF");
}

bool CheckMoveTransfersLifecycle() {
  StreamLifecycle source;
  StreamLifecycle destination(std::move(source));
  return Check(destination.ValidateRead().has_value(), "move did not transfer Open state") &&
         Check(!source.ValidateRead().has_value() &&
                   source.ValidateRead().error() == std::errc::bad_file_descriptor,
               "moved-from lifecycle was not Closed");
}

}  // namespace

int main() {
  if (!CheckShutdownLifecycle()) return 1;
  if (!CheckCloseLifecycle()) return 1;
  if (!CheckMoveTransfersLifecycle()) return 1;

  std::cout << "stream lifecycle smoke: PASS\n";
  return 0;
}
