// SPDX-License-Identifier: MIT

#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <csignal>
#include <cstdio>
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

bool ExpectChildAbort(void (*entry)(), const char* message) {
  const pid_t child = ::fork();
  if (child < 0) {
    return Check(false, "fork failed for stream lifecycle invariant test");
  }
  if (child == 0) {
    (void)::freopen("/dev/null", "w", stderr);
    entry();
    ::_exit(0);
  }

  int status = 0;
  while (::waitpid(child, &status, 0) < 0 && errno == EINTR) {
  }
  return Check(WIFSIGNALED(status), message) &&
         Check(WTERMSIG(status) == SIGABRT,
               "stream lifecycle invariant must terminate with SIGABRT");
}

void CommitShutdownWithoutPreparation() {
  StreamLifecycle lifecycle;
  lifecycle.CommitShutdown();
}

void CommitShutdownWhileClosing() {
  StreamLifecycle lifecycle;
  (void)lifecycle.PrepareClose();
  lifecycle.CommitShutdown();
}

void AbortShutdownWithoutPreparation() {
  StreamLifecycle lifecycle;
  lifecycle.AbortShutdownPreparation();
}

void AbortCloseWithoutPreparation() {
  StreamLifecycle lifecycle;
  lifecycle.AbortClosePreparation();
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

bool CheckShutdownPreparationRollback() {
  StreamLifecycle lifecycle;

  auto prepared = lifecycle.PrepareShutdown(false);
  if (!Check(prepared.has_value() && *prepared, "Shutdown preparation did not start")) {
    return false;
  }
  if (!Check(!lifecycle.ValidateWrite().has_value() &&
                 lifecycle.ValidateWrite().error() == std::errc::device_or_resource_busy,
             "Shutdown preparation did not exclude writes")) {
    return false;
  }

  lifecycle.AbortShutdownPreparation();
  prepared = lifecycle.PrepareShutdown(false);
  const bool restored = Check(prepared.has_value() && *prepared,
                              "aborted Shutdown preparation did not restore writable state");
  if (restored) {
    lifecycle.AbortShutdownPreparation();
  }
  return restored;
}

bool CheckCloseRejectsShutdownPreparation() {
  StreamLifecycle lifecycle;
  auto prepared = lifecycle.PrepareShutdown(false);
  if (!Check(prepared.has_value() && *prepared, "Shutdown preparation did not start")) {
    return false;
  }

  auto close = lifecycle.PrepareClose();
  lifecycle.AbortShutdownPreparation();
  return Check(!close.has_value() && close.error() == std::errc::device_or_resource_busy,
               "Close did not reject an in-progress Shutdown preparation");
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

bool CheckInvalidTransitionsFailClosed() {
  return Check(ExpectChildAbort(&CommitShutdownWithoutPreparation,
                                "CommitShutdown without PrepareShutdown must terminate in Release"),
               "missing CommitShutdown preparation was accepted") &&
         Check(
             ExpectChildAbort(&CommitShutdownWhileClosing,
                              "CommitShutdown during Close preparation must terminate in Release"),
             "CommitShutdown during Close preparation was accepted") &&
         Check(ExpectChildAbort(&AbortCloseWithoutPreparation,
                                "AbortClosePreparation outside Closing must terminate in Release"),
               "AbortClosePreparation outside Closing was accepted") &&
         Check(ExpectChildAbort(
                   &AbortShutdownWithoutPreparation,
                   "AbortShutdownPreparation outside preparation must terminate in Release"),
               "AbortShutdownPreparation outside preparation was accepted");
}

}  // namespace

int main() {
  if (!CheckShutdownLifecycle()) return 1;
  if (!CheckShutdownPreparationRollback()) return 1;
  if (!CheckCloseRejectsShutdownPreparation()) return 1;
  if (!CheckCloseLifecycle()) return 1;
  if (!CheckMoveTransfersLifecycle()) return 1;
  if (!CheckInvalidTransitionsFailClosed()) return 1;

  std::cout << "stream lifecycle smoke: PASS\n";
  return 0;
}
