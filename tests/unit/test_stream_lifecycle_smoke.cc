// SPDX-License-Identifier: MIT

#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <csignal>
#include <cstdio>
#include <iostream>
#include <system_error>
#include <utility>

#include "alyrn/net/detail/stream_lifecycle.h"

namespace {

using alyrn::net::detail::StreamLifecycle;

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

void CommitCloseReadWithoutPreparation() {
  StreamLifecycle lifecycle;
  lifecycle.CommitCloseRead();
}

void CommitCloseReadWhileClosing() {
  StreamLifecycle lifecycle;
  (void)lifecycle.PrepareClose();
  lifecycle.CommitCloseRead();
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
  if (!Check(!pending.HasValue() && pending.Error() == std::errc::device_or_resource_busy,
             "pending write did not block Shutdown")) {
    return false;
  }

  auto prepared = lifecycle.PrepareShutdown(false);
  if (!Check(prepared.HasValue() && *prepared, "first Shutdown did not require a syscall")) {
    return false;
  }
  lifecycle.CommitShutdown();

  auto repeated = lifecycle.PrepareShutdown(false);
  return Check(lifecycle.ValidateRead().HasValue(), "Shutdown disabled the read direction") &&
         Check(!lifecycle.ValidateWrite().HasValue() &&
                   lifecycle.ValidateWrite().Error() == std::errc::broken_pipe,
               "Shutdown did not reject writes with EPIPE") &&
         Check(repeated.HasValue() && !*repeated, "repeated Shutdown was not idempotent");
}

bool CheckShutdownPreparationRollback() {
  StreamLifecycle lifecycle;

  auto prepared = lifecycle.PrepareShutdown(false);
  if (!Check(prepared.HasValue() && *prepared, "Shutdown preparation did not start")) {
    return false;
  }
  if (!Check(!lifecycle.ValidateWrite().HasValue() &&
                 lifecycle.ValidateWrite().Error() == std::errc::device_or_resource_busy,
             "Shutdown preparation did not exclude writes")) {
    return false;
  }

  lifecycle.AbortShutdownPreparation();
  prepared = lifecycle.PrepareShutdown(false);
  const bool restored = Check(prepared.HasValue() && *prepared,
                              "aborted Shutdown preparation did not restore writable state");
  if (restored) {
    lifecycle.AbortShutdownPreparation();
  }
  return restored;
}

bool CheckCloseReadLifecycle() {
  StreamLifecycle lifecycle;

  auto pending = lifecycle.PrepareCloseRead(true);
  if (!Check(!pending.HasValue() && pending.Error() == std::errc::device_or_resource_busy,
             "pending read did not block CloseRead")) {
    return false;
  }

  auto prepared = lifecycle.PrepareCloseRead(false);
  if (!Check(prepared.HasValue() && *prepared,
             "first CloseRead did not require a syscall")) {
    return false;
  }
  lifecycle.CommitCloseRead();

  auto repeated = lifecycle.PrepareCloseRead(false);
  return Check(lifecycle.IsReadShutdown(), "CloseRead did not mark the read side shut down") &&
         Check(lifecycle.ValidateRead().HasValue(), "CloseRead closed the resource") &&
         Check(lifecycle.ValidateWrite().HasValue(), "CloseRead disabled the write side") &&
         Check(repeated.HasValue() && !*repeated, "repeated CloseRead was not idempotent");
}

bool CheckCloseReadPreparationRollback() {
  StreamLifecycle lifecycle;

  auto prepared = lifecycle.PrepareCloseRead(false);
  if (!Check(prepared.HasValue() && *prepared, "CloseRead preparation did not start")) {
    return false;
  }
  auto duplicate = lifecycle.PrepareCloseRead(false);
  if (!Check(!duplicate.HasValue() &&
                 duplicate.Error() == std::errc::device_or_resource_busy,
             "CloseRead preparation did not exclude a duplicate CloseRead")) {
    return false;
  }

  lifecycle.AbortCloseReadPreparation();
  prepared = lifecycle.PrepareCloseRead(false);
  const bool restored = Check(prepared.HasValue() && *prepared,
                              "aborted CloseRead preparation did not restore readable state");
  if (restored) {
    lifecycle.AbortCloseReadPreparation();
  }
  return restored;
}

bool CheckCloseRejectsShutdownPreparation() {
  StreamLifecycle lifecycle;
  auto prepared = lifecycle.PrepareShutdown(false);
  if (!Check(prepared.HasValue() && *prepared, "Shutdown preparation did not start")) {
    return false;
  }

  auto close = lifecycle.PrepareClose();
  lifecycle.AbortShutdownPreparation();
  return Check(!close.HasValue() && close.Error() == std::errc::device_or_resource_busy,
               "Close did not reject an in-progress Shutdown preparation");
}

bool CheckCloseRejectsCloseReadPreparation() {
  StreamLifecycle lifecycle;
  auto prepared = lifecycle.PrepareCloseRead(false);
  if (!Check(prepared.HasValue() && *prepared, "CloseRead preparation did not start")) {
    return false;
  }

  auto close = lifecycle.PrepareClose();
  lifecycle.AbortCloseReadPreparation();
  return Check(!close.HasValue() && close.Error() == std::errc::device_or_resource_busy,
               "Close did not reject an in-progress CloseRead preparation");
}

bool CheckCloseLifecycle() {
  StreamLifecycle lifecycle;
  auto prepared = lifecycle.PrepareClose();
  if (!Check(prepared.HasValue() && *prepared, "Open did not enter close preparation")) {
    return false;
  }

  auto duplicate = lifecycle.PrepareClose();
  if (!Check(!duplicate.HasValue() && duplicate.Error() == std::errc::device_or_resource_busy,
             "duplicate close did not return EBUSY") ||
      !Check(!lifecycle.ValidateRead().HasValue() &&
                 lifecycle.ValidateRead().Error() == std::errc::operation_canceled,
             "Closing did not reject reads with ECANCELED")) {
    return false;
  }

  lifecycle.AbortClosePreparation();
  if (!Check(lifecycle.ValidateRead().HasValue(),
             "close preparation abort did not restore Open")) {
    return false;
  }

  prepared = lifecycle.PrepareClose();
  lifecycle.MarkClosed();
  auto closed_again = lifecycle.PrepareClose();
  return Check(prepared.HasValue() && *prepared, "second close did not enter preparation") &&
         Check(closed_again.HasValue() && !*closed_again, "Closed close was not idempotent") &&
         Check(!lifecycle.ValidateRead().HasValue() &&
                   lifecycle.ValidateRead().Error() == std::errc::bad_file_descriptor,
               "Closed did not reject reads with EBADF");
}

bool CheckMoveTransfersLifecycle() {
  StreamLifecycle source;
  StreamLifecycle destination(std::move(source));
  return Check(destination.ValidateRead().HasValue(), "move did not transfer Open state") &&
         Check(!source.ValidateRead().HasValue() &&
                   source.ValidateRead().Error() == std::errc::bad_file_descriptor,
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
         Check(ExpectChildAbort(
                   &CommitCloseReadWithoutPreparation,
                   "CommitCloseRead without preparation must terminate in Release"),
               "missing CommitCloseRead preparation was accepted") &&
         Check(ExpectChildAbort(
                   &CommitCloseReadWhileClosing,
                   "CommitCloseRead during Close preparation must terminate in Release"),
               "CommitCloseRead during Close preparation was accepted") &&
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
  if (!CheckCloseReadLifecycle()) return 1;
  if (!CheckCloseReadPreparationRollback()) return 1;
  if (!CheckCloseRejectsShutdownPreparation()) return 1;
  if (!CheckCloseRejectsCloseReadPreparation()) return 1;
  if (!CheckCloseLifecycle()) return 1;
  if (!CheckMoveTransfersLifecycle()) return 1;
  if (!CheckInvalidTransitionsFailClosed()) return 1;

  std::cout << "stream lifecycle smoke: PASS\n";
  return 0;
}
