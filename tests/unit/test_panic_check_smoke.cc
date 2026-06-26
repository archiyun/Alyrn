// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <csignal>
#include <cstdio>
#include <string>
#include <string_view>

#include "vexo/base/check.h"

namespace {

bool Expect(bool condition, std::string_view message) {
  if (condition) {
    return true;
  }
  std::fprintf(stderr, "[FAIL] %.*s\n", static_cast<int>(message.size()), message.data());
  return false;
}

void TriggerCheckFailure() { VEXO_CHECK(false, "intentional check failure"); }

struct ChildResult {
  int status{-1};
  std::string stderr_output;
};

ChildResult RunChild(void (*entry)()) {
  int pipe_fds[2];
  if (::pipe(pipe_fds) != 0) {
    return {};
  }

  const pid_t pid = ::fork();
  if (pid < 0) {
    ::close(pipe_fds[0]);
    ::close(pipe_fds[1]);
    return {};
  }

  if (pid == 0) {
    ::close(pipe_fds[0]);
    if (::dup2(pipe_fds[1], STDERR_FILENO) < 0) {
      ::_exit(127);
    }
    ::close(pipe_fds[1]);
    entry();
    ::_exit(0);
  }

  ::close(pipe_fds[1]);

  ChildResult result;
  char buffer[512];
  for (;;) {
    const ssize_t count = ::read(pipe_fds[0], buffer, sizeof(buffer));
    if (count > 0) {
      result.stderr_output.append(buffer, static_cast<std::size_t>(count));
      continue;
    }
    if (count < 0 && errno == EINTR) {
      continue;
    }
    break;
  }
  ::close(pipe_fds[0]);

  while (::waitpid(pid, &result.status, 0) < 0 && errno == EINTR) {
  }
  return result;
}

bool TestSuccessfulCheckEvaluation() {
  int condition_calls = 0;
  int message_calls = 0;

  auto message = [&]() -> std::string_view {
    ++message_calls;
    return "must not be evaluated";
  };

  VEXO_CHECK(++condition_calls == 1, message());

  return Expect(condition_calls == 1, "VEXO_CHECK must evaluate condition exactly once") &&
         Expect(message_calls == 0, "VEXO_CHECK must not evaluate message on success");
}

bool TestDebugCheckEvaluation() {
  int condition_calls = 0;
  int message_calls = 0;

  auto message = [&]() -> std::string_view {
    ++message_calls;
    return "must not be evaluated";
  };

  VEXO_DCHECK(++condition_calls == 1, message());

#ifndef NDEBUG
  return Expect(condition_calls == 1, "VEXO_DCHECK must evaluate condition in debug builds") &&
         Expect(message_calls == 0, "VEXO_DCHECK must not evaluate message on success");
#else
  return Expect(condition_calls == 0,
                "VEXO_DCHECK must not evaluate condition in release builds") &&
         Expect(message_calls == 0, "VEXO_DCHECK must not evaluate message in release builds");
#endif
}

bool TestFailedCheckDiagnostics() {
  const ChildResult child = RunChild(&TriggerCheckFailure);

  bool ok = true;
  ok &= Expect(WIFSIGNALED(child.status), "failed VEXO_CHECK must terminate by signal");
  if (WIFSIGNALED(child.status)) {
    ok &=
        Expect(WTERMSIG(child.status) == SIGABRT, "failed VEXO_CHECK must terminate with SIGABRT");
  }
  ok &= Expect(child.stderr_output.contains("[VEXO PANIC] intentional check failure"),
               "panic output must contain the failure message");
  ok &= Expect(child.stderr_output.contains("condition: false"),
               "panic output must contain the failed condition");
  ok &= Expect(child.stderr_output.contains("test_panic_check_smoke.cc"),
               "panic output must contain the source file");
  ok &= Expect(child.stderr_output.contains("TriggerCheckFailure"),
               "panic output must contain the function name");
  return ok;
}

}  // namespace

int main() {
  bool ok = true;
  ok &= TestSuccessfulCheckEvaluation();
  ok &= TestDebugCheckEvaluation();
  ok &= TestFailedCheckDiagnostics();

  if (ok) {
    std::fprintf(stdout, "panic/check smoke: PASS\n");
    return 0;
  }
  return 1;
}
