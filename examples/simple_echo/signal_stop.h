// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <pthread.h>
#include <signal.h>

#include <cerrno>
#include <stop_token>

#include "coropact/base/error.h"

namespace simple_echo {

// Blocks SIGINT and SIGTERM in the calling thread. Call this before Runtime
// creates worker threads so they inherit the same signal mask.
[[nodiscard]]
inline coropact::base::Result<void> BlockTerminationSignals() noexcept {
  sigset_t signals;
  (void)::sigemptyset(&signals);
  (void)::sigaddset(&signals, SIGINT);
  (void)::sigaddset(&signals, SIGTERM);

  const int error = ::pthread_sigmask(SIG_BLOCK, &signals, nullptr);
  if (error != 0) {
    return std::unexpected(coropact::base::MakeErrno(error));
  }
  return {};
}

// Runs on an example-owned jthread. sigtimedwait() keeps C++ code out of an
// asynchronous signal handler; a received termination signal requests the
// application's stop source. The timeout lets jthread destruction finish even
// when shutdown originates from another control path.
inline void ForwardTerminationSignals(std::stop_token thread_stop,
                                      std::stop_source* application_stop) noexcept {
  sigset_t signals;
  (void)::sigemptyset(&signals);
  (void)::sigaddset(&signals, SIGINT);
  (void)::sigaddset(&signals, SIGTERM);

  constexpr timespec kPollTimeout{0, 100'000'000};
  while (!thread_stop.stop_requested() && !application_stop->stop_requested()) {
    siginfo_t signal_info{};
    const int signal = ::sigtimedwait(&signals, &signal_info, &kPollTimeout);
    if (signal == SIGINT || signal == SIGTERM) {
      (void)application_stop->request_stop();
      return;
    }
    if (signal < 0 && (errno == EAGAIN || errno == EINTR)) {
      continue;
    }

    // A signal-wait failure must not leave the demonstration server running
    // without a shutdown path.
    (void)application_stop->request_stop();
    return;
  }
}

}  // namespace simple_echo
