// SPDX-License-Identifier: MIT
//
// Uring RecvSource echo: multishot accept plus multishot receive into kernel
// provided buffers.
//
// AcceptSource yields accepted streams from one multishot accept request.
// RecvSource yields BufferLease values that point into the Loop's provided
// buffer ring. Releasing (or destroying) a lease returns its buffer to the
// ring, and RecvSource::Stop() waits until every lease is back.
//
// Build:
//   make uring
//
// Run:
//   ./build/uring/debug/examples/uring/demo_luring_recv_source_echo
//
// Try:
//   nc 127.0.0.1 19097

#include <pthread.h>
#include <signal.h>

#include <csignal>
#include <cstdint>
#include <ctime>
#include <print>
#include <stop_token>
#include <thread>
#include <utility>

#include "alyrn/alyrn.h"
#include "alyrn/net.h"
#include "alyrn/uring.h"

using namespace alyrn;

namespace {

constexpr std::uint16_t kPort = 19097;
constexpr std::size_t kBufferSize = 4096;

DetachedTask EchoSession(uring::Stream stream) {
  uring::RecvSourceOptions recv_options;
  recv_options.buffer_size = kBufferSize;
  auto source = uring::RecvSource::Create(stream.OwnerLoop(), stream.Fd(), recv_options);
  if (!source.HasValue()) {
    std::println(stderr, "RecvSource::Create failed: {}", source.Error().message());
    (void)co_await stream.Close();
    co_return;
  }

  bool writable = true;
  for (;;) {
    auto next = co_await source->Next();
    if (!next.HasValue()) {
      // ECANCELED when the loop stops.
      std::println(stderr, "receive failed: {}", next.Error().message());
      break;
    }
    if (!next->has_value()) {
      break;  // the peer closed its side, or RequestStop() below took effect
    }

    // The lease views a kernel-provided buffer, so the echo writes straight
    // from it. The buffer returns to the ring once the lease is released.
    net::BufferLease lease = std::move((*next)->buffer);
    if (writable && !(co_await stream.Write(lease.Bytes())).HasValue()) {
      // Stop receiving, but keep taking the events already produced: Stop()
      // below completes only once every queued event has been consumed.
      writable = false;
      (void)source->RequestStop();
    }
  }

  // Stop() cancels the multishot request and waits for outstanding leases;
  // the source borrows the stream's descriptor, so it stops before Close().
  if (auto stopped = co_await source->Stop(); !stopped.HasValue()) {
    std::println(stderr, "RecvSource::Stop failed: {}", stopped.Error().message());
  }
  (void)co_await stream.Close();
}

DetachedTask AcceptLoop(uring::Loop& loop, uring::AcceptSource& accepts) {
  for (;;) {
    auto next = co_await accepts.Next();
    if (!next.HasValue() || !next->has_value()) {
      co_return;  // the loop is stopping, or the source stopped
    }
    SpawnDetach(loop, EchoSession(std::move(**next)));
  }
}

// Forwards SIGINT/SIGTERM to stop. Both signals are blocked in every thread;
// the timeout lets this jthread finish when main returns first.
void ForwardTerminationSignals(std::stop_token thread_stop, std::stop_source* stop) {
  sigset_t signals;
  (void)::sigemptyset(&signals);
  (void)::sigaddset(&signals, SIGINT);
  (void)::sigaddset(&signals, SIGTERM);
  constexpr timespec kPoll{0, 100'000'000};
  while (!thread_stop.stop_requested()) {
    const int signal = ::sigtimedwait(&signals, nullptr, &kPoll);
    if (signal == SIGINT || signal == SIGTERM) {
      (void)stop->request_stop();
      return;
    }
  }
}

}  // namespace

int main() {
  std::signal(SIGPIPE, SIG_IGN);

  sigset_t signals;
  (void)::sigemptyset(&signals);
  (void)::sigaddset(&signals, SIGINT);
  (void)::sigaddset(&signals, SIGTERM);
  (void)::pthread_sigmask(SIG_BLOCK, &signals, nullptr);
  std::stop_source stop;
  std::jthread signal_forwarder(ForwardTerminationSignals, &stop);

  uring::Loop loop;
  uring::Options options;
  options.entries = 1024;
  // The provided-buffer ring shared by every RecvSource on this loop.
  options.shared_buffer_capacity = 256;
  options.shared_buffer_size = kBufferSize;
  if (auto initialized = loop.Init(options); !initialized.HasValue()) {
    std::println(stderr, "loop init failed: {}", initialized.Error().message());
    return 1;
  }

  auto listener = uring::Listener::Create(&loop, net::Endpoint::Loopback(kPort));
  if (!listener.HasValue()) {
    std::println(stderr, "listen failed: {}", listener.Error().message());
    return 1;
  }
  auto accepts = listener->CreateAcceptSource();
  if (!accepts.HasValue()) {
    std::println(stderr, "AcceptSource creation failed: {}", accepts.Error().message());
    return 1;
  }
  SpawnDetach(loop, AcceptLoop(loop, *accepts));

  std::println("uring RecvSource echo listening on 127.0.0.1:{}", kPort);
  loop.Run(stop.get_token());
  return 0;
}
