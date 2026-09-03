// SPDX-License-Identifier: MIT
//
// Build:
//   make build
// Run:
//   ./build/debug/examples/simple_echo_container

#include <csignal>
#include <cstdint>
#include <print>
#include <stop_token>
#include <thread>
#include <utility>

#include "alyrn/net.h"
#include "alyrn/epoll.h"
#include "echo_app.h"
#include "signal_stop.h"

using namespace alyrn;

namespace {

constexpr std::uint16_t kPort = 9090;

}  // namespace

int main() {
  std::signal(SIGPIPE, SIG_IGN);

  std::stop_source stop_source;
  auto blocked_signals = simple_echo::BlockTerminationSignals();
  if (!blocked_signals.HasValue()) {
    std::println(stderr, "failed to block termination signals: {}",
                 blocked_signals.Error().message());
    return 1;
  }
  std::jthread signal_forwarder{simple_echo::ForwardTerminationSignals, &stop_source};

  auto runtime = Runtime::Create<runtime::Epoll>(
      net::Endpoint::Any(kPort, net::Endpoint::Family::kIPv4),
      [](auto stream) { return simple_echo::HandleConnection(std::move(stream)); });

  std::println("Alyrn container echo server listening on 0.0.0.0:{}", kPort);
  auto ran = runtime.Run(stop_source.get_token());
  (void)stop_source.request_stop();
  if (!ran.HasValue()) {
    std::println(stderr, "failed to run Epoll runtime: {}", ran.Error().message());
    return 1;
  }
  return 0;
}
