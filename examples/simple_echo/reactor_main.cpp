// SPDX-License-Identifier: MIT
//
// Build:
//   cmake --build build --target simple_echo -j
//
// Run:
//   ./build/examples/simple_echo
//
// Try:
//   nc 127.0.0.1 9090

#include <csignal>
#include <cstdint>
#include <print>
#include <stop_token>
#include <thread>
#include <utility>

#include "alyrn/net.h"
#include "alyrn/reactor.h"
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
  if (!blocked_signals.has_value()) {
    std::println(stderr, "failed to block termination signals: {}",
                 blocked_signals.error().message());
    return 1;
  }
  std::jthread signal_forwarder{simple_echo::ForwardTerminationSignals, &stop_source};

  auto runtime = Runtime::Create<runtime::Reactor>(
      net::Endpoint::Loopback(kPort),
      [](auto stream) { return simple_echo::HandleConnection(std::move(stream)); });

  std::println("simple echo (Reactor) listening on 127.0.0.1:{}", kPort);
  auto ran = runtime.Run(stop_source.get_token());
  (void)stop_source.request_stop();
  if (!ran.has_value()) {
    std::println(stderr, "failed to run Reactor runtime: {}", ran.error().message());
    return 1;
  }
  return 0;
}
