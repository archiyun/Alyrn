// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
//
// Config-driven gateway demo.
//
// Run backend demo servers first:
//   启动两个监听 9001 / 9002 的 HTTP 上游服务
//
// Then run the gateway from the repository root:
//   ./build/examples/gateway/demo_gateway_config examples/gateway/gateway.yaml
#include <exception>
#include <iostream>
#include <string_view>
#include <system_error>
#include <utility>

#include "coropact/gateway/gateway_config.h"
#include "coropact/gateway/gateway_session_service.h"
#include "coropact/gateway/upstream_registry.h"
#include "coropact/net/net_utils.h"
#include "coropact/net/reactor_connect.h"
#include "coropact/net/reactor_worker_group.h"

int main(int argc, char** argv) {
  const bool check_only = argc > 1 && std::string_view(argv[1]) == "--check";
  const char* config_path = check_only ? (argc > 2 ? argv[2] : "examples/gateway/gateway.yaml")
                                       : (argc > 1 ? argv[1] : "examples/gateway/gateway.yaml");

  try {
    coropact::gateway::GatewayConfig config = coropact::gateway::LoadGatewayConfigFromYaml(config_path);

    coropact::gateway::UpstreamRegistry registry;
    coropact::gateway::BuildGatewayUpstreamRegistry(config, registry);

    if (check_only) {
      std::cout << "config ok: " << config_path << '\n';
      return 0;
    }

    auto listen_addr = coropact::net::ParseIPv4Address(config.server.host, config.server.port);
    if (!listen_addr) {
      throw coropact::gateway::GatewayConfigError("server.listen: expected a numeric IPv4 address");
    }
    using Service =
        coropact::gateway::GatewaySessionService<coropact::net::ReactorStream,
                                                 coropact::net::ReactorConnector>;
    Service gateway(config.server.name, registry);

    coropact::gateway::ApplyGatewayConfig(config, gateway);

    coropact::net::ReactorWorkerGroupOptions options;
    options.worker_num = 1;
    coropact::net::ReactorWorkerGroup workers(
        *listen_addr, std::move(options), {},
        [&gateway](coropact::net::ReactorWorkerContext& context,
                   coropact::net::ReactorStream stream) -> coropact::coro::Task<void> {
          co_await gateway.Serve(std::move(stream),
                                  coropact::net::ReactorConnector(&context.loop));
        });

    auto started = workers.Start();
    if (!started.has_value()) {
      throw std::system_error(started.error(), "failed to start reactor workers");
    }

    std::cout << "gateway listening on " << config.server.host << ':' << config.server.port
              << "; press Enter to stop\n";
    std::cin.get();
    workers.Stop();
  } catch (const std::exception& ex) {
    std::cerr << "demo_gateway_config: " << ex.what() << '\n';
    return 1;
  }

  return 0;
}
