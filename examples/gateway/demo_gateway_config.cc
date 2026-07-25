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
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <utility>

#include "coropact/gateway/gateway_config.h"
#include "coropact/gateway/gateway_health_service.h"
#include "coropact/gateway/gateway_session_service.h"
#include "coropact/gateway/upstream_registry.h"
#include "coropact/net/net_utils.h"
#include "coropact/reactor/reactor_connect.h"
#include "coropact/reactor/reactor_worker_group.h"

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

    auto listen_addr = coropact::net::ParseIpAddress(config.server.host, config.server.port);
    if (!listen_addr) {
      throw coropact::gateway::GatewayConfigError("server.listen: expected a numeric IPv4 address");
    }
    using Service =
        coropact::gateway::GatewaySessionService<coropact::reactor::ReactorStream,
                                                 coropact::reactor::ReactorConnector>;
    Service gateway(config.server.name, registry);
    Service::Pool pool;
    coropact::gateway::GatewayHealthService<coropact::reactor::ReactorConnector> health(
        registry, config.health_check.config);

    coropact::gateway::ApplyGatewayConfig(config, gateway);

    coropact::reactor::ReactorWorkerGroupOptions options;
    options.worker_num = 1;
    const bool health_enabled = config.health_check.enabled;
    auto on_worker_init = [&health, health_enabled](coropact::reactor::ReactorWorkerContext& context) {
      if (health_enabled && context.index == 0 &&
          !health.Start(context.scheduler,
                        coropact::reactor::ReactorConnector(&context.loop))) {
        throw std::runtime_error("gateway health service already started");
      }
    };
    coropact::reactor::ReactorWorkerGroup workers(
        *listen_addr, std::move(options), std::move(on_worker_init),
        [&gateway, &pool](coropact::reactor::ReactorWorkerContext& context,
                          coropact::reactor::ReactorStream stream) -> coropact::coro::Task<void> {
          co_await gateway.Serve(std::move(stream),
                                  coropact::reactor::ReactorConnector(&context.loop), pool);
        });

    auto started = workers.Start();
    if (!started.has_value()) {
      throw std::system_error(started.error(), "failed to start reactor workers");
    }

    std::cout << "gateway listening on " << config.server.host << ':' << config.server.port
              << "; press Enter to stop\n";
    std::cin.get();
    health.StopAndJoin();
    workers.Stop();
  } catch (const std::exception& ex) {
    std::cerr << "demo_gateway_config: " << ex.what() << '\n';
    return 1;
  }

  return 0;
}
