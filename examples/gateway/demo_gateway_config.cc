// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
//
// Config-driven gateway demo.
//
// Run backend demo servers first:
//   PORT=9001 ./build/examples/http/demo_http_server
//   PORT=9002 ./build/examples/http/demo_http_server
//
// Then run the gateway from the repository root:
//   ./build/examples/gateway/demo_gateway_config examples/gateway/gateway.yaml
#include <exception>
#include <iostream>
#include <string_view>

#include "vexo/gateway/gateway_config.h"
#include "vexo/gateway/gateway_server.h"
#include "vexo/gateway/upstream_registry.h"
#include "vexo/net/event_loop.h"

int main(int argc, char** argv) {
  const bool check_only = argc > 1 && std::string_view(argv[1]) == "--check";
  const char* config_path = check_only ? (argc > 2 ? argv[2] : "examples/gateway/gateway.yaml")
                                       : (argc > 1 ? argv[1] : "examples/gateway/gateway.yaml");

  try {
    vexo::gateway::GatewayConfig config = vexo::gateway::LoadGatewayConfigFromYaml(config_path);

    vexo::gateway::UpstreamRegistry registry;
    vexo::gateway::BuildGatewayUpstreamRegistry(config, registry);

    if (check_only) {
      std::cout << "config ok: " << config_path << '\n';
      return 0;
    }

    vexo::net::EventLoop loop;
    vexo::gateway::GatewayServer gateway(&loop, vexo::gateway::MakeGatewayListenAddress(config),
                                         config.server.name, registry);

    vexo::gateway::ApplyGatewayConfig(config, gateway);

    gateway.Start();
    loop.Loop();
  } catch (const std::exception& ex) {
    std::cerr << "demo_gateway_config: " << ex.what() << '\n';
    return 1;
  }

  return 0;
}
