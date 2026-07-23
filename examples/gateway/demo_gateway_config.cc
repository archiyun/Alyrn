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
#include "coropact/gateway/gateway_server.h"
#include "coropact/gateway/upstream_registry.h"
#include "coropact/net/event_loop.h"
#include "coropact/net/event_loop_scheduler.h"
#include "coropact/net/net_utils.h"
#include "coropact/net/reactor_connect.h"
#include "coropact/net/reactor_listener.h"

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

    coropact::net::EventLoop loop;
    coropact::net::EventLoopScheduler scheduler(&loop);
    auto listen_addr = coropact::net::ParseIPv4Address(config.server.host, config.server.port);
    if (!listen_addr) {
      throw coropact::gateway::GatewayConfigError("server.listen: expected a numeric IPv4 address");
    }
    auto listener_result = coropact::net::ReactorListener::Create(&loop, *listen_addr);
    if (!listener_result.has_value()) {
      throw std::system_error(listener_result.error(), "failed to create listener");
    }
    auto listener = std::move(*listener_result);

    auto connector_result = coropact::net::ReactorConnector::Create(&loop);
    if (!connector_result.has_value()) {
      throw std::system_error(connector_result.error(), "failed to create connector");
    }
    auto connector = std::move(*connector_result);
    coropact::gateway::GatewayServer<coropact::net::ReactorListener, coropact::net::ReactorConnector> gateway(
        listener, scheduler, config.server.name, registry, connector);

    coropact::gateway::ApplyGatewayConfig(config, gateway);

    gateway.Start();
    loop.Loop();
  } catch (const std::exception& ex) {
    std::cerr << "demo_gateway_config: " << ex.what() << '\n';
    return 1;
  }

  return 0;
}
