// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <iostream>
#include <memory>
#include <string>

#include "vexo/gateway/gateway_core.h"
#include "vexo/gateway/upstream_peer.h"

namespace {

bool Expect(bool ok, const char* msg) {
  if (!ok) std::cerr << "[FAIL] " << msg << '\n';
  return ok;
}

vexo::http::HttpRequest MakeRequest(std::string_view path) {
  vexo::http::HttpRequest req;
  req.set_method(vexo::http::Method::Get);
  req.set_version(vexo::http::Version::Http11);
  req.set_path(path);
  return req;
}

bool TestDirectRoute() {
  vexo::gateway::UpstreamRegistry registry;
  vexo::gateway::GatewayCore core("gw-core", registry);
  core.Get("/hello", [](const vexo::http::HttpRequest&, vexo::http::HttpResponse& resp) {
    resp.set_status_code(vexo::http::StatusCode::Ok);
    resp.set_body("ok");
  });

  auto action = core.HandleRequest(MakeRequest("/hello"), "127.0.0.1");
  if (!Expect(action.kind == vexo::gateway::GatewayActionKind::Send,
              "direct route should produce a send action")) {
    return false;
  }
  if (!Expect(action.response.find("200 OK") != std::string::npos,
              "direct response should be 200")) {
    return false;
  }
  return Expect(action.response.find("ok") != std::string::npos,
                "direct response should contain handler body");
}

bool TestProxyDecision() {
  vexo::gateway::UpstreamRegistry registry;
  auto upstream =
      std::make_shared<vexo::gateway::Upstream>(vexo::gateway::UpstreamConfig{.name = "backend"});
  upstream->AddPeer(std::make_shared<vexo::gateway::UpstreamPeer>(
      vexo::gateway::UpstreamPeerConfig{.name = "backend-1", .host = "127.0.0.1", .port = 9001}));
  registry.Add(upstream);

  vexo::gateway::GatewayCore core("gw-core", registry);
  core.AddProxyRoute("/api", "backend", "round_robin");

  auto action = core.HandleRequest(MakeRequest("/api/users"), "203.0.113.7");
  if (!Expect(action.kind == vexo::gateway::GatewayActionKind::Proxy,
              "proxy route should produce a proxy action")) {
    return false;
  }
  if (!Expect(action.proxy.upstream == upstream, "proxy action should bind the upstream")) {
    return false;
  }
  if (!Expect(action.proxy.request_ctx.client_ip == "203.0.113.7",
              "proxy action should preserve client identity")) {
    return false;
  }

  const auto forwarded = core.MakeForwardedContext(action.proxy);
  if (!Expect(forwarded.gateway_name == "gw-core", "forwarded context should name gateway")) {
    return false;
  }
  return Expect(!forwarded.request_id.empty(), "forwarded context should carry a request id");
}

bool TestParseErrorAction() {
  vexo::gateway::UpstreamRegistry registry;
  vexo::gateway::GatewayCore core("gw-core", registry);

  auto action = core.HandleParseError(vexo::http::ParseStatus::HeaderTooLarge);
  if (!Expect(action.kind == vexo::gateway::GatewayActionKind::Send,
              "parse error should produce a send action")) {
    return false;
  }
  if (!Expect(action.close_after_send, "parse error should close the connection")) {
    return false;
  }
  return Expect(action.response.find("431") != std::string::npos,
                "header-too-large should map to 431");
}

}  // namespace

int main() {
  if (!TestDirectRoute()) return 1;
  if (!TestProxyDecision()) return 1;
  if (!TestParseErrorAction()) return 1;
  std::cout << "[gateway_core_smoke] ok\n";
  return 0;
}
