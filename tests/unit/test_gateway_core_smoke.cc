// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <iostream>
#include <memory>
#include <string>

#include "coropact/gateway/gateway_core.h"
#include "coropact/gateway/upstream_peer.h"

namespace {

bool Expect(bool ok, const char* msg) {
  if (!ok) std::cerr << "[FAIL] " << msg << '\n';
  return ok;
}

coropact::http::HttpRequest MakeRequest(
    std::string_view path, coropact::http::Method method = coropact::http::Method::Get) {
  coropact::http::HttpRequest req;
  req.SetMethod(method);
  req.SetVersion(coropact::http::Version::Http11);
  req.SetPath(path);
  return req;
}

bool TestDirectRoute() {
  coropact::gateway::UpstreamRegistry registry;
  coropact::gateway::GatewayCore core("gw-core", registry);
  core.Get("/hello", [](const coropact::http::HttpRequest&, coropact::http::HttpResponse& resp) {
    resp.SetStatusCode(coropact::http::StatusCode::Ok);
    resp.SetBody("ok");
  });

  auto action = core.HandleRequest(MakeRequest("/hello"), "127.0.0.1");
  if (!Expect(action.kind == coropact::gateway::GatewayActionKind::Send,
              "direct route should produce a send action")) {
    return false;
  }
  if (!Expect(action.response.find("200 OK") != std::string::npos,
              "direct response should be 200")) {
    return false;
  }
  if (!Expect(action.response.find("ok") != std::string::npos,
              "direct response should contain handler body")) {
    return false;
  }

  auto wrong_method = core.HandleRequest(
      MakeRequest("/hello", coropact::http::Method::Post), "127.0.0.1");
  return Expect(wrong_method.response.find("405 Method Not Allowed") != std::string::npos,
                "GET route should reject POST with 405");
}

bool TestProxyDecision() {
  coropact::gateway::UpstreamRegistry registry;
  auto upstream =
      std::make_shared<coropact::gateway::Upstream>(coropact::gateway::UpstreamConfig{.name = "backend"});
  upstream->AddPeer(std::make_shared<coropact::gateway::UpstreamPeer>(
      coropact::gateway::UpstreamPeerConfig{.name = "backend-1", .host = "127.0.0.1", .port = 9001}));
  if (!registry.Register(upstream).has_value()) return false;

  coropact::gateway::GatewayCore core("gw-core", registry);
  core.AddProxyRoute("/api", "backend", "round_robin");

  auto action = core.HandleRequest(
      MakeRequest("/api/users", coropact::http::Method::Post), "203.0.113.7");
  if (!Expect(action.kind == coropact::gateway::GatewayActionKind::Proxy,
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
  coropact::gateway::UpstreamRegistry registry;
  coropact::gateway::GatewayCore core("gw-core", registry);

  auto action = core.HandleParseError(coropact::http::ParseStatus::HeaderTooLarge);
  if (!Expect(action.kind == coropact::gateway::GatewayActionKind::Send,
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
