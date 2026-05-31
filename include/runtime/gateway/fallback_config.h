// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <string>

#include "runtime/http/http_response.h"
#include "runtime/http/http_types.h"

namespace runtime::gateway {

// Static fallback response configuration for a proxy route.
//
// When enabled, GatewayServer serves this pre-rendered response instead of the
// generated default error response for unavailable upstreams, open circuit
// breakers, and other proxy failure paths.
struct FallbackConfig {
  bool enabled{false};
  runtime::http::StatusCode status_code{runtime::http::StatusCode::ServiceUnavailable};
  std::string content_type{"application/json; charset=utf-8"};
  std::string body{R"({"error":"service temporarily unavailable"})"};
  // Cached wire-format response used on the hot path after Init().
  std::string pre_rendered;

  // Builds the cached response. Call during route setup, before serving traffic.
  void Init() {
    if (!enabled) return;
    runtime::http::HttpResponse resp(true);
    resp.set_status_code(status_code);
    resp.set_content_type(content_type);
    resp.set_body(body);
    pre_rendered = resp.ToString();
  }
};

}  // namespace runtime::gateway
