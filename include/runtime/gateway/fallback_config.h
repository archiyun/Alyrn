#pragma once

#include "runtime/http/http_response.h"
#include "runtime/http/http_types.h"

#include <string>

namespace runtime::gateway {

struct FallbackConfig {    
  bool enabled{false};
  runtime::http::StatusCode status_code{runtime::http::StatusCode::ServiceUnavailable};
  std::string content_type{"application/json; charset=utf-8"};
  std::string body{R"({"error":"service temporarily unavailable"})"};
  std::string pre_rendered; // 完整的 HTTP 响应字节

  void Init() {
    if (!enabled) return;      
    runtime::http::HttpResponse resp(true);
    resp.SetStatusCode(status_code);
    resp.SetContentType(content_type);
    resp.SetBody(body);
    pre_rendered = resp.ToString();
  }
};

} // namespace runtime::gateway
