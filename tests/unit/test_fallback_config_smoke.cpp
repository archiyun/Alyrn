// FallbackConfig 降级配置冒烟测试
//
// 编译
// cmake --build build-tests --target fallback_config_smoke_test -j$(nproc)
// 运行
// ./build-tests/tests/fallback_config_smoke_test

#include "runtime/gateway/fallback_config.h"
#include "runtime/http/http_types.h"

#include <iostream>
#include <string>

namespace {

bool Expect(bool condition, const char* msg) {
  if (!condition) {
    std::cerr << "[FAIL] " << msg << '\n';
    return false;
  }
  return true;
}

void Passed(const char* name) {
  std::cout << "[PASS] " << name << '\n';
}

// ================================================================
// 默认值
// ================================================================

bool TestDefaultDisabled() {
  runtime::gateway::FallbackConfig cfg;
  if (!Expect(!cfg.enabled, "default enabled must be false")) return false;
  Passed("TestDefaultDisabled");
  return true;
}

bool TestDefaultStatusCodeIs503() {
  runtime::gateway::FallbackConfig cfg;
  if (!Expect(cfg.status_code == runtime::http::StatusCode::ServiceUnavailable,
              "default status_code must be 503")) return false;
  Passed("TestDefaultStatusCodeIs503");
  return true;
}

bool TestDefaultContentType() {
  runtime::gateway::FallbackConfig cfg;
  if (!Expect(cfg.content_type == "application/json; charset=utf-8",
              "default content_type must be application/json")) return false;
  Passed("TestDefaultContentType");
  return true;
}

bool TestDefaultBody() {
  runtime::gateway::FallbackConfig cfg;
  if (!Expect(cfg.body.find("service temporarily unavailable") != std::string::npos,
              "default body must mention 'service temporarily unavailable'")) return false;
  Passed("TestDefaultBody");
  return true;
}

// ================================================================
// Init()
// ================================================================

bool TestInitNoOpWhenDisabled() {
  runtime::gateway::FallbackConfig cfg;
  cfg.Init();
  if (!Expect(cfg.pre_rendered.empty(),
              "pre_rendered must be empty when enabled=false")) return false;
  Passed("TestInitNoOpWhenDisabled");
  return true;
}

bool TestInitProducesValidHttpResponse() {
  runtime::gateway::FallbackConfig cfg;
  cfg.enabled = true;
  cfg.Init();

  if (!Expect(!cfg.pre_rendered.empty(),
              "pre_rendered must not be empty after Init")) return false;
  // 应该包含 HTTP/1.1 开头的状态行
  if (!Expect(cfg.pre_rendered.starts_with("HTTP/1.1"),
              "pre_rendered must start with HTTP/1.1")) return false;
  // 应该包含 Content-Type
  if (!Expect(cfg.pre_rendered.find("Content-Type:") != std::string::npos,
              "pre_rendered must contain Content-Type header")) return false;
  // 应该包含 Content-Length
  if (!Expect(cfg.pre_rendered.find("Content-Length:") != std::string::npos,
              "pre_rendered must contain Content-Length header")) return false;
  Passed("TestInitProducesValidHttpResponse");
  return true;
}

bool TestInitContainsDefaultBody() {
  runtime::gateway::FallbackConfig cfg;
  cfg.enabled = true;
  cfg.Init();

  if (!Expect(cfg.pre_rendered.find("\"error\":\"service temporarily unavailable\"") != std::string::npos,
              "pre_rendered must contain default error JSON")) return false;
  Passed("TestInitContainsDefaultBody");
  return true;
}

// ================================================================
// 自定义响应
// ================================================================

bool TestCustomStatusCodeAppearsInResponse() {
  runtime::gateway::FallbackConfig cfg;
  cfg.enabled = true;
  cfg.status_code = runtime::http::StatusCode::BadGateway;
  cfg.body = "bad gateway";
  cfg.Init();

  // 状态行应该包含 502
  if (!Expect(cfg.pre_rendered.find("502") != std::string::npos,
              "pre_rendered must contain custom status code 502")) return false;
  if (!Expect(cfg.pre_rendered.find("bad gateway") != std::string::npos,
              "pre_rendered must contain custom body")) return false;
  Passed("TestCustomStatusCodeAppearsInResponse");
  return true;
}

bool TestCustomContentTypeAppearsInResponse() {
  runtime::gateway::FallbackConfig cfg;
  cfg.enabled = true;
  cfg.content_type = "text/html; charset=utf-8";
  cfg.body = "<html><body>down</body></html>";
  cfg.Init();

  if (!Expect(cfg.pre_rendered.find("text/html") != std::string::npos,
              "pre_rendered must contain custom content type")) return false;
  Passed("TestCustomContentTypeAppearsInResponse");
  return true;
}

bool TestCloseConnectionFlagIsSet() {
  runtime::gateway::FallbackConfig cfg;
  cfg.enabled = true;
  cfg.Init();

  // 降级响应应当关闭连接（Connection: close）
  if (!Expect(cfg.pre_rendered.find("Connection: close") != std::string::npos,
              "pre_rendered must contain Connection: close")) return false;
  Passed("TestCloseConnectionFlagIsSet");
  return true;
}

// ================================================================
// 不可变性：Init 多次调用幂等
// ================================================================

bool TestInitIdempotent() {
  runtime::gateway::FallbackConfig cfg;
  cfg.enabled = true;
  cfg.Init();
  std::string first = cfg.pre_rendered;
  cfg.Init();
  std::string second = cfg.pre_rendered;
  if (!Expect(!first.empty() && first == second,
              "multiple Init calls must produce the same result")) return false;
  Passed("TestInitIdempotent");
  return true;
}

}  // namespace

int main() {
  int passed = 0;
  int total = 0;

#define RUN(test) do { ++total; if (test()) ++passed; } while (0)

  RUN(TestDefaultDisabled);
  RUN(TestDefaultStatusCodeIs503);
  RUN(TestDefaultContentType);
  RUN(TestDefaultBody);
  RUN(TestInitNoOpWhenDisabled);
  RUN(TestInitProducesValidHttpResponse);
  RUN(TestInitContainsDefaultBody);
  RUN(TestCustomStatusCodeAppearsInResponse);
  RUN(TestCustomContentTypeAppearsInResponse);
  RUN(TestCloseConnectionFlagIsSet);
  RUN(TestInitIdempotent);

  std::cout << "===========================\n";
  std::cout << passed << "/" << total << " tests passed.\n";

  return (passed == total) ? 0 : 1;
}
