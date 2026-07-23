#include <iostream>
#include <string>

#include "coropact/http/http_parser.h"
#include "coropact/http/http_request.h"
#include "coropact/http/http_response.h"
#include "coropact/http/router.h"

namespace {

bool Expect(bool condition, const char* message) {
  if (!condition) {
    std::cerr << "[FAIL] " << message << '\n';
    return false;
  }
  return true;
}

bool ParseOk(coropact::http::ParseStatus s) {
  return s == coropact::http::ParseStatus::Continue || s == coropact::http::ParseStatus::GotAll;
}

bool TestParsesHttp11KeepAliveRequest() {
  coropact::http::HttpParser parser;
  const std::string request =
      "GET /api/health?verbose=1 HTTP/1.1\r\n"
      "Host: localhost\r\n"
      "Connection: keep-alive\r\n"
      "X-Trace-Id: trace-123\r\n"
      "\r\n";
  bool ok = Expect(ParseOk(parser.Feed(request)), "parser should accept a valid HTTP/1.1 request");
  ok &= Expect(parser.GotAll(), "parser should complete the request");

  const coropact::http::HttpRequest parsed = parser.TakeRequest();
  ok &= Expect(parsed.method() == coropact::http::Method::Get, "request method should be GET");
  ok &= Expect(parsed.path() == "/api/health", "path should be parsed");
  ok &= Expect(parsed.query() == "verbose=1", "query string should be parsed");
  ok &= Expect(parsed.keep_alive(), "HTTP/1.1 keep-alive should be enabled");
  ok &= Expect(parsed.header("X-TrAcE-Id") == "trace-123",
               "trace id header should be accessible case-insensitively");
  return ok;
}

bool TestMutatesHeadersCaseInsensitively() {
  coropact::http::HttpRequest request;
  request.AddHeader("X-Trace-ID", "first");
  request.set_header("x-TRACE-id", "second");

  bool ok = Expect(request.header("X-Trace-Id") == "second",
                   "set_header should find an existing header without normalizing a lookup key");
  ok &= Expect(request.headers().size() == 1,
               "case variants should not create duplicate header entries");
  ok &= Expect(request.RemoveHeader("X-tRaCe-iD"),
               "RemoveHeader should support a mixed-case heterogeneous lookup");
  ok &=
      Expect(request.header("x-trace-id").empty(), "removed header should no longer be accessible");
  return ok;
}

bool TestBuildsHttpResponse() {
  coropact::http::HttpResponse response(false);
  response.set_status_code(coropact::http::StatusCode::Ok);
  response.set_content_type("application/json; charset=utf-8");
  response.AddHeader("X-Trace-Id", "trace-abc");
  response.set_body("{\"ok\":true}");

  const std::string encoded = response.ToString();
  bool ok =
      Expect(encoded.find("HTTP/1.1 200 OK\r\n") == 0, "response should start with a status line");
  ok &= Expect(encoded.find("Connection: keep-alive\r\n") != std::string::npos,
               "response should encode keep-alive");
  ok &= Expect(encoded.find("X-Trace-Id: trace-abc\r\n") != std::string::npos,
               "response should include trace id");
  ok &= Expect(encoded.find("Content-Length: 11\r\n") != std::string::npos,
               "response should include content length");
  return ok;
}

bool TestParsesRequestBodyAcrossChunks() {
  coropact::http::HttpParser parser;
  const std::string head =
      "POST /api/echo?src=test HTTP/1.1\r\n"
      "Host: localhost\r\n"
      "Content-Length: 5\r\n"
      "\r\n";

  bool ok = Expect(ParseOk(parser.Feed(head)), "parser should accept a partial POST request");
  ok &= Expect(!parser.GotAll(), "parser should wait for the remaining body bytes");

  ok &= Expect(ParseOk(parser.Feed("hello")), "parser should accept the completed body");
  ok &= Expect(parser.GotAll(), "parser should complete after receiving the body");

  const coropact::http::HttpRequest parsed = parser.TakeRequest();
  ok &= Expect(parsed.method() == coropact::http::Method::Post, "request method should be POST");
  ok &= Expect(parsed.body() == "hello", "body should be parsed from the buffer");
  ok &= Expect(parsed.query() == "src=test", "query string should remain available");
  return ok;
}

bool TestMatchesStaticRoute() {
  coropact::http::Router router;
  router.Get("/api/health", [](const coropact::http::HttpRequest&, coropact::http::HttpResponse&) {});

  const auto match = router.Match(coropact::http::Method::Get, "/api/health");
  bool ok = Expect(static_cast<bool>(match.handler), "static route should return a handler");
  ok &= Expect(match.path_matched, "matched static route should set path_matched");
  ok &= Expect(match.params.empty(), "static route should not produce params");
  return ok;
}

bool TestMatchesDynamicRouteAndParams() {
  coropact::http::Router router;
  router.Get("/users/:id/posts/:post_id",
             [](const coropact::http::HttpRequest&, coropact::http::HttpResponse&) {});

  const auto match = router.Match(coropact::http::Method::Get, "/users/42/posts/7");
  bool ok = Expect(static_cast<bool>(match.handler), "dynamic route should return a handler");
  ok &= Expect(match.path_matched, "dynamic route should mark path as matched");

  auto find_param = [&](std::string_view key) -> const coropact::http::PathParam* {
    for (const auto& p : match.params) {
      if (p.key == key) return &p;
    }
    return nullptr;
  };
  const auto* id_p = find_param("id");
  const auto* post_p = find_param("post_id");
  ok &= Expect(id_p != nullptr, "dynamic route should capture id");
  ok &= Expect(post_p != nullptr, "dynamic route should capture post_id");
  ok &= Expect(id_p && id_p->value == "42", "id param should equal 42");
  ok &= Expect(post_p && post_p->value == "7", "post_id param should equal 7");
  return ok;
}

bool TestPrefersStaticRouteOverParamRoute() {
  coropact::http::Router router;
  bool static_handler_called = false;
  bool param_handler_called = false;

  router.Get("/users/me", [&](const coropact::http::HttpRequest&, coropact::http::HttpResponse&) {
    static_handler_called = true;
  });
  router.Get("/users/:id", [&](const coropact::http::HttpRequest&, coropact::http::HttpResponse&) {
    param_handler_called = true;
  });

  const auto match = router.Match(coropact::http::Method::Get, "/users/me");
  bool ok = Expect(static_cast<bool>(match.handler), "static route should still produce a handler");
  ok &=
      Expect(match.params.empty(), "static route should not capture params when exact path exists");

  if (match.handler) {
    coropact::http::HttpRequest request;
    coropact::http::HttpResponse response(true);
    match.handler(request, response);
  }

  ok &= Expect(static_handler_called, "static route handler should win over parameter route");
  ok &= Expect(!param_handler_called,
               "parameter route handler should not run when static route matches");
  return ok;
}

bool TestDistinguishes404And405() {
  coropact::http::Router router;
  router.Get("/users/:id", [](const coropact::http::HttpRequest&, coropact::http::HttpResponse&) {});

  const auto wrong_method = router.Match(coropact::http::Method::Post, "/users/42");
  bool ok = Expect(!wrong_method.handler, "wrong method should not return a handler");
  ok &= Expect(wrong_method.path_matched, "wrong method should still mark path as matched for 405");

  const auto missing = router.Match(coropact::http::Method::Get, "/articles/42");
  ok &= Expect(!missing.handler, "missing route should not return a handler");
  ok &= Expect(!missing.path_matched, "missing route should leave path_matched false for 404");
  return ok;
}

bool TestRejectsTransferEncoding() {
  coropact::http::HttpParser parser;
  return Expect(!ParseOk(parser.Feed("POST / HTTP/1.1\r\n"
                                     "Host: x\r\n"
                                     "Transfer-Encoding: chunked\r\n"
                                     "\r\n")),
                "parser should reject Transfer-Encoding");
}

bool TestRejectsCLAndTEBoth() {
  coropact::http::HttpParser parser;
  return Expect(!ParseOk(parser.Feed("POST / HTTP/1.1\r\n"
                                     "Host: x\r\n"
                                     "Content-Length: 5\r\n"
                                     "Transfer-Encoding: chunked\r\n"
                                     "\r\n")),
                "parser should reject Content-Length + Transfer-Encoding");
}

bool TestRejectsDuplicateContentLength() {
  coropact::http::HttpParser parser;
  return Expect(!ParseOk(parser.Feed("POST / HTTP/1.1\r\n"
                                     "Host: x\r\n"
                                     "Content-Length: 10\r\n"
                                     "Content-Length: 20\r\n"
                                     "\r\n"
                                     "0123456789")),
                "parser should reject duplicate Content-Length");
}

bool TestRejectsTooManyHeaders() {
  std::string request = "GET / HTTP/1.1\r\n";
  for (int i = 0; i < 200; ++i) {
    request += "X-Spam-" + std::to_string(i) + ": v\r\n";
  }
  request += "\r\n";
  coropact::http::HttpParser parser;
  return Expect(!ParseOk(parser.Feed(request)),
                "parser should reject when header count exceeds the cap");
}

bool TestRejectsOversizedRequestLine() {
  const std::string request =
      "GET /" + std::string(9000, 'a') + " HTTP/1.1\r\n\r\n";  // > kMaxRequestLine (8 KiB)
  coropact::http::HttpParser parser;
  return Expect(!ParseOk(parser.Feed(request)), "parser should reject request line beyond the cap");
}

bool TestRejectsObsFold() {
  // RFC 9112 §5.2: obsolete line folding (continuation line starting with
  // whitespace) must be rejected. The fold line lacks a colon, so the parser
  // rejects it as a malformed header — lock that behavior down.
  coropact::http::HttpParser parser;
  const auto s = parser.Feed(
      "GET / HTTP/1.1\r\n"
      "Host: x\r\n"
      "X-Foo: bar\r\n"
      " continuation\r\n"
      "\r\n");
  return Expect(s == coropact::http::ParseStatus::BadRequest,
                "obs-fold continuation must yield BadRequest");
}

bool TestMapsParseStatusToStatusCode() {
  using coropact::http::ParseStatus;
  using coropact::http::ParseStatusToStatusCode;
  using coropact::http::StatusCode;
  bool ok = true;
  ok &= Expect(ParseStatusToStatusCode(ParseStatus::UriTooLong) == StatusCode::UriTooLong, "414");
  ok &= Expect(ParseStatusToStatusCode(ParseStatus::HeaderTooLarge) ==
                   StatusCode::RequestHeaderFieldsTooLarge,
               "431");
  ok &= Expect(ParseStatusToStatusCode(ParseStatus::PayloadTooLarge) == StatusCode::PayloadTooLarge,
               "413");
  ok &=
      Expect(ParseStatusToStatusCode(ParseStatus::BadMethod) == StatusCode::NotImplemented, "501");
  ok &= Expect(
      ParseStatusToStatusCode(ParseStatus::BadVersion) == StatusCode::HttpVersionNotSupported,
      "505");
  ok &= Expect(ParseStatusToStatusCode(ParseStatus::BadRequest) == StatusCode::BadRequest, "400");
  return ok;
}

bool TestReusesParserOwnedRequest() {
  coropact::http::HttpParser parser;
  bool ok = Expect(ParseOk(parser.Feed("GET /first HTTP/1.1\r\nHost: localhost\r\n\r\n")),
                   "parser should complete the first request");
  auto* first = &parser.CurrentRequest();
  ok &= Expect(first->path() == "/first", "current request should expose the parsed path");

  parser.Reset();
  ok &= Expect(!parser.GotAll(), "reset should make the parser ready for the next request");
  ok &= Expect(ParseOk(parser.Feed("GET /second HTTP/1.1\r\nHost: localhost\r\n\r\n")),
               "parser should complete the second request");
  auto* second = &parser.CurrentRequest();
  ok &= Expect(first == second, "current request should be reused in place");
  ok &= Expect(second->path() == "/second", "reused request should contain the new path");
  return ok;
}

bool TestParsesConnectMethod() {
  coropact::http::HttpParser parser;
  bool ok = Expect(ParseOk(parser.Feed("CONNECT example.com:443 HTTP/1.1\r\n"
                                       "Host: example.com:443\r\n"
                                       "\r\n")),
                   "CONNECT should parse");
  const auto request = parser.TakeRequest();
  ok &= Expect(request.method() == coropact::http::Method::Connect, "method enum should be Connect");
  return ok;
}

}  // namespace

bool TestRouterPathNormalization() {
  coropact::http::Router r;
  bool hit = false;
  r.Get("/foo", [&](const coropact::http::HttpRequest&, coropact::http::HttpResponse&) { hit = true; });

  for (auto p : {"/foo", "/foo/", "//foo", "/foo//"}) {
    auto m = r.Match(coropact::http::Method::Get, p);
    if (!m.handler) {
      std::fprintf(stderr, "missed: %s\n", p);
      return false;
    }
  }
  return true;
}

int main() {
  const bool ok = TestParsesHttp11KeepAliveRequest() && TestMutatesHeadersCaseInsensitively() &&
                  TestBuildsHttpResponse() && TestParsesRequestBodyAcrossChunks() &&
                  TestReusesParserOwnedRequest() && TestMatchesStaticRoute() &&
                  TestMatchesDynamicRouteAndParams() && TestPrefersStaticRouteOverParamRoute() &&
                  TestDistinguishes404And405() && TestRejectsTransferEncoding() &&
                  TestRejectsCLAndTEBoth() && TestRejectsDuplicateContentLength() &&
                  TestRejectsTooManyHeaders() && TestRejectsOversizedRequestLine() &&
                  TestRejectsObsFold() && TestMapsParseStatusToStatusCode() &&
                  TestParsesConnectMethod() && TestRouterPathNormalization();
  if (!ok) {
    return 1;
  }

  std::cout << "[PASS] http_smoke_test\n";
  return 0;
}
