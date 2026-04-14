#include "runtime/http/http_context.h"
#include "runtime/http/http_request.h"
#include "runtime/http/http_response.h"
#include "runtime/http/router.h"
#include "runtime/net/buffer.h"

#include <iostream>
#include <string>

namespace {

bool Expect(bool condition, const char* message) {
    if (!condition) {
        std::cerr << "[FAIL] " << message << '\n';
        return false;
    }
    return true;
}

bool TestParsesHttp11KeepAliveRequest() {
    runtime::http::HttpContext context;
    runtime::net::Buffer buffer;
    const std::string request =
        "GET /api/health?verbose=1 HTTP/1.1\r\n"
        "Host: localhost\r\n"
        "Connection: keep-alive\r\n"
        "X-Trace-Id: trace-123\r\n"
        "\r\n";
    buffer.Append(request);

    bool ok = Expect(context.ParseRequest(buffer, runtime::time::Timestamp::Now()),
                     "parser should accept a valid HTTP/1.1 request");
    ok &= Expect(context.GotAll(), "parser should complete the request");

    const runtime::http::HttpRequest& parsed = context.Request();
    ok &= Expect(parsed.GetMethod() == runtime::http::Method::Get,
                 "request method should be GET");
    ok &= Expect(parsed.Path() == "/api/health", "path should be parsed");
    ok &= Expect(parsed.Query() == "verbose=1", "query string should be parsed");
    ok &= Expect(parsed.KeepAlive(), "HTTP/1.1 keep-alive should be enabled");
    ok &= Expect(parsed.GetHeader("x-trace-id") == "trace-123",
                 "trace id header should be accessible case-insensitively");
    return ok;
}

bool TestBuildsHttpResponse() {
    runtime::http::HttpResponse response(false);
    response.SetStatusCode(runtime::http::StatusCode::Ok);
    response.SetContentType("application/json; charset=utf-8");
    response.AddHeader("X-Trace-Id", "trace-abc");
    response.SetBody("{\"ok\":true}");

    const std::string encoded = response.ToString();
    bool ok = Expect(encoded.find("HTTP/1.1 200 OK\r\n") == 0,
                     "response should start with a status line");
    ok &= Expect(encoded.find("Connection: keep-alive\r\n") != std::string::npos,
                 "response should encode keep-alive");
    ok &= Expect(encoded.find("X-Trace-Id: trace-abc\r\n") != std::string::npos,
                 "response should include trace id");
    ok &= Expect(encoded.find("Content-Length: 11\r\n") != std::string::npos,
                 "response should include content length");
    return ok;
}

bool TestParsesRequestBodyAcrossChunks() {
    runtime::http::HttpContext context;
    runtime::net::Buffer buffer;
    const std::string head =
        "POST /api/echo?src=test HTTP/1.1\r\n"
        "Host: localhost\r\n"
        "Content-Length: 5\r\n"
        "\r\n";

    buffer.Append(head);
    bool ok = Expect(context.ParseRequest(buffer, runtime::time::Timestamp::Now()),
                     "parser should accept a partial POST request");
    ok &= Expect(!context.GotAll(), "parser should wait for the remaining body bytes");

    buffer.Append("hello");
    ok &= Expect(context.ParseRequest(buffer, runtime::time::Timestamp::Now()),
                 "parser should accept the completed body");
    ok &= Expect(context.GotAll(), "parser should complete after receiving the body");

    const runtime::http::HttpRequest& parsed = context.Request();
    ok &= Expect(parsed.GetMethod() == runtime::http::Method::Post,
                 "request method should be POST");
    ok &= Expect(parsed.Body() == "hello", "body should be parsed from the buffer");
    ok &= Expect(parsed.Query() == "src=test", "query string should remain available");
    return ok;
}

bool TestMatchesStaticRoute() {
    runtime::http::Router router;
    router.Get("/api/health",
               [](const runtime::http::HttpRequest&, runtime::http::HttpResponse&) {});

    const auto match = router.Match(runtime::http::Method::Get, "/api/health");
    bool ok = Expect(static_cast<bool>(match.handler),
                     "static route should return a handler");
    ok &= Expect(match.path_matched, "matched static route should set path_matched");
    ok &= Expect(match.params.empty(), "static route should not produce params");
    return ok;
}

bool TestMatchesDynamicRouteAndParams() {
    runtime::http::Router router;
    router.Get("/users/:id/posts/:post_id",
               [](const runtime::http::HttpRequest&, runtime::http::HttpResponse&) {});

    const auto match =
        router.Match(runtime::http::Method::Get, "/users/42/posts/7");
    bool ok = Expect(static_cast<bool>(match.handler),
                     "dynamic route should return a handler");
    ok &= Expect(match.path_matched, "dynamic route should mark path as matched");

    const auto id_it = match.params.find("id");
    const auto post_it = match.params.find("post_id");
    ok &= Expect(id_it != match.params.end(), "dynamic route should capture id");
    ok &= Expect(post_it != match.params.end(),
                 "dynamic route should capture post_id");
    ok &= Expect(id_it != match.params.end() && id_it->second == "42",
                 "id param should equal 42");
    ok &= Expect(post_it != match.params.end() && post_it->second == "7",
                 "post_id param should equal 7");
    return ok;
}

bool TestPrefersStaticRouteOverParamRoute() {
    runtime::http::Router router;
    bool static_handler_called = false;
    bool param_handler_called = false;

    router.Get("/users/me",
               [&](const runtime::http::HttpRequest&, runtime::http::HttpResponse&) {
                   static_handler_called = true;
               });
    router.Get("/users/:id",
               [&](const runtime::http::HttpRequest&, runtime::http::HttpResponse&) {
                   param_handler_called = true;
               });

    const auto match = router.Match(runtime::http::Method::Get, "/users/me");
    bool ok = Expect(static_cast<bool>(match.handler),
                     "static route should still produce a handler");
    ok &= Expect(match.params.empty(),
                 "static route should not capture params when exact path exists");

    if (match.handler) {
        runtime::http::HttpRequest request;
        runtime::http::HttpResponse response(true);
        match.handler(request, response);
    }

    ok &= Expect(static_handler_called,
                 "static route handler should win over parameter route");
    ok &= Expect(!param_handler_called,
                 "parameter route handler should not run when static route matches");
    return ok;
}

bool TestDistinguishes404And405() {
    runtime::http::Router router;
    router.Get("/users/:id",
               [](const runtime::http::HttpRequest&, runtime::http::HttpResponse&) {});

    const auto wrong_method = router.Match(runtime::http::Method::Post, "/users/42");
    bool ok = Expect(!wrong_method.handler,
                     "wrong method should not return a handler");
    ok &= Expect(wrong_method.path_matched,
                 "wrong method should still mark path as matched for 405");

    const auto missing = router.Match(runtime::http::Method::Get, "/articles/42");
    ok &= Expect(!missing.handler, "missing route should not return a handler");
    ok &= Expect(!missing.path_matched,
                 "missing route should leave path_matched false for 404");
    return ok;
}

}  // namespace

int main() {
    const bool ok = TestParsesHttp11KeepAliveRequest() &&
                    TestBuildsHttpResponse() &&
                    TestParsesRequestBodyAcrossChunks() &&
                    TestMatchesStaticRoute() &&
                    TestMatchesDynamicRouteAndParams() &&
                    TestPrefersStaticRouteOverParamRoute() &&
                    TestDistinguishes404And405();
    if (!ok) {
        return 1;
    }

    std::cout << "[PASS] http_smoke_test\n";
    return 0;
}
