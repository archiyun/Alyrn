#include "runtime/http/http_server.h"
#include "runtime/log/logger.h"
#include "runtime/net/event_loop.h"
#include "runtime/net/inet_address.h"

int main() {
    auto& logger = runtime::log::Logger::Instance();
    logger.Init("demo_http_server.log", runtime::log::LogLevel::INFO);

    runtime::net::EventLoop loop;
    runtime::net::InetAddress addr(18080, "127.0.0.1");
    runtime::http::HttpServer server(&loop, addr, "demo");

    server.SetThreadNum(2);

    server.Get("/", [](const runtime::http::HttpRequest&,
                       runtime::http::HttpResponse& resp) {
        resp.SetContentType("text/plain; charset=utf-8");
        resp.SetBody("hello from runtime-http\n");
    });

    server.Get("/health", [](const runtime::http::HttpRequest&,
                             runtime::http::HttpResponse& resp) {
        resp.SetContentType("application/json; charset=utf-8");
        resp.SetBody("{\"ok\":true}");
    });

    server.Post("/echo", [](const runtime::http::HttpRequest& req,
                            runtime::http::HttpResponse& resp) {
        resp.SetContentType("application/json; charset=utf-8");
        resp.SetBody("{\"body\":\"" + req.Body() + "\"}");
    });

    LOG_INFO() << "listening on 127.0.0.1:18080";
    server.Start();
    loop.Loop();
    return 0;
}
