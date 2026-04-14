#include "runtime/http/http_server.h"
#include "runtime/log/logger.h"
#include "runtime/net/event_loop.h"
#include "runtime/net/inet_address.h"

#include <algorithm>
#include <cstdlib>
#include <string>
#include <thread>

namespace {

std::string ReadEnvOrDefault(const char* key, const char* fallback) {
    const char* value = std::getenv(key);
    return value == nullptr ? std::string(fallback) : std::string(value);
}

unsigned int ResolveThreadCount() {
    const char* value = std::getenv("IO_THREADS");
    if (value != nullptr) {
        return static_cast<unsigned int>(std::max(1, std::stoi(value)));
    }

    const unsigned int detected = std::thread::hardware_concurrency();
    if (detected == 0) {
        return 4;
    }
    return std::min(detected, 4u);
}

std::string JsonEscape(const std::string& input) {
    std::string out;
    out.reserve(input.size() + 8);
    for (char ch : input) {
        switch (ch) {
        case '\\':
            out += "\\\\";
            break;
        case '"':
            out += "\\\"";
            break;
        case '\n':
            out += "\\n";
            break;
        case '\r':
            out += "\\r";
            break;
        case '\t':
            out += "\\t";
            break;
        default:
            out.push_back(ch);
            break;
        }
    }
    return out;
}

}  // namespace

int main() {
    auto& logger = runtime::log::Logger::Instance();
    logger.Init("demo_http_server.log", runtime::log::LogLevel::INFO);

    const std::string host = ReadEnvOrDefault("HOST", "127.0.0.1");
    const std::uint16_t port = static_cast<std::uint16_t>(
        std::stoi(ReadEnvOrDefault("PORT", "18080")));

    runtime::net::EventLoop loop;
    runtime::http::HttpServer server(
        &loop,
        runtime::net::InetAddress(port, host),
        "DemoHttpServer");

    const unsigned int thread_count = ResolveThreadCount();
    server.SetThreadNum(static_cast<int>(thread_count));

    server.Get("/", [](const runtime::http::HttpRequest&,
                       runtime::http::HttpResponse& resp) {
        resp.SetContentType("text/plain; charset=utf-8");
        resp.SetBody("runtime http server is up\n");
    });

    server.Get("/api/health",
               [](const runtime::http::HttpRequest& req,
                  runtime::http::HttpResponse& resp) {
                   resp.SetContentType("application/json; charset=utf-8");
                   resp.SetBody(
                       "{\"ok\":true,\"path\":\"" + JsonEscape(req.Path()) + "\"}");
               });

    server.Post("/api/echo",
                [](const runtime::http::HttpRequest& req,
                   runtime::http::HttpResponse& resp) {
                    resp.SetContentType("application/json; charset=utf-8");
                    resp.SetBody(
                        "{\"echo\":\"" + JsonEscape(req.Body()) + "\",\"query\":\"" +
                        JsonEscape(req.Query()) + "\"}");
                });

    server.Start();

    LOG_INFO() << "demo http server listening on " << host << ':' << port
               << " io_threads=" << thread_count;

    loop.Loop();
    return 0;
}
