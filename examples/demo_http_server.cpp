#include "runtime/http/http_server.h"
#include "runtime/log/logger.h"
#include "runtime/net/event_loop.h"
#include "runtime/net/inet_address.h"

#include <algorithm>
#include <cstdlib>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

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

// ── in-memory KV store ───────────────────────────────────────────────────────
// Shared across all IO threads; protected by a mutex.
// Keys and values are plain strings; values are JSON-escaped on output.
struct KvStore {
    std::mutex              mu;
    std::unordered_map<std::string, std::string> data;

    // Returns {true, value} on hit, {false, ""} on miss.
    std::pair<bool, std::string> Get(const std::string& key) {
        std::lock_guard<std::mutex> lk(mu);
        auto it = data.find(key);
        if (it == data.end()) return {false, {}};
        return {true, it->second};
    }

    void Set(const std::string& key, std::string value) {
        std::lock_guard<std::mutex> lk(mu);
        data[key] = std::move(value);
    }

    // Returns a JSON array of all keys: ["a","b",...]
    std::string ListKeysJson() {
        std::lock_guard<std::mutex> lk(mu);
        std::string out = "[";
        bool first = true;
        for (const auto& [k, _] : data) {
            if (!first) out += ',';
            out += '"';
            out += JsonEscape(k);
            out += '"';
            first = false;
        }
        out += ']';
        return out;
    }
};

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

    const bool et_mode = (std::getenv("ET_MODE") != nullptr);
    server.SetEdgeTriggered(et_mode);

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

    // KV routes:
    // POST /api/kv/:key writes the request body as the value.
    // GET  /api/kv/:key reads one value.
    // GET  /api/kv lists all keys.
    // The shared store is mutex-protected because handlers may run on
    // multiple I/O threads.
    auto kv = std::make_shared<KvStore>();

    server.Post("/api/kv/:key",
                [kv](const runtime::http::HttpRequest& req,
                     runtime::http::HttpResponse& resp) {
                    const std::string key(req.PathParam("key"));
                    if (key.empty()) {
                        resp.SetStatusCode(runtime::http::StatusCode::BadRequest);
                        resp.SetContentType("application/json; charset=utf-8");
                        resp.SetBody("{\"error\":\"key is empty\"}");
                        return;
                    }
                    kv->Set(key, req.Body());
                    resp.SetContentType("application/json; charset=utf-8");
                    resp.SetBody("{\"ok\":true,\"key\":\"" + JsonEscape(key) + "\"}");
                });

    server.Get("/api/kv/:key",
               [kv](const runtime::http::HttpRequest& req,
                    runtime::http::HttpResponse& resp) {
                   const std::string key(req.PathParam("key"));
                   auto [found, value] = kv->Get(key);
                   resp.SetContentType("application/json; charset=utf-8");
                   if (!found) {
                       resp.SetStatusCode(runtime::http::StatusCode::NotFound);
                       resp.SetBody("{\"error\":\"key not found\",\"key\":\"" +
                                    JsonEscape(key) + "\"}");
                       return;
                   }
                   resp.SetBody("{\"key\":\"" + JsonEscape(key) +
                                "\",\"value\":\"" + JsonEscape(value) + "\"}");
               });

    server.Get("/api/kv",
               [kv](const runtime::http::HttpRequest&,
                    runtime::http::HttpResponse& resp) {
                   resp.SetContentType("application/json; charset=utf-8");
                   resp.SetBody("{\"keys\":" + kv->ListKeysJson() + "}");
               });

    server.Start();

    LOG_INFO() << "demo http server listening on " << host << ':' << port
               << " io_threads=" << thread_count;

    loop.Loop();
    return 0;
}
