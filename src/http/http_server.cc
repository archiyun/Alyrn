// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "runtime/http/http_server.h"

#include "runtime/http/debug_handler.h"
#include "runtime/http/http_context.h"
#include "runtime/http/metrics_handler.h"
#include "runtime/http/router.h"
#include "runtime/net/event_loop.h"
#include "runtime/task/scheduler.h"
#ifdef RUNTIME_ENABLE_HTTP2
#include "runtime/http/http2_session.h"
#endif

namespace runtime::http {

HttpServer::HttpServer(runtime::net::EventLoop* loop,
                       const runtime::net::InetAddress& addr,
                       std::string name)
    : server_(loop, addr, name) {
  server_.set_connection_callback(
      [this](const TcpConnectionPtr& conn) { OnConnection(conn); });
  server_.set_message_callback(
      [this](const TcpConnectionPtr& conn, runtime::net::Buffer& buf,
             runtime::time::Timestamp ts) { OnMessage(conn, buf, ts); });
}

void HttpServer::set_thread_num(int num_threads) {
  server_.set_thread_num(num_threads);
}

void HttpServer::set_edge_triggered(bool et) {
  server_.set_edge_triggered(et);
}

void HttpServer::set_scheduler(std::shared_ptr<runtime::task::Scheduler> sched) {
  scheduler_ = std::move(sched);
}

void HttpServer::Get(std::string_view path, Handler handler,
                     std::source_location loc) {
  router_.Get(path, std::move(handler), loc);
}

void HttpServer::Post(std::string_view path, Handler handler,
                      std::source_location loc) {
  router_.Post(path, std::move(handler), loc);
}

void HttpServer::Add(Method method, std::string_view path, Handler handler,
                     std::source_location loc) {
  router_.Add(method, path, std::move(handler), loc);
}

#ifdef RUNTIME_ENABLE_SSL
void HttpServer::set_tls(runtime::net::SslContext* ctx) { ssl_ctx_ = ctx; }
#endif

void HttpServer::Start() { server_.Start(); }

void HttpServer::RegisterDebugTasksRoute() {
  Get("/debug/tasks", [this](const HttpRequest&, HttpResponse& resp) {
    if (!scheduler_) {
      resp.set_status_code(StatusCode::InternalServerError);
      resp.set_content_type("application/json; charset=utf-8");
      resp.set_body("{\"error\":\"scheduler not set\"}");
      return;
    }
    const auto& history = scheduler_->History();
    resp.set_status_code(StatusCode::Ok);
    resp.set_content_type("application/json; charset=utf-8");
    resp.set_body(MakeDebugTasksJson(history.Snapshot(), history.capacity()));
  });
}

void HttpServer::RegisterMetricsRoute() {
  Get("/metrics", [this](const HttpRequest&, HttpResponse& resp) {
    if (!scheduler_) {
      resp.set_status_code(StatusCode::InternalServerError);
      resp.set_content_type("application/json; charset=utf-8");
      resp.set_body("{\"error\":\"scheduler not set\"}");
      return;
    }
    resp.set_status_code(StatusCode::Ok);
    resp.set_content_type("application/json; charset=utf-8");
    resp.set_body(MakeMetricsJson(scheduler_->metrics().Load()));
  });
}

void HttpServer::OnConnection(const TcpConnectionPtr& conn) {
  if (!conn->Connected()) return;

#ifdef RUNTIME_ENABLE_SSL
  if (!ssl_ctx_) {
    conn->set_context(std::make_shared<HttpContext>());
    return;
  }

  conn->set_ssl(ssl_ctx_->NewSsl());
  // Capture weak_ptr to avoid conn → handshake_cb_ → conn circular reference.
  conn->set_handshake_callback(
      [this, weak = std::weak_ptr<runtime::net::TcpConnection>(conn)](
          const std::string& proto) {
        auto c = weak.lock();
        if (!c) return;

#ifdef RUNTIME_ENABLE_HTTP2
        if (proto == "h2") {
          auto session = std::make_shared<Http2Session>(
              c,
              [this](HttpRequest& req, HttpResponse& resp,
                     std::shared_ptr<runtime::net::TcpConnection> c,
                     int32_t sid) {
                auto match = router_.Match(req.method(), req.path());
                if (!match.handler) {
                  resp = match.path_matched
                      ? MakeError(StatusCode::MethodNotAllowed, "method not allowed")
                      : MakeError(StatusCode::NotFound, "not found");
                  auto& ses = std::any_cast<std::shared_ptr<Http2Session>&>(
                      c->context());
                  ses->SendResponse(sid, resp);
                  return;
                }
                req.set_path_params(std::move(match.params));
                if (scheduler_) {
                  scheduler_->Submit(
                      [c, req = std::move(req), resp = std::move(resp),
                       handler = match.handler, sid]() mutable {
                        try {
                          handler(req, resp);
                        } catch (const std::exception& ex) {
                          resp.set_status_code(StatusCode::InternalServerError);
                          resp.set_content_type("application/json; charset=utf-8");
                          resp.set_body("{\"error\":\"" +
                                       std::string(ex.what()) + "\"}");
                        }
                        c->loop()->RunInLoop(
                            [c, resp = std::move(resp), sid] {
                              auto& ses = std::any_cast<
                                  std::shared_ptr<Http2Session>&>(
                                  c->context());
                              ses->SendResponse(sid, resp);
                            });
                      });
                  return;
                }
                try {
                  match.handler(req, resp);
                } catch (const std::exception& ex) {
                  resp = MakeError(StatusCode::InternalServerError, ex.what());
                }
                auto& ses = std::any_cast<std::shared_ptr<Http2Session>&>(
                    c->context());
                ses->SendResponse(sid, resp);
              });
          c->set_context(std::move(session));
        } else {
#endif  // RUNTIME_ENABLE_HTTP2
          c->set_context(std::make_shared<HttpContext>());
#ifdef RUNTIME_ENABLE_HTTP2
        }
#endif
      });
#else
  conn->set_context(std::make_shared<HttpContext>());
#endif  // RUNTIME_ENABLE_SSL
}

void HttpServer::OnMessage(const TcpConnectionPtr& conn,
                           runtime::net::Buffer& buf,
                           runtime::time::Timestamp ts) {
  auto& ctx = conn->context();
#ifdef RUNTIME_ENABLE_HTTP2
  // HTTP/2: feed raw bytes into nghttp2; it drives dispatch via DispatchFn.
  if (ctx.type() == typeid(std::shared_ptr<Http2Session>)) {
    auto& session = std::any_cast<std::shared_ptr<Http2Session>&>(ctx);
    if (!session->Feed(buf)) conn->Shutdown();
    return;
  }
#endif

  auto& h1ctx = *std::any_cast<std::shared_ptr<HttpContext>&>(
      conn->context());
  const ParseStatus parse_status = h1ctx.ParseRequest(buf, ts);
  if (parse_status != ParseStatus::Continue &&
      parse_status != ParseStatus::GotAll) {
    const StatusCode code = ParseStatusToStatusCode(parse_status);
    HttpResponse err = MakeError(code, StatusMessage(code));
    conn->Send(err.ToString());
    conn->Shutdown();
    return;
  }

  while (h1ctx.GotAll()) {
    // TakeRequest moves the parsed request out (HttpRequest is move-only
    // because it owns its arena); Reset reconstructs a fresh HttpRequest
    // so the next pipelined parse starts from a valid arena state.
    HttpRequest request = h1ctx.TakeRequest();
    h1ctx.Reset();

    const bool keep_alive = request.keep_alive();
    HttpResponse response(!keep_alive);

    auto match = router_.Match(request.method(), request.path());

    if (match.handler && scheduler_) {
      request.set_path_params(std::move(match.params));
      // HttpRequest 现在持有 Pool unique_ptr, move-only; std::function
      // 要求 callable 可拷贝, 因此把 req/resp 装进 shared_ptr 让 lambda
      // 持有可拷贝引用.
      struct DispatchState {
        HttpRequest  request;
        HttpResponse response;
      };
      auto state = std::make_shared<DispatchState>(
          DispatchState{std::move(request), std::move(response)});
      scheduler_->Submit(
          [conn, state, handler = match.handler]() mutable {
            try {
              handler(state->request, state->response);
            } catch (const std::exception& ex) {
              state->response.set_status_code(StatusCode::InternalServerError);
              state->response.set_content_type("application/json; charset=utf-8");
              state->response.set_body(
                  "{\"error\":\"" + std::string(ex.what()) + "\"}");
              state->response.set_close_connection(true);
            }
            // 在 worker 上 serialize (把 allocation 留给 worker), 然后
            // 把 Send + Shutdown 原子地派回归属 IO 线程, 保证连接状态写
            // 入单线程.
            std::string wire = state->response.ToString();
            const bool close = state->response.close_connection();
            conn->loop()->RunInLoop(
                [conn, wire = std::move(wire), close] {
                  conn->Send(wire);
                  if (close) conn->Shutdown();
                });
          });
      break;
    } else if (match.handler) {
      request.set_path_params(std::move(match.params));
      try {
        match.handler(request, response);
      } catch (const std::exception &ex) {
        response = MakeError(StatusCode::InternalServerError, ex.what());
        response.set_close_connection(true);
      }
    } else if (match.path_matched) {
      response = MakeError(StatusCode::MethodNotAllowed, "method not allowed");
    } else {
      response = MakeError(StatusCode::NotFound, "not found");
    }
    conn->Send(response.ToString());
    if (response.close_connection()) {
      conn->Shutdown();
      return;
    }
  }
}

HttpResponse HttpServer::MakeError(StatusCode code,
                                   std::string_view message) const {
  HttpResponse response(true);
  response.set_status_code(code);
  response.set_content_type("application/json; charset=utf-8");
  response.set_body("{\"error\":\"" + std::string(message) + "\"}");
  return response;
}

}  // namespace runtime::http
