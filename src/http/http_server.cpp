// Copyright (c) 2026 Aresna
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
  server_.SetConnectionCallback(
      [this](const TcpConnectionPtr& conn) { OnConnection(conn); });
  server_.SetMessageCallback(
      [this](const TcpConnectionPtr& conn, runtime::net::Buffer& buf,
             runtime::time::Timestamp ts) { OnMessage(conn, buf, ts); });
}

void HttpServer::SetThreadNum(int num_threads) {
  server_.SetThreadNum(num_threads);
}

void HttpServer::SetEdgeTriggered(bool et) {
  server_.SetEdgeTriggered(et);
}

void HttpServer::SetScheduler(std::shared_ptr<runtime::task::Scheduler> sched) {
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
void HttpServer::SetTls(runtime::net::SslContext* ctx) { ssl_ctx_ = ctx; }
#endif

void HttpServer::Start() { server_.Start(); }

void HttpServer::RegisterDebugTasksRoute() {
  Get("/debug/tasks", [this](const HttpRequest&, HttpResponse& resp) {
    if (!scheduler_) {
      resp.SetStatusCode(StatusCode::InternalServerError);
      resp.SetContentType("application/json; charset=utf-8");
      resp.SetBody("{\"error\":\"scheduler not set\"}");
      return;
    }
    const auto& history = scheduler_->History();
    resp.SetStatusCode(StatusCode::Ok);
    resp.SetContentType("application/json; charset=utf-8");
    resp.SetBody(MakeDebugTasksJson(history.Snapshot(), history.Capacity()));
  });
}

void HttpServer::RegisterMetricsRoute() {
  Get("/metrics", [this](const HttpRequest&, HttpResponse& resp) {
    if (!scheduler_) {
      resp.SetStatusCode(StatusCode::InternalServerError);
      resp.SetContentType("application/json; charset=utf-8");
      resp.SetBody("{\"error\":\"scheduler not set\"}");
      return;
    }
    resp.SetStatusCode(StatusCode::Ok);
    resp.SetContentType("application/json; charset=utf-8");
    resp.SetBody(MakeMetricsJson(scheduler_->Metrics().Load()));
  });
}

void HttpServer::OnConnection(const TcpConnectionPtr& conn) {
  if (!conn->Connected()) return;

#ifdef RUNTIME_ENABLE_SSL
  if (!ssl_ctx_) {
    conn->SetContext(std::make_shared<HttpContext>());
    return;
  }

  conn->SetSsl(ssl_ctx_->NewSsl());
  // Capture weak_ptr to avoid conn → handshake_cb_ → conn circular reference.
  conn->SetHandshakeCallback(
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
                auto match = router_.Match(req.GetMethod(), req.Path());
                if (!match.handler) {
                  resp = match.path_matched
                      ? MakeError(StatusCode::MethodNotAllowed, "method not allowed")
                      : MakeError(StatusCode::NotFound, "not found");
                  auto& ses = std::any_cast<std::shared_ptr<Http2Session>&>(
                      c->GetContext());
                  ses->SendResponse(sid, resp);
                  return;
                }
                req.SetPathParams(std::move(match.params));
                if (scheduler_) {
                  scheduler_->Submit(
                      [c, req = std::move(req), resp = std::move(resp),
                       handler = match.handler, sid]() mutable {
                        try {
                          handler(req, resp);
                        } catch (const std::exception& ex) {
                          resp.SetStatusCode(StatusCode::InternalServerError);
                          resp.SetContentType("application/json; charset=utf-8");
                          resp.SetBody("{\"error\":\"" +
                                       std::string(ex.what()) + "\"}");
                        }
                        c->GetLoop()->RunInLoop(
                            [c, resp = std::move(resp), sid] {
                              auto& ses = std::any_cast<
                                  std::shared_ptr<Http2Session>&>(
                                  c->GetContext());
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
                    c->GetContext());
                ses->SendResponse(sid, resp);
              });
          c->SetContext(std::move(session));
        } else {
#endif  // RUNTIME_ENABLE_HTTP2
          c->SetContext(std::make_shared<HttpContext>());
#ifdef RUNTIME_ENABLE_HTTP2
        }
#endif
      });
#else
  conn->SetContext(std::make_shared<HttpContext>());
#endif  // RUNTIME_ENABLE_SSL
}

void HttpServer::OnMessage(const TcpConnectionPtr& conn,
                           runtime::net::Buffer& buf,
                           runtime::time::Timestamp ts) {
  auto& ctx = conn->GetContext();
#ifdef RUNTIME_ENABLE_HTTP2
  // HTTP/2: feed raw bytes into nghttp2; it drives dispatch via DispatchFn.
  if (ctx.type() == typeid(std::shared_ptr<Http2Session>)) {
    auto& session = std::any_cast<std::shared_ptr<Http2Session>&>(ctx);
    if (!session->Feed(buf)) conn->Shutdown();
    return;
  }
#endif

  auto& h1ctx = *std::any_cast<std::shared_ptr<HttpContext>&>(
      conn->GetContext());
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
    // TakeRequest moves the parsed request out of h1ctx; Reset reconstructs
    // a fresh HttpRequest so the next pipelined parse starts from a valid
    // arena state.
    HttpRequest request = h1ctx.TakeRequest();
    h1ctx.Reset();

    const bool keep_alive = request.KeepAlive();
    HttpResponse response(!keep_alive);

    auto match = router_.Match(request.GetMethod(), request.GetPath());

    if (match.handler && scheduler_) {
      request.SetPathParams(match.params);
      // HttpRequest is move-only (owns a Pool); std::function requires
      // copy-constructibility, so the request/response are shuttled through
      // a shared_ptr-owned state struct that the lambda holds by value.
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
              state->response.SetStatusCode(StatusCode::InternalServerError);
              state->response.SetContentType(
                  "application/json; charset=utf-8");
              state->response.SetBody(
                  "{\"error\":\"" + std::string(ex.what()) + "\"}");
              state->response.SetCloseConnection(true);
            }
            // Serialize on the worker (keeps allocation off the IO thread),
            // then dispatch Send + Shutdown atomically to the owning IO thread
            // so all connection-state writes stay single-threaded.
            std::string wire = state->response.ToString();
            const bool close = state->response.CloseConnection();
            conn->GetLoop()->RunInLoop(
                [conn, wire = std::move(wire), close] {
                  conn->Send(wire);
                  if (close) conn->Shutdown();
                });
          });
      break;
    } else if (match.handler) {
      request.SetPathParams(match.params);
      try {
        match.handler(request, response);
      } catch (const std::exception &ex) {
        response = MakeError(StatusCode::InternalServerError, ex.what());
        response.SetCloseConnection(true);
      }
    } else if (match.path_matched) {
      response = MakeError(StatusCode::MethodNotAllowed, "method not allowed");
    } else {
      response = MakeError(StatusCode::NotFound, "not found");
    }
    conn->Send(response.ToString());
    if (response.CloseConnection()) {
      conn->Shutdown();
      return;
    }
  }
}

HttpResponse HttpServer::MakeError(StatusCode code,
                                   std::string_view message) const {
  HttpResponse response(true);
  response.SetStatusCode(code);
  response.SetContentType("application/json; charset=utf-8");
  response.SetBody("{\"error\":\"" + std::string(message) + "\"}");
  return response;
}

}  // namespace runtime::http
