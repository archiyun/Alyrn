// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "runtime/http/http_server.h"

#include "runtime/http/http_context.h"
#include "runtime/http/metrics_handler.h"
#include "runtime/http/router.h"
#include "runtime/net/event_loop.h"
#include "runtime/task/blocking_executor.h"

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

void HttpServer::set_blocking_executor(
    std::shared_ptr<runtime::task::BlockingExecutor> executor) {
  blocking_executor_ = std::move(executor);
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

void HttpServer::Start() { server_.Start(); }

void HttpServer::RegisterMetricsRoute() {
  Get("/metrics", [this](const HttpRequest&, HttpResponse& resp) {
    if (!blocking_executor_) {
      resp.set_status_code(StatusCode::InternalServerError);
      resp.set_content_type("application/json; charset=utf-8");
      resp.set_body("{\"error\":\"blocking executor not set\"}");
      return;
    }
    resp.set_status_code(StatusCode::Ok);
    resp.set_content_type("application/json; charset=utf-8");
    resp.set_body(MakeMetricsJson(blocking_executor_->metrics().Load()));
  });
}

void HttpServer::OnConnection(const TcpConnectionPtr& conn) {
  if (!conn->Connected()) return;
  conn->set_context(std::make_shared<HttpContext>());
}

void HttpServer::OnMessage(const TcpConnectionPtr& conn,
                           runtime::net::Buffer& buf,
                           runtime::time::Timestamp ts) {
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

    if (match.handler && blocking_executor_) {
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
      blocking_executor_->Submit(
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
