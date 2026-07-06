// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "vexo/http/http_server.h"

#include "vexo/http/http_context.h"
#include "vexo/http/router.h"

#include <any>
#include <exception>
#include <memory>
#include <utility>

namespace vexo::http {

HttpServer::HttpServer(vexo::net::EventLoop* loop,
                       const vexo::net::InetAddress& addr,
                       std::string name)
    : server_(loop, addr, name) {
  server_.set_connection_callback(
      [this](const TcpConnectionPtr& conn) { OnConnection(conn); });
  server_.set_message_callback(
      [this](const TcpConnectionPtr& conn, vexo::net::Buffer& buf,
             vexo::time::Timestamp ts) { OnMessage(conn, buf, ts); });
}

void HttpServer::set_thread_num(int num_threads) {
  server_.set_thread_num(num_threads);
}

void HttpServer::set_edge_triggered(bool et) {
  server_.set_edge_triggered(et);
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

void HttpServer::OnConnection(const TcpConnectionPtr& conn) {
  if (!conn->Connected()) return;
  conn->set_context(std::make_shared<HttpContext>());
}

void HttpServer::OnMessage(const TcpConnectionPtr& conn,
                           vexo::net::Buffer& buf,
                           vexo::time::Timestamp ts) {
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

    if (match.handler) {
      request.set_path_params(std::move(match.params));
      try {
        match.handler(request, response);
      } catch (const std::exception& ex) {
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

}  // namespace vexo::http
