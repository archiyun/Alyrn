#include "runtime/http/http_server.h"
#include "runtime/http/http_context.h"
#include "runtime/http/router.h"
#include "runtime/task/scheduler.h"

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

void HttpServer::Get(std::string_view path, Handler handler) {
  router_.Get(path, std::move(handler));
}

void HttpServer::Post(std::string_view path, Handler handler) {
  router_.Post(path, std::move(handler));
}

void HttpServer::Add(Method method, std::string_view path, Handler handler) {
  router_.Add(method, path, std::move(handler));
}

void HttpServer::Start() { server_.Start(); }

void HttpServer::OnConnection(const TcpConnectionPtr& conn) {
  if (conn->Connected()) {
    conn->SetContext(HttpContext{});
  }
}

void HttpServer::OnMessage(const TcpConnectionPtr& conn,
                           runtime::net::Buffer& buf,
                           runtime::time::Timestamp ts) {
  auto& ctx = std::any_cast<HttpContext&>(conn->GetContext());
  if (!ctx.ParseRequest(buf, ts)) {
    HttpResponse err = MakeError(StatusCode::BadRequest, "malformed request");
    conn->Send(err.ToString());
    conn->Shutdown();
    return;
  }

  while (ctx.GotAll()) {
    HttpRequest request = ctx.Request();
    ctx.Reset();

    const bool keep_alive = request.KeepAlive();
    HttpResponse response(!keep_alive);

    auto match = router_.Match(request.GetMethod(), request.Path());

    if (match.handler && scheduler_) {
      request.SetPathParams(std::move(match.params));
      // Run the handler on a worker thread; conn->Send() is called from the
      // worker (cross-thread fd access — Phase 6 will fix via RunInLoop).
      scheduler_->Submit(
          [conn, req = std::move(request), resp = std::move(response),
           handler = match.handler]() mutable {
            try {
              handler(req, resp);
            } catch (const std::exception& ex) {
              resp.SetStatusCode(StatusCode::InternalServerError);
              resp.SetContentType("application/json; charset=utf-8");
              resp.SetBody("{\"error\":\"" + std::string(ex.what()) + "\"}");
              resp.SetCloseConnection(true);
            }
            conn->Send(resp.ToString());
            if (resp.CloseConnection()) conn->Shutdown();
          });
      break;
    } else if (match.handler) {
      request.SetPathParams(std::move(match.params));
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
