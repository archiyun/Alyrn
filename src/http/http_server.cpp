#include "runtime/http/http_server.h"
#include "runtime/http/http_context.h"
#include "runtime/http/router.h"
#include "runtime/task/scheduler.h"
namespace runtime::http {

HttpServer::HttpServer(runtime::net::EventLoop *loop,
                       const runtime::net::InetAddress &addr, std::string name)
    : server_(loop, addr, name) {
  // HttpServer 复用底层 TcpServer 的连接和消息回调,
  // 把 TCP 事件接入 HTTP 处理链
  server_.SetConnectionCallback(
      [this](const TcpConnectionPtr &conn) { OnConnection(conn); });
  server_.SetMessageCallback(
      [this](const TcpConnectionPtr &conn, runtime::net::Buffer &buf,
             runtime::time::Timestamp ts) { OnMessage(conn, buf, ts); });
}

void HttpServer::SetThreadNum(int num_threads) {
  server_.SetThreadNum(num_threads);
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

void HttpServer::OnConnection(const TcpConnectionPtr &conn) {
  if (conn->Connected()) {
    // 每条已建立连接维护一个独立的 HttpContext,
    // 用于 keep-alive 场景下持续增量解析请求
    conn->SetContext(HttpContext{});
  }
}
void HttpServer::OnMessage(const TcpConnectionPtr &conn,
                           runtime::net::Buffer &buf,
                           runtime::time::Timestamp ts) {
  auto &ctx = std::any_cast<HttpContext &>(conn->GetContext());
  // 解析失败说明当前字节流不满足 HTTP 请求格式， 直接返回 400 并关闭连接
  if (!ctx.ParseRequest(buf, ts)) {
    HttpResponse err = MakeError(StatusCode::BadRequest, "malformed request");
    conn->Send(err.ToString());
    conn->Shutdown();
    return;
  }

  while (ctx.GotAll()) {
    HttpRequest request = ctx.Request();
    ctx.Reset();

    // keep-alive
    const bool keep_alive = request.KeepAlive();
    HttpResponse response(!keep_alive); // 构造参数是 close_connection

    auto match = router_.Match(request.GetMethod(), request.Path());

    if (match.handler && scheduler_) {
      request.SetPathParams(std::move(match.params));
      runtime::task::Task task([conn, req = std::move(request),
                                resp = std::move(response),
                                handler = match.handler]() mutable {
        try {
          handler(req, resp);
        } catch (const std::exception &ex) {
          resp.SetStatusCode(StatusCode::InternalServerError);
          resp.SetContentType("application/json; charset=utf-8");
          resp.SetBody("{\"error\":\"" + std::string(ex.what()) + "\"}");
          resp.SetCloseConnection(true);
        }
        conn->Send(resp.ToString());
        if (resp.CloseConnection())
          conn->Shutdown();
      });
      scheduler_->Submit(std::move(task));
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
    // keep-alive: buf 继续循环
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
} // namespace runtime::http