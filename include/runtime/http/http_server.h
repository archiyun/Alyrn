#pragma once

#include "runtime/base/noncopyable.h"
#include "runtime/http/router.h"
#include "runtime/net/tcp_server.h"

#include <string>
#include <string_view>

namespace runtime::http {

class HttpServer : public runtime::base::NonCopyable {
public:
    using TcpConnectionPtr = runtime::net::TcpConnection::TcpConnectionPtr;

    HttpServer(runtime::net::EventLoop *loop,
               const runtime::net::InetAddress &addr,
               std::string name);

    void SetThreadNum(int num_threads);

    // 路由注册，委托 Router
    void Get(std::string_view path, Handler handler);
    void Post(std::string_view path, Handler handler);
    void Add(Method method, std::string_view path, Handler handler);

    void Start();

private:
    void OnConnection(const TcpConnectionPtr &conn);
    void OnMessage(const TcpConnectionPtr &conn,
                   runtime::net::Buffer &buf,
                   runtime::time::Timestamp ts);

    HttpResponse MakeError(StatusCode code, std::string_view message) const;

    runtime::net::TcpServer server_;
    Router router_;
};

}  // namespace runtime::http
