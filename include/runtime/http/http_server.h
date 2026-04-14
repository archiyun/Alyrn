#pragma once

#include "runtime/base/noncopyable.h"
#include "runtime/http/router.h"
#include "runtime/net/tcp_server.h"
#include "runtime/task/scheduler.h"

#include <memory>
#include <string>
#include <string_view>

namespace runtime::http {

/**
 * HttpServer:
 * 在 TcpServer 之上封装一层最小 HTTP/1.1 服务能力。
 *
 * 负责:
 * - 接收 TcpConnection 上的输入数据
 * - 为每条连接维护一个 HttpContext 做增量解析
 * - 将完整 HttpRequest 分发给 Router
 * - 生成 HttpResponse 并回写到连接
 *
 * 对外接口只暴露线程数设置、路由注册和启动入口，
 * 具体的连接生命周期和协议解析流程由内部回调驱动。
 * 
 * 一次请求的大致流程是:
 * TcpConnection -> HttpContext -> Router -> HttpResponse
 */
class HttpServer : public runtime::base::NonCopyable {
public:
    using TcpConnectionPtr = runtime::net::TcpConnection::TcpConnectionPtr;

    HttpServer(runtime::net::EventLoop *loop,
               const runtime::net::InetAddress &addr,
               std::string name);

    void SetThreadNum(int num_threads);

    void SetScheduler(std::shared_ptr<runtime::task::Scheduler> sched);

    // 注册接口只是对 Router 的薄封装，最终落到内部路由表。
    void Get(std::string_view path, Handler handler);
    void Post(std::string_view path, Handler handler);
    void Add(Method method, std::string_view path, Handler handler);

    void Start();

private:
    // 连接建立时为 TcpConnection 初始化 HTTP 上下文
    void OnConnection(const TcpConnectionPtr &conn);
    // 收到字节流后尝试增量解析请求， 并在请求完整时执行路由分发
    void OnMessage(const TcpConnectionPtr &conn,
                   runtime::net::Buffer &buf,
                   runtime::time::Timestamp ts);

    // 统一构造 JSON 格式错误响应
    HttpResponse MakeError(StatusCode code, std::string_view message) const;

    runtime::net::TcpServer server_;
    Router router_;
    std::shared_ptr<runtime::task::Scheduler>   scheduler_;
};

}  // namespace runtime::http
