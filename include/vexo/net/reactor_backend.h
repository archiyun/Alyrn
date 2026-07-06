// CopyRight (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <any>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "vexo/io/io_backend.h"
#include "vexo/net/buffer.h"
#include "vexo/net/inet_address.h"
#include "vexo/net/tcp_connection.h"
#include "vexo/net/tcp_server.h"
#include "vexo/time/timestamp.h"

namespace vexo::net {

class IConnection;
using ConnPtr = std::shared_ptr<IConnection>;

class IConnection {
public:
  virtual ~IConnection() = default;

  [[nodiscard]] virtual bool Connected() const = 0;
  virtual bool Send(std::string_view data) = 0;
  virtual void Shutdown() = 0;
  [[nodiscard]] virtual const InetAddress& peer_address() const = 0;

  virtual void set_context(std::any ctx) = 0;
  [[nodiscard]] virtual std::any& context() = 0;
};

class IServer {
public:
  using ConnectionCallback = std::function<void(const ConnPtr&)>;
  using MessageCallback = std::function<void(const ConnPtr, Buffer&, vexo::time::Timestamp)>;

  virtual ~IServer() = default;

  virtual void set_connection_callback(ConnectionCallback cb) = 0;
  virtual void set_message_callback(MessageCallback cb) = 0;
  virtual void set_thread_num(int num_threads) = 0;
  virtual void Start() = 0;
  [[nodiscard]] virtual std::string_view name() const = 0;
};

// IConnection adapter over a reactor TcpConnection.
//
// LifeTime: a shared_ptr<ReactorConn> is stashed inside the underlying
// TcpConnection's context slot, so the adapter lives exactly as long as the
// connection. It holds a weak_ptr back to the connection to avoid an ownership
// cycle. Transport ops only run on the owning loop thread while the connection
// is alive, so lock() always succeeds there.
class ReactorConn final : public IConnection {
public:
  explicit ReactorConn(const TcpConnection::TcpConnectionPtr& conn)
      : conn_(conn), peer_(conn->peer_address()) {}

  [[nodiscard]] bool Connected() const override {
    auto c = conn_.lock();
    return c && c->Connected();
  }

  bool Send(std::string_view data) override {
    auto c = conn_.lock();
    return c && c->Send(data);
  }

  void Shutdown() override {
    if (auto c = conn_.lock()) c->Shutdown();
  }

  [[nodiscard]] const InetAddress& peer_address() const override { return peer_; }

  void set_context(std::any ctx) override { context_ = std::move(ctx); }
  [[nodiscard]] std::any& context() override { return context_; }

  [[nodiscard]] TcpConnection::TcpConnectionPtr native() const { return conn_.lock(); }

private:
  std::weak_ptr<TcpConnection> conn_;
  InetAddress peer_;
  std::any context_;  // upper-layer state (separate from the conn's own slot).
};

class ReactorServer final : public IServer {
public:
  ReactorServer(EventLoop* loop, const InetAddress& addr, std::string name)
      : server_(loop, addr, std::move(name)) {}

  void set_connection_callback(ConnectionCallback cb) override {
    connection_callback_ = std::move(cb);
  }

  void set_message_callback(MessageCallback cb) override { message_callback_ = std::move(cb); }

  void set_thread_num(int num_threads) override { server_.set_thread_num(num_threads); }

  std::string_view name() const override { return server_.name(); }

  void Start() override;

private:
  void OnConnection(const TcpConnection::TcpConnectionPtr& conn);
  void OnMessage(const TcpConnection::TcpConnectionPtr& conn, Buffer& buf,
                 vexo::time::Timestamp ts);
  TcpServer server_;
  ConnectionCallback connection_callback_;
  MessageCallback message_callback_;
};

[[nodiscard]] inline TcpConnection::TcpConnectionPtr ReactorNativeConn(const ConnPtr& conn) {
  auto* rc = dynamic_cast<ReactorConn*>(conn.get());
  return rc ? rc->native() : nullptr;
}

[[nodiscard]] std::unique_ptr<IServer> MakeReactorServer(vexo::io::Backend backend, EventLoop* loop,
                                                         const InetAddress& addr, std::string name);

}  // namespace vexo::net
