// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <any>
#include <memory>
#include <functional>
#include <string>
#include <string_view>
#include "vexo/time/timestamp.h"


namespace vexo::net {

class Buffer;
class EventLoop;
class InetAddress;

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

enum class Backend : uint8_t {
  kEpoll,
  kUring,
};

[[nodiscard]] std::unique_ptr<IServer> MakeServer(Backend backend,
                                                  EventLoop* loop,
                                                  const InetAddress& addr,
                                                  std::string name);

}  // namespace vexo::net
