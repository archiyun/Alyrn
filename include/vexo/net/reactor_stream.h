#pragma once

#include <cstddef>
#include <span>

#include "vexo/base/error.h"
#include "vexo/coro/task.h"
#include "vexo/net/channel.h"
#include "vexo/net/event_loop.h"
#include "vexo/net/socket.h"

namespace vexo::net {
class ReactorStream {
public:
  ReactorStream(EventLoop* loop, int fd);
  ~ReactorStream();

  coro::Task<base::Result<std::size_t>> ReadSome(std::span<std::byte> buffer);

private:
  class ReadAwaiter;

  void HandleRead(vexo::time::Timestamp);
  void CompleteRead(vexo::base::Result<std::size_t> result);

  EventLoop* loop_;
  Socket socket_;
  Channel channel_;
  ReadAwaiter* pending_read_{nullptr};
};
}  // namespace vexo::net
