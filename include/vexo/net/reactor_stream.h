#pragma once

#include <cstddef>
#include <span>

#include "vexo/base/error.h"
#include "vexo/coro/task.h"
#include "vexo/net/channel.h"
#include "vexo/net/event_loop.h"
#include "vexo/net/socket.h"
#include "vexo/utils/macros.h"

namespace vexo::net {

class ReactorStream {
public:
  VEXO_DISABLE_COPY_ALLOW_MOVE(ReactorStream);
  ReactorStream(EventLoop* loop, int fd);
  ~ReactorStream();

  coro::Task<base::Result<std::size_t>> ReadSome(std::span<std::byte> buffer);
  coro::Task<base::Result<std::size_t>> WriteSome(std::span<const std::byte> buffer);
  coro::Task<base::Result<void>> Shutdown();
  coro::Task<base::Result<void>> Close();

private:
  class ReadAwaiter;
  class WriteAwaiter;

  void HandleRead(vexo::time::Timestamp receive_time);
  void HandleWrite();
  void HandleClose();
  void HandleError();

  void CompleteRead(base::Result<std::size_t> result);
  void CompleteWrite(base::Result<std::size_t> result);
  void DetachChannel();

  EventLoop* loop_;
  Socket socket_;
  Channel channel_;
  ReadAwaiter* pending_read_{nullptr};
  WriteAwaiter* pending_write_{nullptr};
  bool closed_{false};
};
}  // namespace vexo::net
