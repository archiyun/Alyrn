#include "runtime/net/select.h"
#include "runtime/net/channel.h"
#include "runtime/log/logger.h"

#include <cassert>
#include <cerrno>
#include <cstring>

namespace runtime::net {

SelectPoller::SelectPoller(EventLoop* loop) : Poller(loop) {
  FD_ZERO(&read_fds_);
  FD_ZERO(&write_fds_);
  FD_ZERO(&error_fds_);
}

runtime::time::Timestamp SelectPoller::Poll(int timeout_ms,
                                            ChannelList* active_channels) {
  // select() 的 fd_set 是 in/out 参数：调用后被内核覆写为就绪集合。
  // 必须每次从原始兴趣集拷贝临时副本，避免兴趣集被破坏。
  fd_set read_ready  = read_fds_;
  fd_set write_ready = write_fds_;
  fd_set error_ready = error_fds_;

  struct timeval tv{};
  tv.tv_sec  = timeout_ms / 1000;
  tv.tv_usec = (timeout_ms % 1000) * 1000;

  const int num_events = ::select(max_fd_ + 1,
                                  &read_ready,
                                  &write_ready,
                                  &error_ready,
                                  &tv);
  const int saved_errno = errno;
  const auto now = runtime::time::Timestamp::Now();

  if (num_events > 0) {
    FillActiveChannels(read_ready, write_ready, error_ready, active_channels);
  } else if (num_events < 0 && saved_errno != EINTR) {
    errno = saved_errno;
    LOG_ERROR() << "select failed: errno=" << saved_errno
                << " message=" << std::strerror(saved_errno);
  }

  return now;
}

void SelectPoller::FillActiveChannels(const fd_set& read_ready,
                                      const fd_set& write_ready,
                                      const fd_set& error_ready,
                                      ChannelList* active_channels) const {
  // select 没有像 poll 的 revents 字段，必须遍历所有注册的 channel，
  // 逐一用 FD_ISSET 查询就绪位图。
  for (const auto& [fd, channel] : channels_) {
    int ev = 0;
    if (FD_ISSET(fd, &read_ready))  ev |= Channel::kReadEvent;
    if (FD_ISSET(fd, &write_ready)) ev |= Channel::kWriteEvent;
    if (FD_ISSET(fd, &error_ready)) ev |= Channel::kReadEvent;  // OOB → read 回调
    if (ev == 0) continue;
    channel->SetRevents(ev);
    active_channels->push_back(channel);
  }
}

void SelectPoller::UpdateChannel(Channel* channel) {
  const int fd = channel->Fd();

  // select 有硬性上限 FD_SETSIZE（通常 1024），超出无法监听
  if (fd >= FD_SETSIZE) {
    LOG_ERROR() << "fd=" << fd << " >= FD_SETSIZE=" << FD_SETSIZE
                << ", SelectPoller cannot monitor this fd";
    return;
  }

  if (channel->Index() < 0) {
    // 新 channel：加入 channels_ map，Index() 置为 fd（注册标志）
    assert(channels_.find(fd) == channels_.end());
    channels_[fd] = channel;
    channel->SetIndex(fd);
  }

  // 根据当前兴趣集同步三个 fd_set
  if (channel->Events() & Channel::kReadEvent) {
    FD_SET(fd, &read_fds_);
  } else {
    FD_CLR(fd, &read_fds_);
  }

  if (channel->Events() & Channel::kWriteEvent) {
    FD_SET(fd, &write_fds_);
  } else {
    FD_CLR(fd, &write_fds_);
  }

  // 有任何兴趣事件时就监听 OOB；全部禁用时清除
  if (!channel->IsNoneEvent()) {
    FD_SET(fd, &error_fds_);
  } else {
    FD_CLR(fd, &error_fds_);
  }

  if (fd > max_fd_) {
    max_fd_ = fd;
  }
}

void SelectPoller::RemoveChannel(Channel* channel) {
  assert(channel->IsNoneEvent());
  const int fd = channel->Fd();
  assert(channels_.find(fd) != channels_.end());

  FD_CLR(fd, &read_fds_);
  FD_CLR(fd, &write_fds_);
  FD_CLR(fd, &error_fds_);
  channels_.erase(fd);
  channel->SetIndex(-1);

  // 若删除的是最大 fd，重新扫描找新的最大值
  if (fd == max_fd_) {
    max_fd_ = -1;
    for (const auto& [key, _] : channels_) {
      if (key > max_fd_) max_fd_ = key;
    }
  }
}

}  // namespace runtime::net
