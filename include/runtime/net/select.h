#pragma once

#include "runtime/net/poller.h"

#include <sys/select.h>

namespace runtime::net {

// SelectPoller is a Poller implementation backed by select(2).
//
// 与 PollPoller 相比的关键差异：
//   - 兴趣集用三个独立的 fd_set 位图表示（读/写/异常），而非结构体数组
//   - select() 的 fd_set 是 in/out 参数，每次调用前必须从原始兴趣集拷贝临时副本
//   - 需要维护 max_fd_：select() 第一个参数 = max_fd_ + 1
//   - fd 上限为 FD_SETSIZE（通常 1024），超出则拒绝注册
//
// Channel::Index() 在此后端仅用作注册标志：-1 表示未注册，>= 0 表示已注册。
class SelectPoller : public Poller {
 public:
  explicit SelectPoller(EventLoop* loop);
  ~SelectPoller() override = default;

  runtime::time::Timestamp Poll(int timeout_ms,
                                ChannelList* active_channels) override;
  void UpdateChannel(Channel* channel) override;
  void RemoveChannel(Channel* channel) override;

 private:
  // 遍历 channels_，用 FD_ISSET 检查三个就绪集，收集活跃 Channel。
  // 参数为 Poll() 内部的临时拷贝（select 调用后已被内核覆写为就绪集）。
  void FillActiveChannels(const fd_set& read_ready,
                          const fd_set& write_ready,
                          const fd_set& error_ready,
                          ChannelList* active_channels) const;

  fd_set read_fds_;    // 关注可读的原始兴趣集
  fd_set write_fds_;   // 关注可写的原始兴趣集
  fd_set error_fds_;   // 关注 OOB 的原始兴趣集，就绪时映射为 kReadEvent
  int max_fd_{-1};     // 已注册 fd 的最大值；select() nfds = max_fd_ + 1
};

}  // namespace runtime::net
