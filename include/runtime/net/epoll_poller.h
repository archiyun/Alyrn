#pragma once

#include "runtime/net/poller.h"

#include <sys/epoll.h>
#include <vector>

namespace runtime::net {
/**
 * epoll 的使用
 * epoll_create;
 * epoll_ctl; add/mod/del
 * epoll_wait;
 */

 // Poller 对 Linux/epoll的具体实现
 // 对下： 和内核打交道把fd注册到epoll
 // 对上:  把活跃事件的Channel提供给Channel
class EPollPoller : public Poller {
public:
    explicit EPollPoller(EventLoop *loop);
    ~EPollPoller() override ;

    // 重写基类的抽象方法
    runtime::time::Timestamp Poll(int timeout_ms, ChannelList *active_channels) override;
    void UpdateChannel(Channel *channel) override;
    void RemoveChannel(Channel *channel) override;
private:
    void Update(int operation, Channel *channel);
    void FillActiveChannels(int num_events, ChannelList *active_channels) const;
private:
    // 初始默认 16 epoll_event, 满了字符翻倍
    static constexpr int kInitEventListSize = 16;
    
    int epollfd_;
    std::vector<epoll_event> events_;
};

}   // namespace runtime::net

/**
 *  调用链
 *  > 注册事件
 *  Channel::EnableReading()
 *         -> Channel::Update()
 *         -> EventLoop::UpdateChannel(channel)
 *         -> EPollPoller::UpdateChannel(channel)
 *         -> epoll_ctl(...)
 *  > 等待事件
 *  EventLoop::Loop()
 *         -> EPoller::Poll(timeout, &activeChannels)
 *         -> epoll_wait(...)
 *         -> FillActiveChannels(...)
 *         -> EventLoop for ... in activeChannels
 *         -> Channel::HandleEvent(...)
 */
