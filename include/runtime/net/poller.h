#pragma once

#include "runtime/base/noncopyable.h"
#include "runtime/time/timestamp.h"

#include <unordered_map>
#include <vector>

namespace runtime::net {
    
class Channel;
class EventLoop;

// 多路IO复用器的用户态抽象, Reactor线程底层事件内核代理
/**
 *  EventLoop
 *      |__->Poller
 *             |_>channels_ // 管理哪些Channel？
 *             |_>Poll()   // 去问内核: 谁活跃
 *             |_>UpdateChannel() // 某个Channel的关注事件变了，去更新
 *             |_>RemoveChannel() // 某个Channel 不玩了，别监视了 删。
 */
class Poller : public runtime::base::NonCopyable {
public:
    using ChannelList = std::vector<Channel*>;

    explicit Poller(EventLoop *loop);
    // 基类析构函数写成虚函数
    virtual ~Poller() = default;

    // 给所有IO复用保留统一的接口 纯虚函数和抽象基类
    virtual runtime::time::Timestamp Poll(int timeout_ms, ChannelList *active_channels) = 0;
    virtual void UpdateChannel(Channel *channel) = 0;
    virtual void RemoveChannel(Channel *channel) = 0;

    // 判断参数Channel在当前Poller当中
    bool HasChannel(Channel *channel) const;

    // EventLoop通过该接口获取默认的IO复用的具体实现
    // 工厂模式: Poller决定在当前平台上创建具体实现
    static Poller* NewDefaultPoller(EventLoop *loop);

protected:
    // key: fd -> value: Channel
    using ChannelMap = std::unordered_map<int, Channel*>;
    ChannelMap channels_;
private:
    // one loop per thread 
    EventLoop *ownerLoop_;
};

}   // namespace runtime::net
