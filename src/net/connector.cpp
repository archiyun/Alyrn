#include "runtime/net/connector.h"
#include "runtime/log/logger.h"
#include "runtime/net/channel.h"
#include "runtime/net/net_utils.h"

#include <cassert>
#include <cerrno>
#include <algorithm>
#include <cstring>
#include <sys/socket.h>
#include <unistd.h>

namespace runtime::net {

// 指数退避上限 30s，与 nginx upstream_connect_timeout 量级一致
static constexpr double kMaxRetryDelaySec = 30.0;

Connector::Connector(EventLoop* loop, const InetAddress& server_addr)
    : loop_(loop), server_addr_(server_addr) {}

Connector::~Connector() {
  // 正常结束时 channel_ 必须已被清除（fd 已交给 TcpConnection）
  assert(!channel_);
}

void Connector::Start() {
  // Start() 可从任意线程调用，用 RunInLoop 保证进入 loop 线程
  auto self = shared_from_this();
  loop_->RunInLoop([self] { self->StartInLoop(); });
}

void Connector::Stop() {
  auto self = shared_from_this();
  loop_->RunInLoop([self] {
    if (self->state_ == State::kConnecting) {
      self->state_ = State::kDisConnected;
      int sockfd = self->RemoveAndResetChannel();
      ::close(sockfd);  // Stop 时彻底关闭，不重试
    }
  });
}

void Connector::StartInLoop() {
  assert(loop_->IsInLoopThread());
  assert(state_ == State::kDisConnected);
  Connect();
}

void Connector::Connect() {
  // 每次尝试都创建新 socket（Retry 会关掉旧的）
  int sockfd = CreateNonBlockingSocket();
  if (sockfd < 0) {
    LOG_ERROR() << "connector: CreateNonBlockingSocket failed";
    return;
  }

  const sockaddr_in addr = server_addr_.GetSockAddr();
  int ret = ::connect(sockfd,
                      reinterpret_cast<const sockaddr*>(&addr),
                      static_cast<socklen_t>(sizeof(addr)));
  int err = (ret == 0) ? 0 : errno;

  switch (err) {
    case 0:
    case EINPROGRESS:  // 非阻塞 connect 的标准路径，握手进行中
    case EINTR:
    case EISCONN:      // 极少见：已连接
      Connecting(sockfd);
      break;

    // 可重试的临时错误
    case EAGAIN:
    case EADDRINUSE:
    case EADDRNOTAVAIL:
    case ECONNREFUSED:
    case ENETUNREACH:
      Retry(sockfd);
      break;

    // 不可重试的严重错误
    default:
      LOG_ERROR() << "connector: connect to " << server_addr_.ToIpPort()
                  << " failed: " << std::strerror(err);
      ::close(sockfd);
      break;
  }
}

void Connector::Connecting(int sockfd) {
  state_ = State::kConnecting;
  assert(!channel_);

  channel_ = std::make_unique<Channel>(loop_, sockfd);

  // 只关注 EPOLLOUT：connect 完成（成功或失败）时触发
  // 不需要 EPOLLIN，握手期间没有数据
  auto self = shared_from_this();
  channel_->SetWriteCallback([self] { self->handlerWrite(); });
  channel_->SetErrorCallback([self] { self->handleError(); });
  channel_->EnableWriting();  // 向 epoll 注册 EPOLLOUT
}

void Connector::handlerWrite() {
  assert(loop_->IsInLoopThread());

  if (state_ != State::kConnecting) {
    return;  // Stop() 在回调触发前已被调用
  }

  // 必须先从 epoll 摘除 channel，再把 fd 交给 TcpConnection
  // 否则同一个 fd 会有两个 Channel 注册到 epoll，行为未定义
  int sockfd = RemoveAndResetChannel();

  // getsockopt(SO_ERROR) 是判断 connect 是否真正成功的唯一可靠方式
  // EPOLLOUT 触发只说明"可写"，不代表连接成功
  int err = 0;
  socklen_t len = static_cast<socklen_t>(sizeof(err));
  if (::getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &err, &len) < 0) {
    err = errno;  // getsockopt 本身失败
  }

  if (err != 0) {
    LOG_WARN() << "connector: connect to " << server_addr_.ToIpPort()
               << " failed: " << std::strerror(err);
    Retry(sockfd);
    return;
  }

  // 自连接检测：在 loopback 上，内核可能把本地端口分配成与目标端口相同
  // 导致 connect 到自己，这种连接没有意义
  if (IsSelfConnect(sockfd)) {
    LOG_WARN() << "connector: self-connect on " << server_addr_.ToIpPort()
               << ", retrying";
    Retry(sockfd);
    return;
  }

  state_ = State::kConnected;
  retry_delay_sec_ = 0.5;  // 重置退避计时器，为下次重用做准备

  LOG_INFO() << "connector: connected to " << server_addr_.ToIpPort()
             << " fd=" << sockfd;

  if (new_connection_cb_) {
    new_connection_cb_(sockfd);  // 把 fd 交给 TcpClient 创建 TcpConnection
  } else {
    ::close(sockfd);
  }
}

void Connector::handleError() {
  assert(loop_->IsInLoopThread());

  if (state_ == State::kConnecting) {
    int sockfd = RemoveAndResetChannel();
    int err = 0;
    socklen_t len = static_cast<socklen_t>(sizeof(err));
    ::getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &err, &len);
    LOG_ERROR() << "connector: error event on " << server_addr_.ToIpPort()
                << ": " << std::strerror(err);
    Retry(sockfd);
  }
}

void Connector::Retry(int sockfd) {
  ::close(sockfd);
  state_ = State::kDisConnected;

  if (!new_connection_cb_) {
    return;  // TcpClient 已析构，不再重试
  }

  // 指数退避：0.5 → 1 → 2 → 4 → ... → 30s
  retry_delay_sec_ = std::min(retry_delay_sec_ * 2.0, kMaxRetryDelaySec);

  LOG_INFO() << "connector: retry " << server_addr_.ToIpPort()
             << " in " << retry_delay_sec_ << "s";

  auto self = shared_from_this();
  loop_->RunAfter(retry_delay_sec_, [self] {
    // RunAfter 回调时再次确认状态，因为 Stop() 可能在等待期间被调用
    if (self->state_ == State::kDisConnected && self->new_connection_cb_) {
      self->Connect();
    }
  });
}

int Connector::RemoveAndResetChannel() {
  channel_->DisableAll();   // 清零 epoll 关注事件
  channel_->Remove();       // 从 epoll 删除这个 fd

  int sockfd = channel_->Fd();

  // 不能在 Channel 的回调执行期间直接 reset channel_（会析构自己）
  // 用 QueueInLoop 推迟到本轮事件分发结束后再清理
  auto self = shared_from_this();
  loop_->QueueInLoop([self] { self->channel_.reset(); });

  return sockfd;
}

}  // namespace runtime::net
