// Copyright (c) 2026 Aresna
// SPDX-License-Identifier: MIT
#include "runtime/net/tcp_connection.h"
#include "runtime/net/channel.h"
#include "runtime/net/event_loop.h"
#include "runtime/net/net_assert.h"
#include "runtime/net/socket.h"
#include "runtime/log/logger.h"

#ifdef RUNTIME_ENABLE_SSL
#include <openssl/ssl.h>
#endif
#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstring>
#include <string_view>
#include <sys/socket.h>
#include <unistd.h>

namespace runtime::net {

namespace {
// Lifetime counters scoped to upstream (proxy-side) TcpConnection only.
// Used to verify the upstream connection pool actually reuses sockets
// instead of silently rebuilding one per request.
std::atomic<uint64_t> g_upstream_ctor_count{0};
std::atomic<uint64_t> g_upstream_dtor_count{0};

bool IsUpstreamName(std::string_view name) {
  return name.size() >= 7 && name.substr(0, 7) == "proxy->";
}
}  // namespace

uint64_t TcpConnection::UpstreamCtorCount() {
  return g_upstream_ctor_count.load(std::memory_order_relaxed);
}
uint64_t TcpConnection::UpstreamDtorCount() {
  return g_upstream_dtor_count.load(std::memory_order_relaxed);
}

TcpConnection::TcpConnection(EventLoop* loop, const std::string& name,
                             int sockfd, const InetAddress& local_addr,
                             const InetAddress& peer_addr)
    : loop_(loop), name_(name), state_(TCPState::kConnecting),
      socket_(std::make_unique<Socket>(sockfd)),
      channel_(std::make_unique<Channel>(loop, sockfd)),
      local_addr_(local_addr), peer_addr_(peer_addr) {
  RUNTIME_ASSERT(loop_ != nullptr, "TcpConnection: loop must not be null");
  RUNTIME_ASSERT(sockfd >= 0, "TcpConnection: sockfd must be valid");
  if (IsUpstreamName(name_)) {
    g_upstream_ctor_count.fetch_add(1, std::memory_order_relaxed);
  }
  channel_->SetReadCallback([this](runtime::time::Timestamp receive_time) {
    HandleRead(receive_time);
  });
  channel_->SetWriteCallback([this] { HandleWrite(); });
  channel_->SetErrorCallback([this] { HandleError(); });
  channel_->SetCloseCallback([this] { HandleClose(); });
}

TcpConnection::~TcpConnection() {
  if (IsUpstreamName(name_)) {
    g_upstream_dtor_count.fetch_add(1, std::memory_order_relaxed);
  }
#ifdef RUNTIME_ENABLE_SSL
  if (ssl_) SSL_free(ssl_);
#endif
}

void TcpConnection::SetEdgeTriggered(bool et) {
  channel_->SetEdgeTriggered(et);
}

bool TcpConnection::Send(const std::string& message) {
  return Send(message.data(), message.size());
}

bool TcpConnection::Send(std::string_view message) {
  return Send(message.data(), message.size());
}

bool TcpConnection::Send(const void* data, std::size_t len) {
  if (state_ != TCPState::kConnected) {
    return false;
  }
  if (loop_->IsInLoopThread()) {
    SendInLoop(data, len);
  } else {
    auto self = shared_from_this();
    std::string copy(static_cast<const char*>(data), len);
    loop_->RunInLoop([self, copy = std::move(copy)] { self->SendInLoop(copy.data(), copy.size());});
  }
  return true;
}

void TcpConnection::SendInLoop(const std::string& message) {
  SendInLoop(message.data(), message.size());
}


void TcpConnection::SendInLoop(const void* data, std::size_t len) {
  RUNTIME_ASSERT(loop_->IsInLoopThread(),
                 "TcpConnection::SendInLoop called from wrong thread");
  if (state_ == TCPState::kDisconnected)
    return;

  ssize_t nwrote = 0;
  if (!channel_->IsWriting() && output_buffer_.ReadableBytes() == 0) {
#ifdef RUNTIME_ENABLE_SSL
    if (ssl_) {
      nwrote = SSL_write(ssl_, data, static_cast<int>(len));
      if (nwrote < 0) {
        const int ssl_err = SSL_get_error(ssl_, static_cast<int>(nwrote));
        nwrote = 0;
        if (ssl_err != SSL_ERROR_WANT_WRITE) {
          LOG_ERROR() << "ssl direct write failed: name=" << name_
                      << " fd=" << channel_->Fd() << " ssl_err=" << ssl_err;
          return;
        }
      }
    } else {
#endif
      nwrote = ::write(channel_->Fd(), data, len);
      if (nwrote < 0) {
        nwrote = 0;
        if (errno != EWOULDBLOCK) {
          LOG_ERROR() << "tcp direct write failed: name=" << name_
                      << " fd=" << channel_->Fd() << " errno=" << errno
                      << " message=" << std::strerror(errno);
          return;
        }
      }
#ifdef RUNTIME_ENABLE_SSL
    }
#endif
  }

  if (static_cast<std::size_t>(nwrote) < len) {
    const std::size_t old_len = output_buffer_.ReadableBytes();
    output_buffer_.Append(static_cast<const char*>(data) + nwrote,len - nwrote);
    const std::size_t new_len = output_buffer_.ReadableBytes();
    if (old_len < high_water_mark_ && new_len >= high_water_mark_ &&
        high_water_mark_callback_) {
      auto self = shared_from_this();
      loop_->QueueInLoop([self, new_len] {
        if (self->high_water_mark_callback_) {
          self->high_water_mark_callback_(self, new_len);
        }
      });
    }
    if (!channel_->IsWriting()) {
      channel_->EnableWriting();
    }
  } else {
    if (write_complete_callback_) {
      write_complete_callback_(shared_from_this());
    }
  }
}

void TcpConnection::SetTcpNoDelay(bool on) {
  socket_->SetTcpNoDelay(on);
}

void TcpConnection::Shutdown() {
  if (state_ == TCPState::kConnected) {
    SetState(TCPState::kDisconnecting);
    auto self = shared_from_this();
    loop_->RunInLoop([self] { self->ShutdownInLoop(); });
  }
}

void TcpConnection::ConnectEstablished() {
  RUNTIME_ASSERT(loop_->IsInLoopThread(),
                 "TcpConnection::ConnectEstablished called from wrong thread");
  RUNTIME_ASSERT(state_ == TCPState::kConnecting,
                 "TcpConnection::ConnectEstablished: expected kConnecting state");
  SetState(TCPState::kConnected);
  channel_->Tie(shared_from_this());
  channel_->EnableReading();

  LOG_DEBUG() << "tcp connection established: name=" << name_
              << " local=" << local_addr_.ToIpPort()
              << " peer=" << peer_addr_.ToIpPort();

  if (connection_callback_) {
    connection_callback_(shared_from_this());
  }
}

void TcpConnection::ConnectDestroyed() {
  RUNTIME_ASSERT(loop_->IsInLoopThread(),
                 "TcpConnection::ConnectDestroyed called from wrong thread");
  const bool notify_state_change = state_ != TCPState::kDisconnected;
  if (notify_state_change) {
    SetState(TCPState::kDisconnected);
    channel_->DisableAll();
  }

  LOG_DEBUG() << "tcp connection destroyed: name=" << name_
              << " peer=" << peer_addr_.ToIpPort();

  if (notify_state_change && connection_callback_) {
    connection_callback_(shared_from_this());
  }

  channel_->Remove();
}

void TcpConnection::HandleRead(runtime::time::Timestamp receive_time) {
#ifdef RUNTIME_ENABLE_SSL
  if (ssl_ && !ssl_handshake_done_) {
    DoSslHandshake();
    return;
  }
#endif

  int saved_errno = 0;
  int fd = channel_->Fd();

  if (channel_->IsEdgeTriggered()) {
    // ET: drain until EAGAIN (plain) or WANT_READ (SSL).
    while (true) {
#ifdef RUNTIME_ENABLE_SSL
      ssize_t n = ssl_ ? input_buffer_.ReadSslFd(ssl_, &saved_errno)
                       : input_buffer_.ReadFd(fd, &saved_errno);
#else
      ssize_t n = input_buffer_.ReadFd(fd, &saved_errno);
#endif
      if (n > 0) {
        continue;
      } else if (n == 0) {
        HandleClose();
        return;
      } else {
#ifdef RUNTIME_ENABLE_SSL
        if (ssl_) {
          if (saved_errno == SSL_ERROR_WANT_READ) break;
          if (saved_errno == SSL_ERROR_WANT_WRITE) {
            channel_->EnableWriting();
            break;
          }
        } else
#endif
        if (saved_errno == EAGAIN || saved_errno == EWOULDBLOCK) {
          break;
        }
        HandleError();
        return;
      }
    }
    if (input_buffer_.ReadableBytes() > 0 && message_callback_) {
      message_callback_(shared_from_this(), input_buffer_, receive_time);
    }
  } else {
    // LT: one read per event is sufficient.
#ifdef RUNTIME_ENABLE_SSL
    ssize_t n = ssl_ ? input_buffer_.ReadSslFd(ssl_, &saved_errno)
                     : input_buffer_.ReadFd(fd, &saved_errno);
#else
    ssize_t n = input_buffer_.ReadFd(fd, &saved_errno);
#endif
    if (n > 0) {
      if (message_callback_) {
        message_callback_(shared_from_this(), input_buffer_, receive_time);
      }
    } else if (n == 0) {
      HandleClose();
    } else {
#ifdef RUNTIME_ENABLE_SSL
      const bool transient = ssl_
          ? (saved_errno == SSL_ERROR_WANT_READ || saved_errno == SSL_ERROR_WANT_WRITE)
          : (saved_errno == EAGAIN || saved_errno == EWOULDBLOCK);
#else
      const bool transient = (saved_errno == EAGAIN || saved_errno == EWOULDBLOCK);
#endif
      if (!transient) {
        LOG_ERROR() << "tcp read failed: name=" << name_
                    << " fd=" << channel_->Fd() << " errno=" << saved_errno;
        HandleError();
      }
    }
  }
}

void TcpConnection::HandleWrite() {
#ifdef RUNTIME_ENABLE_SSL
  // Handshake may need to write (e.g. ServerHello); retry it here.
  if (ssl_ && !ssl_handshake_done_) {
    DoSslHandshake();
    return;
  }
#endif

  if (!channel_->IsWriting()) {
    return;
  }

  int saved_errno = 0;

  auto do_write = [&]() -> ssize_t {
#ifdef RUNTIME_ENABLE_SSL
    return ssl_ ? output_buffer_.WriteSslFd(ssl_, &saved_errno)
                : output_buffer_.WriteFd(channel_->Fd(), &saved_errno);
#else
    return output_buffer_.WriteFd(channel_->Fd(), &saved_errno);
#endif
  };

  auto is_transient = [&]() -> bool {
#ifdef RUNTIME_ENABLE_SSL
    return ssl_
        ? (saved_errno == SSL_ERROR_WANT_WRITE || saved_errno == SSL_ERROR_WANT_READ)
        : (saved_errno == EAGAIN || saved_errno == EWOULDBLOCK);
#else
    return (saved_errno == EAGAIN || saved_errno == EWOULDBLOCK);
#endif
  };

  if (channel_->IsEdgeTriggered()) {
    while (output_buffer_.ReadableBytes() > 0) {
      ssize_t n = do_write();
      if (n < 0) {
        if (is_transient()) break;
        LOG_ERROR() << "tcp write failed: name=" << name_
                    << " fd=" << channel_->Fd() << " errno=" << saved_errno;
        HandleError();
        return;
      }
    }
    if (output_buffer_.ReadableBytes() == 0) {
      channel_->DisableWriting();
      if (write_complete_callback_) write_complete_callback_(shared_from_this());
      if (state_ == TCPState::kDisconnecting) ShutdownInLoop();
    }
  } else {
    ssize_t n = do_write();
    if (n > 0) {
      if (output_buffer_.ReadableBytes() == 0) {
        channel_->DisableWriting();
        if (write_complete_callback_) write_complete_callback_(shared_from_this());
        if (state_ == TCPState::kDisconnecting) ShutdownInLoop();
      }
    } else if (n < 0 && !is_transient()) {
      LOG_ERROR() << "tcp write failed: name=" << name_
                  << " fd=" << channel_->Fd() << " errno=" << saved_errno;
      HandleError();
    }
  }
}

void TcpConnection::HandleClose() {
  SetState(TCPState::kDisconnected);
  channel_->DisableAll();

  LOG_DEBUG() << "tcp connection closed: name=" << name_
              << " peer=" << peer_addr_.ToIpPort();

  TcpConnectionPtr guard(shared_from_this());
  if (connection_callback_) {
    connection_callback_(guard);
  }
  if (close_callback_) {
    close_callback_(guard);
  }
}

void TcpConnection::HandleError() {
  int err = 0;
  socklen_t len = static_cast<socklen_t>(sizeof(err));
  ::getsockopt(channel_->Fd(), SOL_SOCKET, SO_ERROR, &err, &len);
  LOG_ERROR() << "tcp connection socket error: name=" << name_
              << " fd=" << channel_->Fd() << " error=" << err
              << " message=" << std::strerror(err);
}

void TcpConnection::ShutdownInLoop() {
  RUNTIME_ASSERT(loop_->IsInLoopThread(),
                 "TcpConnection::ShutdownInLoop called from wrong thread");
  if (!channel_->IsWriting()) {
    socket_->ShutdownWrite();
  }
}

#ifdef RUNTIME_ENABLE_SSL
void TcpConnection::SetSsl(SSL* ssl) {
  ssl_ = ssl;
  SSL_set_fd(ssl_, channel_->Fd());
  SSL_set_accept_state(ssl_); 
}

void TcpConnection::DoSslHandshake() {
  const int ret = SSL_accept(ssl_);
  if (ret == 1) {
    ssl_handshake_done_ = true;
    const unsigned char* proto = nullptr;
    unsigned int proto_len = 0;
    SSL_get0_alpn_selected(ssl_, &proto, &proto_len);
    const std::string negotiated(proto ? reinterpret_cast<const char*>(proto) : "http/1.1",
                                 proto ? proto_len : 8);
    if (handshake_cb_) handshake_cb_(negotiated);
    return;
  }

  const int err = SSL_get_error(ssl_, ret);
  if (err == SSL_ERROR_WANT_READ) {
    channel_->EnableReading();
  } else if (err == SSL_ERROR_WANT_WRITE) {
    channel_->EnableWriting();
  } else {
    HandleError();
  }
}
#endif

}  // namespace runtime::net
