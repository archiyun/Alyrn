#include "runtime/net/tcp_connection.h"
#include "runtime/net/event_loop.h"
#include "runtime/log/logger.h"

#include <openssl/ssl.h>
#include <cassert>
#include <cerrno>
#include <cstring>
#include <sys/socket.h>
#include <unistd.h>

namespace runtime::net {

TcpConnection::TcpConnection(EventLoop* loop, const std::string& name,
                             int sockfd, const InetAddress& local_addr,
                             const InetAddress& peer_addr)
    : loop_(loop), name_(name), state_(TCPState::kConnecting),
      socket_(std::make_unique<Socket>(sockfd)),
      channel_(std::make_unique<Channel>(loop, sockfd)),
      local_addr_(local_addr), peer_addr_(peer_addr) {
  channel_->SetReadCallback([this](runtime::time::Timestamp receive_time) {
    HandleRead(receive_time);
  });
  channel_->SetWriteCallback([this] { HandleWrite(); });
  channel_->SetErrorCallback([this] { HandleError(); });
  channel_->SetCloseCallback([this] { HandleClose(); });
}

TcpConnection::~TcpConnection() {
  if (ssl_) SSL_free(ssl_);
}

void TcpConnection::Send(const std::string& message) {
  if (state_ == TCPState::kConnected) {
    if (loop_->IsInLoopThread()) {
      SendInLoop(message);
    } else {
      auto self = shared_from_this();
      loop_->RunInLoop([self, message] { self->SendInLoop(message); });
    }
  } else {
    LOG_WARN() << "send ignored on disconnected connection: name=" << name_;
  }
}

void TcpConnection::SendInLoop(const std::string& message) {
  if (state_ == TCPState::kDisconnected)
    return;

  ssize_t nwrote = 0;
  if (!channel_->IsWriting() && output_buffer_.ReadableBytes() == 0) {
    if (ssl_) {
      nwrote = SSL_write(ssl_, message.data(), static_cast<int>(message.size()));
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
      nwrote = ::write(channel_->Fd(), message.data(), message.size());
      if (nwrote < 0) {
        nwrote = 0;
        if (errno != EWOULDBLOCK) {
          LOG_ERROR() << "tcp direct write failed: name=" << name_
                      << " fd=" << channel_->Fd() << " errno=" << errno
                      << " message=" << std::strerror(errno);
          return;
        }
      }
    }
  }

  if (static_cast<std::size_t>(nwrote) < message.size()) {
    output_buffer_.Append(message.data() + nwrote, message.size() - nwrote);
    if (!channel_->IsWriting()) {
      channel_->EnableWriting();
    }
  } else {
    if (write_complete_callback_) {
      write_complete_callback_(shared_from_this());
    }
  }
}

void TcpConnection::Shutdown() {
  if (state_ == TCPState::kConnected) {
    SetState(TCPState::kDisconnecting);
    auto self = shared_from_this();
    loop_->RunInLoop([self] { self->ShutdownInLoop(); });
  }
}

void TcpConnection::ConnectEstablished() {
  SetState(TCPState::kConnected);
  channel_->Tie(shared_from_this());
  channel_->EnableReading();

  LOG_INFO() << "tcp connection established: name=" << name_
             << " local=" << local_addr_.ToIpPort()
             << " peer=" << peer_addr_.ToIpPort();

  if (connection_callback_) {
    connection_callback_(shared_from_this());
  }
}

void TcpConnection::ConnectDestroyed() {
  const bool notify_state_change = state_ != TCPState::kDisconnected;
  if (notify_state_change) {
    SetState(TCPState::kDisconnected);
    channel_->DisableAll();
  }

  LOG_INFO() << "tcp connection destroyed: name=" << name_
             << " peer=" << peer_addr_.ToIpPort();

  if (notify_state_change && connection_callback_) {
    connection_callback_(shared_from_this());
  }

  channel_->Remove();
}

void TcpConnection::HandleRead(runtime::time::Timestamp receive_time) {
  if (ssl_ && !ssl_handshake_done_) {
    DoSslHandshake();
    return;
  }

  int saved_errno = 0;
  int fd = channel_->Fd();

  if (channel_->IsEdgeTriggered()) {
    // ET: drain until EAGAIN (plain) or WANT_READ (SSL).
    while (true) {
      ssize_t n = ssl_ ? input_buffer_.ReadSslFd(ssl_, &saved_errno)
                       : input_buffer_.ReadFd(fd, &saved_errno);
      if (n > 0) {
        continue;
      } else if (n == 0) {
        HandleClose();
        return;
      } else {
        if (ssl_) {
          if (saved_errno == SSL_ERROR_WANT_READ) break;
          if (saved_errno == SSL_ERROR_WANT_WRITE) {
            channel_->EnableWriting();
            break;
          }
        } else if (saved_errno == EAGAIN || saved_errno == EWOULDBLOCK) {
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
    ssize_t n = ssl_ ? input_buffer_.ReadSslFd(ssl_, &saved_errno)
                     : input_buffer_.ReadFd(fd, &saved_errno);
    if (n > 0) {
      if (message_callback_) {
        message_callback_(shared_from_this(), input_buffer_, receive_time);
      }
    } else if (n == 0) {
      HandleClose();
    } else {
      const bool transient = ssl_
          ? (saved_errno == SSL_ERROR_WANT_READ || saved_errno == SSL_ERROR_WANT_WRITE)
          : (saved_errno == EAGAIN || saved_errno == EWOULDBLOCK);
      if (!transient) {
        LOG_ERROR() << "tcp read failed: name=" << name_
                    << " fd=" << channel_->Fd() << " errno=" << saved_errno;
        HandleError();
      }
    }
  }
}

void TcpConnection::HandleWrite() {
  // Handshake may need to write (e.g. ServerHello); retry it here.
  if (ssl_ && !ssl_handshake_done_) {
    DoSslHandshake();
    return;
  }

  if (!channel_->IsWriting()) {
    return;
  }

  int saved_errno = 0;

  auto do_write = [&]() -> ssize_t {
    return ssl_ ? output_buffer_.WriteSslFd(ssl_, &saved_errno)
                : output_buffer_.WriteFd(channel_->Fd(), &saved_errno);
  };

  auto is_transient = [&]() -> bool {
    return ssl_
        ? (saved_errno == SSL_ERROR_WANT_WRITE || saved_errno == SSL_ERROR_WANT_READ)
        : (saved_errno == EAGAIN || saved_errno == EWOULDBLOCK);
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

  LOG_INFO() << "tcp connection closed: name=" << name_
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
  if (!channel_->IsWriting()) {
    socket_->ShutdownWrite();
  }
}

// attached SSL
void TcpConnection::SetSsl(SSL* ssl) {
  ssl_ = ssl;
  SSL_set_fd(ssl_, channel_->Fd());
  SSL_set_accept_state(ssl_); // 服务端模式
}

void TcpConnection::DoSslHandshake() {
  const int ret = SSL_accept(ssl_);
  if (ret == 1) {
    // 握手完成， 查询 ALPN 协商结果
    ssl_handshake_done_ = true;
    const unsigned char* proto = nullptr;
    unsigned int proto_len = 0;
    SSL_get0_alpn_selected(ssl_, &proto, &proto_len);
    const std::string negotiated(proto ? reinterpret_cast<const char*>(proto) : "http/1.1", 
                                proto ? proto_len : 8);
    if (handshake_cb_) handshake_cb_(negotiated);
    return;
  }

  const int err =  SSL_get_error(ssl_, ret);
  if (err == SSL_ERROR_WANT_READ) {
    channel_->EnableReading();
  } else if (err == SSL_ERROR_WANT_WRITE) {
    channel_->EnableWriting();
  } else {
    HandleError();
  }
}
}  // namespace runtime::net
