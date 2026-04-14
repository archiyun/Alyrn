#include "runtime/net/socket.h"
#include "runtime/log/logger.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

namespace runtime::net {

Socket::Socket(int sockfd)
    : sockfd_(sockfd) {}

Socket::~Socket() {
    ::close(sockfd_);
}

void Socket::BindAddress(const InetAddress &localAddr) {
    const int ret = ::bind(
        sockfd_,
        reinterpret_cast<const sockaddr*>(&localAddr.GetSockAddr()),
        static_cast<socklen_t>(sizeof(sockaddr_in)));
    if (ret == 0) {
        return;
    }

    LOG_FATAL() << "bind failed: fd=" << sockfd_
                << " address=" << localAddr.ToIpPort()
                << " errno=" << errno
                << " message=" << std::strerror(errno);
    std::abort();
}

void Socket::Listen() {
    const int ret = ::listen(sockfd_, SOMAXCONN);
    if (ret == 0) {
        return;
    }

    LOG_FATAL() << "listen failed: fd=" << sockfd_
                << " errno=" << errno
                << " message=" << std::strerror(errno);
    std::abort();
}

int Socket::Accept(InetAddress *peeraddr) {
    sockaddr_in addr{};
    socklen_t len = static_cast<socklen_t>(sizeof(addr));

#if defined(__linux__)
    int connfd = ::accept4(
        sockfd_,
        reinterpret_cast<sockaddr*>(&addr),
        &len,
        SOCK_NONBLOCK | SOCK_CLOEXEC);
#else 
    int connfd = ::accept(
        sockfd_,
        reinterpret_cast<sockaddr*>(&addr),
        &len);
    if (connfd >= 0) {
        if (!SetNonBlocking(connfd)) {
            LOG_ERROR() << "failed to set accepted socket non-blocking: fd="
                        << connfd << " errno=" << errno
                        << " message=" << std::strerror(errno);
            ::close(connfd);
            return -1;
        }
        if (!SetCloseOnExec(connfd)) {
            LOG_ERROR() << "failed to set accepted socket close-on-exec: fd="
                        << connfd << " errno=" << errno
                        << " message=" << std::strerror(errno);
            ::close(connfd);
            return -1;
        }
    }
#endif
    if(connfd >= 0 && peeraddr) {
        *peeraddr = InetAddress(addr);
    } else if (connfd < 0 && errno != EAGAIN && errno != EWOULDBLOCK &&
               errno != EINTR) {
        LOG_ERROR() << "accept failed: listen_fd=" << sockfd_
                    << " errno=" << errno
                    << " message=" << std::strerror(errno);
    }

    return connfd;
}

void Socket::ShutdownWrite() {
    if (::shutdown(sockfd_, SHUT_WR) == 0) {
        return;
    }

    LOG_ERROR() << "shutdown write failed: fd=" << sockfd_
                << " errno=" << errno
                << " message=" << std::strerror(errno);
}

void Socket::SetTcpNoDelay(bool on) {
    int optval = on ? 1 : 0;
    if (::setsockopt(sockfd_, IPPROTO_TCP, TCP_NODELAY, &optval, sizeof(optval)) == 0) {
        return;
    }

    LOG_ERROR() << "setsockopt TCP_NODELAY failed: fd=" << sockfd_
                << " on=" << on
                << " errno=" << errno
                << " message=" << std::strerror(errno);
}

void Socket::SetReuseAddr(bool on) {
    int optval = on ? 1 : 0;
    if (::setsockopt(sockfd_, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)) == 0) {
        return;
    }

    LOG_ERROR() << "setsockopt SO_REUSEADDR failed: fd=" << sockfd_
                << " on=" << on
                << " errno=" << errno
                << " message=" << std::strerror(errno);
}

void Socket::SetReusePort(bool on) {
#ifdef SO_REUSEPORT
    int optval = on ? 1 : 0;
    if (::setsockopt(sockfd_, SOL_SOCKET, SO_REUSEPORT, &optval, sizeof(optval)) == 0) {
        return;
    }

    LOG_ERROR() << "setsockopt SO_REUSEPORT failed: fd=" << sockfd_
                << " on=" << on
                << " errno=" << errno
                << " message=" << std::strerror(errno);
#else
    (void)on;
    LOG_WARN() << "SO_REUSEPORT is not supported on this platform: fd=" << sockfd_;
#endif
}

void Socket::SetKeepAlive(bool on) {
    int optval = on ? 1 : 0;
    if (::setsockopt(sockfd_, SOL_SOCKET, SO_KEEPALIVE, &optval, sizeof(optval)) == 0) {
        return;
    }

    LOG_ERROR() << "setsockopt SO_KEEPALIVE failed: fd=" << sockfd_
                << " on=" << on
                << " errno=" << errno
                << " message=" << std::strerror(errno);
}
}   // namespace runtime::net
