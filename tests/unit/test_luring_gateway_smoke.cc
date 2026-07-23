// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <expected>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>

#include "coropact/base/error.h"
#include "coropact/coro/task.h"
#include "coropact/gateway/gateway_session_service.h"
#include "coropact/gateway/upstream.h"
#include "coropact/gateway/upstream_peer.h"
#include "coropact/gateway/upstream_registry.h"
#include "coropact/http/http_response.h"
#include "coropact/luring/connector.h"
#include "coropact/luring/server.h"
#include "coropact/luring/stream.h"
#include "coropact/net/inet_address.h"

namespace {

class UniqueFd {
public:
  UniqueFd() = default;
  explicit UniqueFd(int fd) noexcept : fd_(fd) {}
  UniqueFd(const UniqueFd&) = delete;
  UniqueFd& operator=(const UniqueFd&) = delete;
  UniqueFd(UniqueFd&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}
  UniqueFd& operator=(UniqueFd&& other) noexcept {
    if (this != &other) {
      Reset();
      fd_ = std::exchange(other.fd_, -1);
    }
    return *this;
  }

  ~UniqueFd() { Reset(); }

  int fd() const noexcept { return fd_; }

  void Reset(int fd = -1) noexcept {
    if (fd_ >= 0) {
      ::close(fd_);
    }
    fd_ = fd;
  }

private:
  int fd_{-1};
};

struct ListenEndpoint {
  UniqueFd fd;
  std::uint16_t port{0};
};

bool Check(bool condition, const char* message) {
  if (!condition) {
    std::cout << "FAIL: " << message << '\n';
    return false;
  }
  return true;
}

bool IsEnvironmentSkip(coropact::base::Error error) {
  return error == std::errc::operation_not_supported || error == std::errc::operation_not_permitted;
}

coropact::net::InetAddress LoopbackAddress(std::uint16_t port) {
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = htons(port);
  return coropact::net::InetAddress(addr);
}

coropact::base::Result<ListenEndpoint> ListenLoopback() {
  int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP);
  if (fd < 0) {
    return std::unexpected(coropact::base::CurrentErrno());
  }

  auto fail = [fd](coropact::base::Error error) -> coropact::base::Result<ListenEndpoint> {
    ::close(fd);
    return std::unexpected(error);
  };

  int reuse = 1;
  if (::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
    return fail(coropact::base::CurrentErrno());
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = htons(0);
  if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    return fail(coropact::base::CurrentErrno());
  }
  if (::listen(fd, SOMAXCONN) < 0) {
    return fail(coropact::base::CurrentErrno());
  }

  socklen_t length = sizeof(addr);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &length) < 0) {
    return fail(coropact::base::CurrentErrno());
  }

  return ListenEndpoint{.fd = UniqueFd(fd), .port = ntohs(addr.sin_port)};
}

coropact::base::Result<std::uint16_t> PickFreePort() {
  auto endpoint = ListenLoopback();
  if (!endpoint.has_value()) {
    return std::unexpected(endpoint.error());
  }
  return endpoint->port;
}

coropact::base::Result<int> ConnectClient(const coropact::net::InetAddress& address) {
  int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP);
  if (fd < 0) {
    return std::unexpected(coropact::base::CurrentErrno());
  }

  const sockaddr_in& peer = address.sock_addr();
  if (::connect(fd, reinterpret_cast<const sockaddr*>(&peer), sizeof(peer)) < 0) {
    auto error = coropact::base::CurrentErrno();
    ::close(fd);
    return std::unexpected(error);
  }

  return fd;
}

bool SendAll(int fd, std::string_view bytes) {
  while (!bytes.empty()) {
    const ssize_t written = ::send(fd, bytes.data(), bytes.size(), MSG_NOSIGNAL);
    if (written < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    if (written == 0) return false;
    bytes.remove_prefix(static_cast<std::size_t>(written));
  }
  return true;
}

std::size_t ContentLength(std::string_view response) {
  constexpr std::string_view kHeader = "Content-Length:";
  const std::size_t begin = response.find(kHeader);
  if (begin == std::string_view::npos) return 0;

  const std::size_t value_begin = begin + kHeader.size();
  const std::size_t value_end = response.find("\r\n", value_begin);
  if (value_end == std::string_view::npos) return 0;

  std::size_t value = 0;
  for (std::size_t i = value_begin; i < value_end; ++i) {
    if (response[i] >= '0' && response[i] <= '9') {
      value = value * 10 + static_cast<std::size_t>(response[i] - '0');
    }
  }
  return value;
}

std::string ReadHttpResponse(int fd) {
  std::string response;
  char buffer[4096];

  for (int attempt = 0; attempt < 200; ++attempt) {
    pollfd event{.fd = fd, .events = POLLIN, .revents = 0};
    const int ready = ::poll(&event, 1, 25);
    if (ready < 0) {
      if (errno == EINTR) continue;
      return {};
    }
    if (ready == 0) continue;

    const ssize_t nread = ::recv(fd, buffer, sizeof(buffer), 0);
    if (nread <= 0) break;
    response.append(buffer, static_cast<std::size_t>(nread));

    const std::size_t header_end = response.find("\r\n\r\n");
    if (header_end == std::string::npos) continue;
    if (response.size() >= header_end + 4 + ContentLength(response)) break;
  }

  return response;
}

coropact::luring::LUringServerOptions MakeOptions() {
  coropact::luring::LUringServerOptions options;
  options.worker_group_options.worker_num = 1;
  options.worker_group_options.worker_options.loop_options.entries = 64;
  options.worker_group_options.worker_options.loop_options.submit_batch = 1;
  options.worker_group_options.worker_options.listen_options.reuse_port = true;
  return options;
}

class UpstreamStub {
public:
  explicit UpstreamStub(ListenEndpoint endpoint) : endpoint_(std::move(endpoint)) {
    thread_ = std::thread([this] { Run(); });
  }

  ~UpstreamStub() {
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  UpstreamStub(const UpstreamStub&) = delete;
  UpstreamStub& operator=(const UpstreamStub&) = delete;

  std::uint16_t port() const noexcept { return endpoint_.port; }

private:
  void Run() noexcept {
    int client = ::accept4(endpoint_.fd.fd(), nullptr, nullptr, SOCK_CLOEXEC);
    if (client < 0) return;

    std::string request;
    char buffer[2048];
    while (request.find("\r\n\r\n") == std::string::npos) {
      const ssize_t nread = ::recv(client, buffer, sizeof(buffer), 0);
      if (nread <= 0) {
        ::close(client);
        return;
      }
      request.append(buffer, static_cast<std::size_t>(nread));
      if (request.size() > 16 * 1024) {
        ::close(client);
        return;
      }
    }

    constexpr std::string_view response =
        "HTTP/1.1 200 OK\r\n"
        "Content-Length: 2\r\n"
        "Connection: close\r\n"
        "X-Upstream: luring\r\n"
        "\r\n"
        "ok";
    SendAll(client, response);
    ::shutdown(client, SHUT_WR);
    ::close(client);
  }

  ListenEndpoint endpoint_;
  std::thread thread_;
};

template <class Service>
void BindServer(Service& service, coropact::luring::LUringServer& server) {
  server.set_session_handler(
      [&service](coropact::luring::LUringWorkerContext& context,
                 coropact::luring::LUringStream stream) -> coropact::coro::Task<void> {
        return service.Serve(std::move(stream), coropact::luring::LUringConnector(&context.loop));
      });
}

bool CheckDirectRoute() {
  auto port = PickFreePort();
  if (!port.has_value()) {
    if (IsEnvironmentSkip(port.error())) {
      std::cout << "SKIP: TCP bind unavailable: " << port.error().message() << '\n';
      return true;
    }
    std::cout << "FAIL: PickFreePort failed: " << port.error().message() << '\n';
    return false;
  }

  coropact::gateway::UpstreamRegistry registry;
  using Service = coropact::gateway::GatewaySessionService<coropact::luring::LUringStream,
                                                       coropact::luring::LUringConnector>;
  Service service("luring-gateway", registry);
  service.Get("/healthz", [](const coropact::http::HttpRequest&, coropact::http::HttpResponse& response) {
    response.set_status_code(coropact::http::StatusCode::Ok);
    response.set_content_type("text/plain");
    response.set_body("ok");
  });

  coropact::luring::LUringServer server(LoopbackAddress(*port), MakeOptions());
  BindServer(service, server);
  auto started = server.Start();
  if (!started.has_value()) {
    if (IsEnvironmentSkip(started.error())) {
      std::cout << "SKIP: io_uring unavailable: " << started.error().message() << '\n';
      return true;
    }
    std::cout << "FAIL: LUringServer::Start failed: " << started.error().message() << '\n';
    return false;
  }

  auto client_fd = ConnectClient(LoopbackAddress(*port));
  if (!client_fd.has_value()) {
    server.Stop();
    std::cout << "FAIL: gateway client connect failed: " << client_fd.error().message() << '\n';
    return false;
  }
  UniqueFd client(*client_fd);

  constexpr std::string_view request =
      "GET /healthz HTTP/1.1\r\nHost: gateway\r\nConnection: close\r\n\r\n";
  const bool sent = SendAll(client.fd(), request);
  ::shutdown(client.fd(), SHUT_WR);
  const std::string response = ReadHttpResponse(client.fd());
  server.Stop();

  return Check(sent, "direct request should be sent") &&
         Check(response.find("HTTP/1.1 200") != std::string::npos,
               "luring gateway direct route should return 200") &&
         Check(response.ends_with("ok"), "luring gateway direct route should return body");
}

bool CheckProxyRoute() {
  auto upstream_endpoint = ListenLoopback();
  auto gateway_port = PickFreePort();
  if (!upstream_endpoint.has_value() || !gateway_port.has_value()) {
    const coropact::base::Error error =
        !upstream_endpoint.has_value() ? upstream_endpoint.error() : gateway_port.error();
    if (IsEnvironmentSkip(error)) {
      std::cout << "SKIP: TCP bind unavailable: " << error.message() << '\n';
      return true;
    }
    std::cout << "FAIL: could not create proxy endpoints: " << error.message() << '\n';
    return false;
  }

  UpstreamStub upstream(std::move(*upstream_endpoint));
  coropact::gateway::UpstreamRegistry registry;
  auto backend =
      std::make_shared<coropact::gateway::Upstream>(coropact::gateway::UpstreamConfig{.name = "backend"});
  backend->AddPeer(std::make_shared<coropact::gateway::UpstreamPeer>(coropact::gateway::UpstreamPeerConfig{
      .name = "loopback", .host = "127.0.0.1", .port = upstream.port()}));
  registry.Add(backend);

  using Service = coropact::gateway::GatewaySessionService<coropact::luring::LUringStream,
                                                       coropact::luring::LUringConnector>;
  Service service("luring-gateway", registry);
  service.AddProxyRoute("/api", "backend", "round_robin");

  coropact::luring::LUringServer server(LoopbackAddress(*gateway_port), MakeOptions());
  BindServer(service, server);
  auto started = server.Start();
  if (!started.has_value()) {
    if (IsEnvironmentSkip(started.error())) {
      std::cout << "SKIP: io_uring unavailable: " << started.error().message() << '\n';
      return true;
    }
    std::cout << "FAIL: LUringServer::Start failed: " << started.error().message() << '\n';
    return false;
  }

  auto client_fd = ConnectClient(LoopbackAddress(*gateway_port));
  if (!client_fd.has_value()) {
    server.Stop();
    std::cout << "FAIL: proxy client connect failed: " << client_fd.error().message() << '\n';
    return false;
  }
  UniqueFd client(*client_fd);

  constexpr std::string_view request =
      "GET /api/health HTTP/1.1\r\nHost: gateway\r\nConnection: close\r\n\r\n";
  const bool sent = SendAll(client.fd(), request);
  ::shutdown(client.fd(), SHUT_WR);
  const std::string response = ReadHttpResponse(client.fd());
  server.Stop();

  return Check(sent, "proxy request should be sent") &&
         Check(response.find("HTTP/1.1 200") != std::string::npos,
               "luring gateway proxy route should return upstream status") &&
         Check(response.ends_with("ok"), "luring gateway proxy route should return upstream body");
}

}  // namespace

int main() {
  if (!CheckDirectRoute()) return 1;
  if (!CheckProxyRoute()) return 1;

  std::cout << "luring gateway smoke: PASS\n";
  return 0;
}
