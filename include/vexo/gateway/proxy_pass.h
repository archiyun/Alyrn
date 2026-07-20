// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <span>
#include <string>
#include <string_view>

#include "vexo/base/error.h"
#include "vexo/coro/task.h"
#include "vexo/gateway/forwarded_header_context.h"
#include "vexo/gateway/load_balancer.h"
#include "vexo/gateway/upstream.h"
#include "vexo/gateway/upstream_conn_pool.h"
#include "vexo/gateway/upstream_peer.h"
#include "vexo/http/http_request.h"
#include "vexo/http/http_types.h"
#include "vexo/io/async_stream.h"
#include "vexo/io/stream_algorithms.h"

namespace vexo::gateway {

enum class BodyFraming : uint8_t {
  kCloseDelimited,
  kContentLength,
  kChunked,
  kNoBody,
};

enum class ProxyForwardStatus : uint8_t {
  kCompleted,
  kNoPeer,
  kClientClosed,
  kUpstreamFailed,
};

struct ProxyForwardResult {
  ProxyForwardStatus status{ProxyForwardStatus::kCompleted};
  bool started{false};
};

template <class T>
concept UpstreamConnector = requires(T& connector, std::string_view host, std::uint16_t port) {
  typename T::Stream;
  requires vexo::io::AsyncStream<typename T::Stream>;
  {
    connector.Connect(host, port)
  } -> std::same_as<vexo::coro::Task<vexo::base::Result<std::unique_ptr<typename T::Stream>>>>;
};

class ProxyPass {
public:
  template <vexo::io::AsyncStream ClientStream, UpstreamConnector Connector>
  static vexo::coro::Task<ProxyForwardResult> Forward(
      ClientStream& client, const vexo::http::HttpRequest& request, Upstream& upstream,
      LoadBalancer& lb, UpstreamStreamPool<typename Connector::Stream>& pool, Connector& connector,
      const RequestContext& ctx = {}, CircuitBreaker* cb = nullptr,
      ForwardedHeaderContext forwarded = {});

  static std::string BuildRequest(const vexo::http::HttpRequest& req, const UpstreamPeer& peer,
                                  ForwardedHeaderContext forwarded = {});

private:
  struct ResponseState {
    BodyFraming framing{BodyFraming::kCloseDelimited};
    uint64_t body_remaining{0};
    bool upstream_keepalive{true};
    int status{0};
  };

  static uint64_t NowMs();
  static bool IsIdempotent(vexo::http::Method method);
  static void RewriteHeaders(std::string_view raw_headers, std::string& out);
  static ResponseState ParseResponseState(std::string_view raw_headers,
                                          vexo::http::Method request_method);
  static std::shared_ptr<UpstreamPeer> SelectFailoverPeer(Upstream& upstream,
                                                          const UpstreamPeer& current);

  template <vexo::io::AsyncReadStream Stream>
  static vexo::coro::Task<vexo::base::Result<std::size_t>> ReadSomeWithTimeout(
      Stream& stream, std::span<std::byte> buffer, std::chrono::milliseconds timeout);

  template <vexo::io::AsyncWriteStream Stream>
  static vexo::coro::Task<vexo::base::Result<void>> WriteString(Stream& stream,
                                                                std::string_view bytes);

  template <vexo::io::AsyncWriteStream Stream>
  static vexo::coro::Task<void> Send502(Stream& client);

  template <vexo::io::AsyncStream ClientStream, vexo::io::AsyncStream UpstreamStream>
  static vexo::coro::Task<vexo::base::Result<bool>> RelayResponse(
      ClientStream& client, UpstreamStream& upstream, UpstreamPeer& peer, CircuitBreaker* cb,
      bool& cb_reported, vexo::http::Method request_method,
      std::chrono::milliseconds request_timeout);
};

template <vexo::io::AsyncReadStream Stream>
vexo::coro::Task<vexo::base::Result<std::size_t>> ProxyPass::ReadSomeWithTimeout(
    Stream& stream, std::span<std::byte> buffer, std::chrono::milliseconds timeout) {
  if constexpr (requires { stream.ReadSomeFor(buffer, timeout); }) {
    co_return co_await stream.ReadSomeFor(buffer, timeout);
  } else {
    co_return co_await stream.ReadSome(buffer);
  }
}

template <vexo::io::AsyncWriteStream Stream>
vexo::coro::Task<vexo::base::Result<void>> ProxyPass::WriteString(Stream& stream,
                                                                  std::string_view bytes) {
  auto result = co_await vexo::io::WriteAll(
      stream, std::as_bytes(std::span<const char>(bytes.data(), bytes.size())));
  co_return result;
}

template <vexo::io::AsyncWriteStream Stream>
vexo::coro::Task<void> ProxyPass::Send502(Stream& client) {
  static constexpr std::string_view kResp =
      "HTTP/1.1 502 Bad Gateway\r\n"
      "Content-Type: text/plain\r\n"
      "Content-Length: 11\r\n"
      "Connection: close\r\n"
      "\r\n"
      "Bad Gateway";
  co_await WriteString(client, kResp);
  if constexpr (vexo::io::AsyncClosableStream<Stream>) {
    co_await client.Shutdown();
  }
}

template <vexo::io::AsyncStream ClientStream, vexo::io::AsyncStream UpstreamStream>
vexo::coro::Task<vexo::base::Result<bool>> ProxyPass::RelayResponse(
    ClientStream& client, UpstreamStream& upstream, UpstreamPeer& peer, CircuitBreaker* cb,
    bool& cb_reported, vexo::http::Method request_method,
    std::chrono::milliseconds request_timeout) {
  std::array<std::byte, 4096> read_buffer{};
  std::string pending;

  for (;;) {
    const std::size_t header_end = pending.find("\r\n\r\n");
    if (header_end != std::string::npos) {
      const std::size_t raw_size = header_end + 4;
      const std::string_view raw_headers(pending.data(), raw_size);
      ResponseState state = ParseResponseState(raw_headers, request_method);

      if (!cb_reported) {
        cb_reported = true;
        if (state.status >= 500) {
          if (cb) cb->OnFailure();
        } else {
          peer.OnSuccess();
          if (cb) cb->OnSuccess();
        }
      }

      std::string outbound;
      outbound.reserve(raw_headers.size() + 64 + pending.size() - raw_size);
      RewriteHeaders(raw_headers, outbound);
      pending.erase(0, raw_size);

      if (!pending.empty()) {
        const uint64_t n = (state.framing == BodyFraming::kContentLength)
                               ? std::min<uint64_t>(pending.size(), state.body_remaining)
                               : pending.size();
        outbound.append(pending.data(), n);
        pending.erase(0, static_cast<std::size_t>(n));
        if (state.framing == BodyFraming::kContentLength) {
          state.body_remaining -= n;
        }
      }

      if (!outbound.empty()) {
        auto write = co_await WriteString(client, outbound);
        if (!write.has_value()) co_return std::unexpected(write.error());
      }

      if (state.framing == BodyFraming::kNoBody ||
          (state.framing == BodyFraming::kContentLength && state.body_remaining == 0)) {
        co_return state.upstream_keepalive;
      }

      for (;;) {
        auto read = co_await ReadSomeWithTimeout(upstream, read_buffer, request_timeout);
        if (!read.has_value()) co_return std::unexpected(read.error());
        if (*read == 0) {
          co_return false;
        }
        std::string_view chunk(reinterpret_cast<const char*>(read_buffer.data()), *read);
        if (state.framing == BodyFraming::kContentLength) {
          const uint64_t n = std::min<uint64_t>(chunk.size(), state.body_remaining);
          auto write = co_await WriteString(client, chunk.substr(0, static_cast<std::size_t>(n)));
          if (!write.has_value()) co_return std::unexpected(write.error());
          state.body_remaining -= n;
          if (state.body_remaining == 0) {
            co_return state.upstream_keepalive;
          }
        } else {
          auto write = co_await WriteString(client, chunk);
          if (!write.has_value()) co_return std::unexpected(write.error());
        }
      }
    }

    auto read = co_await ReadSomeWithTimeout(upstream, read_buffer, request_timeout);
    if (!read.has_value()) co_return std::unexpected(read.error());
    if (*read == 0) co_return std::unexpected(vexo::base::make_errno(EPIPE));
    pending.append(reinterpret_cast<const char*>(read_buffer.data()), *read);
  }
}

template <vexo::io::AsyncStream ClientStream, UpstreamConnector Connector>
vexo::coro::Task<ProxyForwardResult> ProxyPass::Forward(
    ClientStream& client, const vexo::http::HttpRequest& request, Upstream& upstream,
    LoadBalancer& lb, UpstreamStreamPool<typename Connector::Stream>& pool, Connector& connector,
    const RequestContext& ctx, CircuitBreaker* cb, ForwardedHeaderContext forwarded) {
  using UpstreamStream = typename Connector::Stream;

  if (!upstream.TryAcquireRequestSlot()) {
    if (cb) cb->OnFailure();
    co_return ProxyForwardResult{.status = ProxyForwardStatus::kNoPeer};
  }

  auto release_upstream = [&upstream] { upstream.ReleaseRequestSlot(); };
  auto peer = lb.Select(upstream, ctx);
  if (!peer) {
    release_upstream();
    if (cb) cb->OnFailure();
    co_return ProxyForwardResult{.status = ProxyForwardStatus::kNoPeer};
  }

  peer->state().active.fetch_add(1, std::memory_order_relaxed);
  peer->state().requests.fetch_add(1, std::memory_order_relaxed);
  auto release_peer = [&peer] {
    if (peer) peer->state().active.fetch_sub(1, std::memory_order_relaxed);
  };

  std::string request_bytes = BuildRequest(request, *peer, forwarded);
  bool cb_reported = false;
  int retries_left = 2;
  bool request_on_wire = false;

  for (;;) {
    std::unique_ptr<UpstreamStream> upstream_stream = pool.Acquire(peer.get());
    if (!upstream_stream) {
      auto connected = co_await connector.Connect(peer->config().host, peer->config().port);
      if (!connected.has_value()) {
        peer->OnFailure(NowMs());
        if ((!request_on_wire || IsIdempotent(request.method())) && retries_left-- > 0) {
          auto next = lb.Select(upstream, ctx);
          if (!next || next.get() == peer.get()) next = SelectFailoverPeer(upstream, *peer);
          if (next && next.get() != peer.get()) {
            release_peer();
            peer = std::move(next);
            peer->state().active.fetch_add(1, std::memory_order_relaxed);
            peer->state().requests.fetch_add(1, std::memory_order_relaxed);
            request_bytes = BuildRequest(request, *peer, forwarded);
            continue;
          }
        }
        if (!cb_reported) {
          cb_reported = true;
          if (cb) cb->OnFailure();
        }
        co_await Send502(client);
        release_peer();
        release_upstream();
        co_return ProxyForwardResult{.status = ProxyForwardStatus::kUpstreamFailed,
                                     .started = true};
      }
      upstream_stream = std::move(*connected);
    }

    auto written = co_await WriteString(*upstream_stream, request_bytes);
    request_on_wire = true;
    if (!written.has_value()) {
      peer->OnFailure(NowMs());
      if (IsIdempotent(request.method()) && retries_left-- > 0) {
        auto next = lb.Select(upstream, ctx);
        if (!next || next.get() == peer.get()) next = SelectFailoverPeer(upstream, *peer);
        if (next && next.get() != peer.get()) {
          release_peer();
          peer = std::move(next);
          peer->state().active.fetch_add(1, std::memory_order_relaxed);
          peer->state().requests.fetch_add(1, std::memory_order_relaxed);
          request_bytes = BuildRequest(request, *peer, forwarded);
          continue;
        }
      }
      if (!cb_reported) {
        cb_reported = true;
        if (cb) cb->OnFailure();
      }
      co_await Send502(client);
      release_peer();
      release_upstream();
      co_return ProxyForwardResult{.status = ProxyForwardStatus::kUpstreamFailed, .started = true};
    }

    auto reusable = co_await RelayResponse(client, *upstream_stream, *peer, cb, cb_reported,
                                           request.method(), upstream.config().request_timeout);
    if (!reusable.has_value()) {
      peer->OnFailure(NowMs());
      if (IsIdempotent(request.method()) && retries_left-- > 0) {
        auto next = lb.Select(upstream, ctx);
        if (!next || next.get() == peer.get()) next = SelectFailoverPeer(upstream, *peer);
        if (next && next.get() != peer.get()) {
          release_peer();
          peer = std::move(next);
          peer->state().active.fetch_add(1, std::memory_order_relaxed);
          peer->state().requests.fetch_add(1, std::memory_order_relaxed);
          request_bytes = BuildRequest(request, *peer, forwarded);
          request_on_wire = false;
          continue;
        }
      }
      if (!cb_reported) {
        cb_reported = true;
        if (cb) cb->OnFailure();
      }
      co_await Send502(client);
      release_peer();
      release_upstream();
      co_return ProxyForwardResult{.status = ProxyForwardStatus::kUpstreamFailed, .started = true};
    }

    if (*reusable) {
      pool.Release(peer.get(), std::move(upstream_stream));
    }
    release_peer();
    release_upstream();
    co_return ProxyForwardResult{.status = ProxyForwardStatus::kCompleted, .started = true};
  }
}

}  // namespace vexo::gateway
