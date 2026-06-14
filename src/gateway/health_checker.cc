// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
// Active upstream health checking.
//
// The checker periodically probes every registered peer with a short-lived
// HTTP GET request. Probe results are used in two ways:
//
//   * Consecutive failures mark a peer down, removing it from load balancing.
//   * Successful probes clear failure state and gradually restore its dynamic
//     effective weight.
//
// The consecutive success/failure counters are owned by the gateway EventLoop
// thread, so they do not require additional synchronization.
#include "runtime/gateway/health_checker.h"

#include <chrono>
#include <string>

#include "runtime/log/logger.h"
#include "runtime/net/buffer.h"
#include "runtime/net/event_loop.h"
#include "runtime/net/inet_address.h"
#include "runtime/net/net_utils.h"
#include "runtime/net/tcp_client.h"
#include "runtime/time/timestamp.h"

namespace runtime::gateway {

namespace {
// Monotonic milliseconds since some unspecified epoch. Used only to stamp
// state.checked_ms so the fail_timeout window can be evaluated later.
uint64_t NowMs() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}
}  // namespace

HealthChecker::HealthChecker(runtime::net::EventLoop* loop, 
                             UpstreamRegistry& registry, 
                             HealthCheckConfig cfg) 
  : loop_(loop), registry_(registry), cfg_(std::move(cfg)) {}


void HealthChecker::Start() {
  if (running_) return;
  running_ = true;
  timer_id_ = loop_->RunEvery(cfg_.interval_sec, [this] { CheckAll(); });
  LOG_INFO() << "health_checker: started, interval=" << cfg_.interval_sec << "s";
}
void HealthChecker::Stop() {
  if (!running_) return;
  running_ = false;
  loop_->Cancel(timer_id_);
}

void HealthChecker::CheckAll() {
  for (const auto& [_, upstream] : registry_.all()) {
    for (const auto& peer : upstream->peers()) {
      CheckOne(peer);
    }
  }
}

void HealthChecker::CheckOne(std::shared_ptr<UpstreamPeer> peer) {
  using TcpConnectionPtr = runtime::net::TcpConnection::TcpConnectionPtr;
  std::string name = peer->config().name;

  auto address = runtime::net::ParseIPv4Address(peer->config().host,
                                                peer->config().port);
  if (!address) {
    peer->OnFailure(NowMs());
    auto& fail_count = consecutive_fail_[name];
    if (++fail_count >= cfg_.unhealthy_threshold) {
      peer->state().down.store(true, std::memory_order_relaxed);
    }
    LOG_ERROR() << "health_checker: invalid IPv4 address for peer " << name
                << ": " << peer->config().host
                << " error=" << address.error.message();
    return;
  }

  // 1. Build a one-shot TcpClient targeting this peer. The shared_ptr keeps
  //    the client alive across the async callbacks below.
  auto client = std::make_shared<runtime::net::TcpClient>(
    loop_, *address.value, "health->" + name);

  // 2. Pre-render the probe request. Connection: close keeps each probe
  //    fully independent — we don't pool health-check sockets.
  const std::string request =
    "GET " + cfg_.path + " HTTP/1.1\r\n"
    "Host: " + peer->config().host + "\r\n"
    "Connection: close\r\n\r\n";

  // 3. ConnectionCallback: on connect, send the request; on early disconnect
  //    (before any bytes were read), count it as a connection-level failure.
  //    `done` guards against the MessageCallback path having already attributed
  //    the outcome to a higher-priority signal.
  auto done = std::make_shared<bool>(false);
  client->set_connection_callback(
    [this, peer, request, name, done](const TcpConnectionPtr& conn) {
      if (conn->Connected()) {
        conn->Send(request);
      }
      else if (!*done) {
        peer->OnFailure(NowMs());  // -1 to effective_weight per failed probe.
        auto& fail_count = consecutive_fail_[name];
        if (++fail_count >= cfg_.unhealthy_threshold &&
            !peer->state().down.load(std::memory_order_relaxed)) {
          peer->state().down.store(true, std::memory_order_relaxed);
        }
      }
    });

  // 4. MessageCallback: inspect the first 12 bytes for "HTTP/1.1 200" and
  //    drive both the binary down/up threshold and the gradual effective_weight
  //    decay/recovery from the result.
  client->set_message_callback(
    [this, peer, name, healthy_threshold = cfg_.healthy_threshold,
     unhealthy_threshold = cfg_.unhealthy_threshold, done](
      const TcpConnectionPtr& conn,
      runtime::net::Buffer& buf,
      runtime::time::Timestamp) {
        // "HTTP/1.1 200" is 12 bytes; wait for the full status line before deciding.
        if (buf.readable_bytes() < 12) return;

        *done = true;  // Suppress the ConnectionCallback fallback path.

        const bool ok = std::string_view(buf.Peek(), 12) == "HTTP/1.1 200";
        buf.RetrieveAll();
        conn->Shutdown();

        auto& ok_count = consecutive_ok_[name];
        if (ok) {
          peer->OnSuccess();  // +1 to effective_weight (capped at config.weight).
          consecutive_fail_[name] = 0;
          ++ok_count;
          // After healthy_threshold consecutive successes, lift the hard down flag.
          if (ok_count >= healthy_threshold &&
              peer->state().down.load(std::memory_order_relaxed)) {
            peer->state().down.store(false, std::memory_order_relaxed);
            peer->state().fails.store(0, std::memory_order_relaxed);
            ok_count = 0;
            LOG_INFO() << "health_checker: upstream peer " << name << " recovered";
          }
        } else {
          peer->OnFailure(NowMs());  // App-layer failure also decays effective_weight.
          ok_count = 0;
          // After unhealthy_threshold consecutive failures, hard-mark the peer down.
          auto& fail_count = consecutive_fail_[name];
          ++fail_count;
          if (fail_count >= unhealthy_threshold &&
              !peer->state().down.load(std::memory_order_relaxed)) {
            peer->state().down.store(true, std::memory_order_relaxed);
            LOG_WARN() << "health_checker: upstream peer " << name << " marked down";
          }
        }
      });

  // 5. Kick off the probe.
  client->Connect();

  // 6. Timeout backstop: if the probe still has a live connection after
  //    `timeout_sec`, force-disconnect so the callbacks above run and the
  //    TcpClient can be released.
  loop_->RunAfter(cfg_.timeout_sec, [client] {
    if (client->connection() && client->connection()->Connected()) {
      client->Disconnect();
    }
  });
}
}  // namespace runtime::gateway
