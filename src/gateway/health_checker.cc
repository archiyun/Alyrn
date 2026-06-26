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
#include "vexo/gateway/health_checker.h"

#include <atomic>
#include <charconv>
#include <chrono>
#include <string>
#include <string_view>
#include <unordered_map>

#include "vexo/log/logger.h"
#include "vexo/net/buffer.h"
#include "vexo/net/event_loop.h"
#include "vexo/net/inet_address.h"
#include "vexo/net/net_utils.h"
#include "vexo/net/tcp_client.h"
#include "vexo/time/timestamp.h"

namespace vexo::gateway {

namespace {
constexpr std::size_t kMaxHealthResponseHeaderBytes = 16 * 1024;

// Monotonic milliseconds since some unspecified epoch. Used only to stamp
// state.checked_ms so the fail_timeout window can be evaluated later.
uint64_t NowMs() {
  return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                   std::chrono::steady_clock::now().time_since_epoch())
                                   .count());
}

int ParseHealthStatus(std::string_view headers) {
  const auto line_end = headers.find("\r\n");
  if (line_end == std::string_view::npos) return 0;

  const std::string_view line = headers.substr(0, line_end);
  if (!(line.starts_with("HTTP/1.1 ") || line.starts_with("HTTP/1.0 ")) || line.size() < 12) {
    return 0;
  }

  const std::string_view code = line.substr(9, 3);
  int status = 0;
  const auto [ptr, ec] = std::from_chars(code.data(), code.data() + code.size(), status);
  if (ec != std::errc{} || ptr != code.data() + code.size()) return 0;
  if (line.size() > 12 && line[12] != ' ') return 0;
  return status;
}

void CancelTimeoutLater(vexo::net::EventLoop* loop, vexo::time::TimerId id) {
  // Cancelling releases the timer's TcpClient capture. Do that after the
  // current connection callback returns.
  loop->QueueInLoop([loop, id] { loop->Cancel(id); });
}
}  // namespace

struct HealthChecker::Probe {
  bool done{false};
  vexo::time::TimerId timeout_id;
};

struct HealthChecker::State {
  State(vexo::net::EventLoop* event_loop, UpstreamRegistry& registry_ref, HealthCheckConfig config)
      : loop(event_loop), registry(registry_ref), cfg(std::move(config)) {}

  bool IsActive(uint64_t expected_generation) const noexcept {
    return active.load(std::memory_order_acquire) &&
           generation.load(std::memory_order_acquire) == expected_generation;
  }

  vexo::net::EventLoop* loop;
  UpstreamRegistry& registry;
  HealthCheckConfig cfg;
  std::atomic<bool> active{false};
  std::atomic<uint64_t> generation{0};

  // Accessed only by the gateway EventLoop thread.
  std::unordered_map<std::string, int> consecutive_ok;
  std::unordered_map<std::string, int> consecutive_fail;
  std::unordered_map<std::string, uint64_t> in_flight_generation;
};

HealthChecker::HealthChecker(vexo::net::EventLoop* loop, UpstreamRegistry& registry,
                             HealthCheckConfig cfg)
    : loop_(loop), state_(std::make_shared<State>(loop, registry, std::move(cfg))) {}

HealthChecker::~HealthChecker() { Stop(); }

void HealthChecker::Start() {
  bool expected = false;
  if (!running_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
    return;
  }

  const uint64_t generation = state_->generation.fetch_add(1, std::memory_order_acq_rel) + 1;
  state_->active.store(true, std::memory_order_release);

  std::weak_ptr<State> weak_state = state_;
  timer_id_ = loop_->RunEvery(state_->cfg.interval_sec, [weak_state, generation] {
    if (auto state = weak_state.lock(); state && state->IsActive(generation)) {
      CheckAll(state, generation);
    }
  });
  LOG_INFO() << "health_checker: started, interval=" << state_->cfg.interval_sec << "s";
}

void HealthChecker::Stop() {
  if (!running_.exchange(false, std::memory_order_acq_rel)) return;

  // Invalidate every callback created by this Start() before requesting timer
  // cancellation. EventLoop::Cancel may be queued when Stop() is called from a
  // different thread, so cancellation alone cannot protect object lifetime.
  state_->active.store(false, std::memory_order_release);
  state_->generation.fetch_add(1, std::memory_order_acq_rel);
  loop_->Cancel(timer_id_);
}

void HealthChecker::CheckAll(const std::shared_ptr<State>& state, uint64_t generation) {
  if (!state->IsActive(generation)) return;

  for (const auto& [_, upstream] : state->registry.all()) {
    for (const auto& peer : upstream->peers()) {
      if (!state->IsActive(generation)) return;
      CheckOne(state, generation, peer);
    }
  }
}

void HealthChecker::CheckOne(const std::shared_ptr<State>& state, uint64_t generation,
                             std::shared_ptr<UpstreamPeer> peer) {
  using TcpConnectionPtr = vexo::net::TcpConnection::TcpConnectionPtr;
  if (!state->IsActive(generation)) return;

  std::string name = peer->config().name;

  auto address = vexo::net::ParseIPv4Address(peer->config().host, peer->config().port);
  if (!address) {
    if (!state->IsActive(generation)) return;
    peer->OnFailure(NowMs());
    state->consecutive_ok[name] = 0;
    auto& fail_count = state->consecutive_fail[name];
    if (++fail_count >= state->cfg.unhealthy_threshold) {
      peer->state().down.store(true, std::memory_order_relaxed);
    }
    LOG_ERROR() << "health_checker: invalid IPv4 address for peer " << name << ": "
                << peer->config().host << " error=" << address.error().message();
    return;
  }

  auto [flight_it, inserted] = state->in_flight_generation.try_emplace(name, generation);
  if (!inserted) {
    if (flight_it->second == generation) {
      return;
    }
    flight_it->second = generation;
  }

  auto client = std::make_shared<vexo::net::TcpClient>(state->loop, *address, "health->" + name);
  client->set_retry_enabled(false);

  const std::string request = "GET " + state->cfg.path +
                              " HTTP/1.1\r\n"
                              "Host: " +
                              peer->config().host +
                              "\r\n"
                              "Connection: close\r\n\r\n";

  auto probe = std::make_shared<Probe>();
  std::weak_ptr<State> weak_state = state;
  client->set_connection_callback(
      [weak_state, generation, peer, request, name, probe](const TcpConnectionPtr& conn) {
        auto state = weak_state.lock();
        if (!state || !state->IsActive(generation)) return;

        if (conn->Connected()) {
          conn->Send(request);
        } else if (CompleteProbe(state, generation, peer, name, probe, false)) {
          CancelTimeoutLater(state->loop, probe->timeout_id);
        }
      });

  client->set_message_callback(
      [weak_state, generation, peer, name, probe](const TcpConnectionPtr& conn,
                                                  vexo::net::Buffer& buf, vexo::time::Timestamp) {
        auto state = weak_state.lock();
        if (!state || !state->IsActive(generation)) {
          buf.RetrieveAll();
          conn->Shutdown();
          return;
        }

        const char* header_end = buf.FindCRLFCRLF();
        if (!header_end && buf.readable_bytes() <= kMaxHealthResponseHeaderBytes) {
          return;
        }

        bool ok = false;
        if (header_end) {
          const std::size_t header_bytes = static_cast<std::size_t>(header_end - buf.Peek()) + 4;
          ok = header_bytes <= kMaxHealthResponseHeaderBytes &&
               ParseHealthStatus(std::string_view(buf.Peek(), header_bytes)) == 200;
        }
        buf.RetrieveAll();
        conn->Shutdown();
        if (CompleteProbe(state, generation, peer, name, probe, ok)) {
          CancelTimeoutLater(state->loop, probe->timeout_id);
        }
      });

  client->Connect();

  // This timer owns the one-shot client until completion or timeout.
  probe->timeout_id = state->loop->RunAfter(
      state->cfg.timeout_sec, [weak_state, generation, peer, name, probe, client] {
        if (auto state = weak_state.lock()) {
          CompleteProbe(state, generation, peer, name, probe, false);
        }
        if (client->connection() && client->connection()->Connected()) {
          client->Disconnect();
        }
      });
}

bool HealthChecker::CompleteProbe(const std::shared_ptr<State>& state, uint64_t generation,
                                  const std::shared_ptr<UpstreamPeer>& peer,
                                  const std::string& name, const std::shared_ptr<Probe>& probe,
                                  bool success) {
  if (probe->done) return false;
  probe->done = true;
  if (!state->IsActive(generation)) return false;

  auto in_flight = state->in_flight_generation.find(name);
  if (in_flight != state->in_flight_generation.end() && in_flight->second == generation) {
    state->in_flight_generation.erase(in_flight);
  }

  auto& ok_count = state->consecutive_ok[name];
  if (success) {
    peer->OnSuccess();
    state->consecutive_fail[name] = 0;
    ++ok_count;
    if (ok_count >= state->cfg.healthy_threshold) {
      peer->state().down.store(false, std::memory_order_relaxed);
      peer->state().fails.store(0, std::memory_order_relaxed);
      ok_count = 0;
      LOG_INFO() << "health_checker: upstream peer " << name << " recovered";
    }
    return true;
  }

  peer->OnFailure(NowMs());
  ok_count = 0;
  auto& fail_count = state->consecutive_fail[name];
  ++fail_count;
  if (fail_count >= state->cfg.unhealthy_threshold &&
      !peer->state().down.load(std::memory_order_relaxed)) {
    peer->state().down.store(true, std::memory_order_relaxed);
    LOG_WARN() << "health_checker: upstream peer " << name << " marked down";
  }
  return true;
}
}  // namespace vexo::gateway
