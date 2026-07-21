// Upstream connection pool smoke test.

#include <cstddef>
#include <iostream>
#include <optional>
#include <span>

#include "vexo/base/error.h"
#include "vexo/coro/task.h"
#include "vexo/gateway/upstream_conn_pool.h"

namespace {

class FakeStream {
public:
  explicit FakeStream(int id) : id_(id) {}

  int id() const noexcept { return id_; }

  vexo::coro::Task<vexo::base::Result<std::size_t>> ReadSome(std::span<std::byte>) {
    co_return std::size_t{0};
  }

  vexo::coro::Task<vexo::base::Result<std::size_t>> WriteSome(std::span<const std::byte>) {
    co_return std::size_t{0};
  }

  vexo::coro::Task<vexo::base::Result<void>> Shutdown() { co_return vexo::base::Result<void>{}; }
  vexo::coro::Task<vexo::base::Result<void>> Close() { co_return vexo::base::Result<void>{}; }

private:
  int id_;
};

using Pool = vexo::gateway::UpstreamStreamPool<FakeStream>;

bool Expect(bool condition, const char* message) {
  if (!condition) std::cerr << "[FAIL] " << message << '\n';
  return condition;
}

std::optional<FakeStream> Take(Pool& pool, const vexo::gateway::UpstreamPeer* peer) {
  return pool.Acquire(peer);
}

bool TestPeerIsolationAndCapacity() {
  vexo::gateway::UpstreamPeer first({.name = "first", .host = "127.0.0.1", .port = 9001});
  vexo::gateway::UpstreamPeer second({.name = "second", .host = "127.0.0.1", .port = 9002});
  Pool pool({.max_idle_per_peer = 2});

  pool.Release(&first, std::optional<FakeStream>{FakeStream(1)});
  pool.Release(&first, std::optional<FakeStream>{FakeStream(2)});
  pool.Release(&first, std::optional<FakeStream>{FakeStream(3)});
  pool.Release(&second, std::optional<FakeStream>{FakeStream(4)});

  auto first_stream = Take(pool, &first);
  auto second_first = Take(pool, &first);
  auto second_peer_stream = Take(pool, &second);
  auto exhausted = Take(pool, &first);

  bool ok = Expect(first_stream && first_stream->id() == 2,
                   "pool should reuse the most recently released stream");
  ok &= Expect(second_first && second_first->id() == 1,
               "pool should retain the earlier stream up to its capacity");
  ok &= Expect(second_peer_stream && second_peer_stream->id() == 4,
               "peer idle streams must remain isolated");
  ok &= Expect(!exhausted, "pool should be empty after its retained streams are acquired");
  return ok;
}

bool TestZeroCapacityAndStaleEviction() {
  vexo::gateway::UpstreamPeer peer({.name = "peer", .host = "127.0.0.1", .port = 9001});

  Pool disabled({.max_idle_per_peer = 0});
  disabled.Release(&peer, std::optional<FakeStream>{FakeStream(1)});
  bool ok = Expect(!Take(disabled, &peer), "zero capacity should not retain a stream");

  Pool expiring({.max_idle_per_peer = 1, .keepalive_timeout_sec = 0.0});
  expiring.Release(&peer, std::optional<FakeStream>{FakeStream(2)});
  expiring.EvictStale();
  ok &= Expect(!Take(expiring, &peer), "expired idle streams should be evicted");
  return ok;
}

}  // namespace

int main() {
  const bool ok = TestPeerIsolationAndCapacity() && TestZeroCapacityAndStaleEviction();
  if (ok) std::cout << "[PASS] upstream connection pool smoke tests\n";
  return ok ? 0 : 1;
}
