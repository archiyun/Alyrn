// Upstream connection pool smoke test.

#include <chrono>
#include <cstddef>
#include <iostream>
#include <optional>
#include <span>
#include <thread>
#include <utility>

#include "coropact/base/error.h"
#include "coropact/coro/task.h"
#include "coropact/gateway/upstream_conn_pool.h"

namespace {

class FakeStream {
public:
  explicit FakeStream(int id) : id_(id) {}

  int id() const noexcept { return id_; }

  coropact::coro::Task<coropact::base::Result<std::size_t>> ReadSome(std::span<std::byte>) {
    co_return std::size_t{0};
  }

  coropact::coro::Task<coropact::base::Result<std::size_t>> WriteSome(std::span<const std::byte>) {
    co_return std::size_t{0};
  }

  coropact::coro::Task<coropact::base::Result<void>> Shutdown() { co_return coropact::base::Result<void>{}; }
  coropact::coro::Task<coropact::base::Result<void>> Close() { co_return coropact::base::Result<void>{}; }

private:
  int id_;
};

using Pool = coropact::gateway::UpstreamStreamPool<FakeStream>;

bool Expect(bool condition, const char* message) {
  if (!condition) std::cerr << "[FAIL] " << message << '\n';
  return condition;
}

std::optional<FakeStream> Take(Pool& pool, const coropact::gateway::UpstreamPeer* peer) {
  return pool.Acquire(peer);
}

bool TestPeerIsolationAndCapacity() {
  coropact::gateway::UpstreamPeer first({.name = "first", .host = "127.0.0.1", .port = 9001});
  coropact::gateway::UpstreamPeer second({.name = "second", .host = "127.0.0.1", .port = 9002});
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
  coropact::gateway::UpstreamPeer peer({.name = "peer", .host = "127.0.0.1", .port = 9001});

  Pool disabled({.max_idle_per_peer = 0});
  disabled.Release(&peer, std::optional<FakeStream>{FakeStream(1)});
  bool ok = Expect(!Take(disabled, &peer), "zero capacity should not retain a stream");

  Pool expiring({.max_idle_per_peer = 1, .keepalive_timeout_sec = 0.0});
  expiring.Release(&peer, std::optional<FakeStream>{FakeStream(2)});
  expiring.EvictStale();
  ok &= Expect(!Take(expiring, &peer), "expired idle streams should be evicted");
  return ok;
}

bool TestSharedCapacity() {
  coropact::gateway::UpstreamPeer first({.name = "first", .host = "127.0.0.1", .port = 9001});
  coropact::gateway::UpstreamPeer second({.name = "second", .host = "127.0.0.1", .port = 9002});
  Pool pool({.max_idle_per_peer = 0, .max_idle_total = 3});

  pool.Release(&first, std::optional<FakeStream>{FakeStream(1)});
  std::this_thread::sleep_for(std::chrono::milliseconds(1));
  pool.Release(&first, std::optional<FakeStream>{FakeStream(2)});
  pool.Release(&second, std::optional<FakeStream>{FakeStream(3)});
  pool.Release(&second, std::optional<FakeStream>{FakeStream(4)});

  bool ok = Expect(pool.idle_count() == 3, "shared budget should cap all peer queues together");
  auto first_stream = Take(pool, &first);
  auto second_stream = Take(pool, &second);
  ok &= Expect(first_stream && first_stream->id() == 2,
               "shared budget should preserve per-peer LIFO reuse");
  ok &= Expect(second_stream && second_stream->id() == 4,
               "shared budget should evict the globally oldest idle stream");
  ok &= Expect(pool.idle_count() == 1, "acquiring streams should reduce the shared idle count");
  return ok;
}

bool TestMovePreservesEntries() {
  coropact::gateway::UpstreamPeer first({.name = "first", .host = "127.0.0.1", .port = 9001});
  coropact::gateway::UpstreamPeer second({.name = "second", .host = "127.0.0.1", .port = 9002});

  Pool source({.max_idle_per_peer = 2});
  source.Release(&first, std::optional<FakeStream>{FakeStream(1)});
  source.Release(&second, std::optional<FakeStream>{FakeStream(2)});

  Pool moved(std::move(source));
  bool ok = Expect(!Take(source, &first), "moved-from pool should be empty");
  auto first_stream = Take(moved, &first);
  auto second_stream = Take(moved, &second);
  ok &= Expect(first_stream && first_stream->id() == 1,
               "move construction should preserve the first peer entry");
  ok &= Expect(second_stream && second_stream->id() == 2,
               "move construction should preserve the second peer entry");

  Pool assigned;
  assigned.Release(&first, std::optional<FakeStream>{FakeStream(3)});
  Pool replacement;
  replacement.Release(&second, std::optional<FakeStream>{FakeStream(4)});
  assigned = std::move(replacement);
  auto assigned_stream = Take(assigned, &second);
  ok &= Expect(assigned_stream && assigned_stream->id() == 4,
               "move assignment should transfer peer entries");
  ok &= Expect(!Take(replacement, &second), "move-assigned-from pool should be empty");
  return ok;
}

}  // namespace

int main() {
  const bool ok =
      TestPeerIsolationAndCapacity() && TestZeroCapacityAndStaleEviction() &&
      TestSharedCapacity() && TestMovePreservesEntries();
  if (ok) std::cout << "[PASS] upstream connection pool smoke tests\n";
  return ok ? 0 : 1;
}
