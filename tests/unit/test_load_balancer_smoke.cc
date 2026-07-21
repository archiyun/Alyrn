// LoadBalancer smoke tests: ConsistentHash ring + the basic LB strategies.
//
// Build:
//   cmake --build build-tests --target load_balancer_smoke_test -j$(nproc)
// Run:
//   ./build-tests/tests/load_balancer_smoke_test

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "vexo/gateway/load_balancer.h"
#include "vexo/gateway/upstream.h"
#include "vexo/gateway/upstream_peer.h"

namespace {

bool Expect(bool condition, const char* msg) {
  if (!condition) {
    std::cerr << "[FAIL] " << msg << '\n';
    return false;
  }
  return true;
}

void Passed(const char* name) {
  std::cout << "[PASS] " << name << '\n';
}

// Helper: build a test Upstream from a list of {peer_name, weight} pairs.
std::shared_ptr<vexo::gateway::Upstream> MakeUpstream(
    const std::string& name,
    const std::vector<std::pair<std::string, int>>& peer_weights) {
  auto upstream = std::make_shared<vexo::gateway::Upstream>(
      vexo::gateway::UpstreamConfig{.name = name});

  for (const auto& [host_port, weight] : peer_weights) {
    upstream->AddPeer(std::make_shared<vexo::gateway::UpstreamPeer>(
        vexo::gateway::UpstreamPeerConfig{
            .name = host_port,
            // host/port are deliberately fixed placeholders: these are pure
            // LB-algorithm tests that never dial a backend. ConsistentHashLB
            // keys its vnodes and fingerprint off config().name only, so the
            // peer identity that matters is `name`. Every peer shares port 80
            // (the cast just silences narrowing into the uint16_t field); the
            // name is what distinguishes them.
            .host = "127.0.0.1",
            .port = static_cast<uint16_t>(80),
            .weight = weight,
        }));
  }
  return upstream;
}

// ================================================================
// ConsistentHashLB tests
// ================================================================

bool TestConsistentHashSameKeySamePeer() {
  auto upstream = MakeUpstream("test_ch", {
      {"peer-a:80", 1}, {"peer-b:80", 1}, {"peer-c:80", 1}});
  vexo::gateway::ConsistentHashLB lb(150, "client_ip");

  vexo::gateway::RequestContext ctx{.client_ip = "10.0.0.1"};
  auto first = lb.Select(*upstream, ctx);

  // The same key must map to the same peer across 100 repeated selects.
  for (int i = 0; i < 100; i++) {
    auto peer = lb.Select(*upstream, ctx);
    if (peer != first) {
      return Expect(false, "same key must always map to same peer");
    }
  }
  Passed("TestConsistentHashSameKeySamePeer");
  return true;
}

bool TestConsistentHashDifferentKeysSpread() {
  auto upstream = MakeUpstream("test_ch", {
      {"peer-a:80", 1}, {"peer-b:80", 1}, {"peer-c:80", 1}});
  vexo::gateway::ConsistentHashLB lb(150, "client_ip");

  std::unordered_set<std::string> seen;
  for (int i = 0; i < 500; i++) {
    vexo::gateway::RequestContext ctx{.client_ip = "10.0.0." + std::to_string(i)};
    auto peer = lb.Select(*upstream, ctx);
    if (!peer) return Expect(false, "must return a peer");
    seen.insert(peer->config().name);
  }
  // 500 distinct IPs should spread across all 3 peers.
  if (!Expect(seen.size() >= 3, "500 different keys should hit at least 3 peers")) return false;
  Passed("TestConsistentHashDifferentKeysSpread");
  return true;
}

bool TestConsistentHashWeightedDistribution() {
  // peer-a weight 10 vs peer-b weight 1 -> peer-a gets ~10x the vnodes.
  auto upstream = MakeUpstream("test_ch_w", {
      {"peer-a:80", 10}, {"peer-b:80", 1}});
  vexo::gateway::ConsistentHashLB lb(150, "client_ip");

  int count_a = 0, count_b = 0;
  for (int i = 0; i < 2000; i++) {
    vexo::gateway::RequestContext ctx{.client_ip = "192.168." + std::to_string(i / 256) + "." + std::to_string(i % 256)};
    auto peer = lb.Select(*upstream, ctx);
    if (peer->config().name == "peer-a:80") count_a++;
    else count_b++;
  }

  double ratio = static_cast<double>(count_a) / static_cast<double>(count_b);
  // With a 10:1 weight, the traffic split should land near 10:1 (loose bounds).
  if (!Expect(ratio > 5.0 && ratio < 20.0,
              "weighted nodes: peer-a should get ~10x peer-b traffic")) return false;
  Passed("TestConsistentHashWeightedDistribution");
  return true;
}

bool TestConsistentHashRingRebuildOnPeerChange() {
  auto upstream = MakeUpstream("test_ch", {
      {"peer-a:80", 1}, {"peer-b:80", 1}});
  vexo::gateway::ConsistentHashLB lb(150, "client_ip");

  vexo::gateway::RequestContext ctx{.client_ip = "10.0.0.5"};
  auto before = lb.Select(*upstream, ctx);

  // Add a new peer.
  upstream->AddPeer(std::make_shared<vexo::gateway::UpstreamPeer>(
      vexo::gateway::UpstreamPeerConfig{
          .name = "peer-c:80", .host = "127.0.0.1", .port = 80, .weight = 1}));

  auto after = lb.Select(*upstream, ctx);
  // The fingerprint changed so the ring is rebuilt. The key may or may not
  // migrate to the new peer (depends on the hash) -- we only assert the ring
  // still serves a valid result.
  if (!Expect(after != nullptr, "ring must work after adding peer")) return false;
  Passed("TestConsistentHashRingRebuildOnPeerChange");
  return true;
}

bool TestConsistentHashExcludesDownPeers() {
  auto upstream = MakeUpstream("test_ch", {
      {"peer-a:80", 1}, {"peer-b:80", 1}});

  // Mark peer-a down.
  auto peers = upstream->peers();
  peers[0]->state().down.store(true);

  vexo::gateway::ConsistentHashLB lb(150, "client_ip");

  // Every request must now land on peer-b only.
  for (int i = 0; i < 200; i++) {
    vexo::gateway::RequestContext ctx{.client_ip = "10.0.0." + std::to_string(i)};
    auto peer = lb.Select(*upstream, ctx);
    if (!peer) return Expect(false, "must return a peer when at least one is up");
    if (peer->config().name != "peer-b:80") {
      return Expect(false, "down peer must be excluded");
    }
  }
  Passed("TestConsistentHashExcludesDownPeers");
  return true;
}

bool TestConsistentHashEmptyUpstreamReturnsNull() {
  auto upstream = MakeUpstream("test_ch", {});
  vexo::gateway::ConsistentHashLB lb(150, "client_ip");

  vexo::gateway::RequestContext ctx{.client_ip = "10.0.0.1"};
  auto peer = lb.Select(*upstream, ctx);
  if (!Expect(peer == nullptr, "must return nullptr for empty upstream")) return false;
  Passed("TestConsistentHashEmptyUpstreamReturnsNull");
  return true;
}

bool TestConsistentHashAllPeersDownReturnsNull() {
  auto upstream = MakeUpstream("test_ch", {{"peer-a:80", 1}});
  upstream->peers()[0]->state().down.store(true);

  vexo::gateway::ConsistentHashLB lb(150, "client_ip");
  vexo::gateway::RequestContext ctx{.client_ip = "10.0.0.1"};
  auto peer = lb.Select(*upstream, ctx);
  if (!Expect(peer == nullptr, "must return nullptr when all peers down")) return false;
  Passed("TestConsistentHashAllPeersDownReturnsNull");
  return true;
}

bool TestConsistentHashHashOnFields() {
  // URI-based routing
  auto upstream = MakeUpstream("test_ch", {
      {"peer-a:80", 1}, {"peer-b:80", 1}, {"peer-c:80", 1}});
  vexo::gateway::ConsistentHashLB lb(150, "uri");

  vexo::gateway::RequestContext ctx1{.uri = "/users/42"};
  vexo::gateway::RequestContext ctx2{.uri = "/users/42"};  // same URI
  vexo::gateway::RequestContext ctx3{.uri = "/posts/99"};   // different URI

  auto p1 = lb.Select(*upstream, ctx1);
  auto p2 = lb.Select(*upstream, ctx2);
  auto p3 = lb.Select(*upstream, ctx3);

  if (!Expect(p1 == p2, "same URI must map to same peer")) return false;
  // p3 may or may not be different, but shouldn't crash
  Passed("TestConsistentHashHashOnFields");
  return true;
}

bool TestConsistentHashVNodNodesBounds() {
  // vnodes_per_unit out of range must be clamped at construction.
  vexo::gateway::ConsistentHashLB lb_low(50, "client_ip");
  vexo::gateway::ConsistentHashLB lb_high(500, "client_ip");

  auto upstream = MakeUpstream("test_ch", {{"peer-a:80", 1}});
  vexo::gateway::RequestContext ctx{.client_ip = "10.0.0.1"};

  auto p_low = lb_low.Select(*upstream, ctx);
  auto p_high = lb_high.Select(*upstream, ctx);
  if (!Expect(p_low != nullptr, "clamped low vnodes must work")) return false;
  if (!Expect(p_high != nullptr, "clamped high vnodes must work")) return false;
  Passed("TestConsistentHashVNodNodesBounds");
  return true;
}

bool TestConsistentHashHashRingUniqueVirtualNodes() {
  // After collision dedup the ring must still be queryable (no crash).
  auto upstream = MakeUpstream("test_ch", {{"peer-a:80", 1}});
  vexo::gateway::ConsistentHashLB lb(150, "client_ip");

  for (int i = 0; i < 100; i++) {
    vexo::gateway::RequestContext ctx{.client_ip = "172.16." + std::to_string(i / 256) + "." + std::to_string(i % 256)};
    auto peer = lb.Select(*upstream, ctx);
    if (!Expect(peer != nullptr, "ring lookup must not crash after dedup")) return false;
  }
  Passed("TestConsistentHashHashRingUniqueVirtualNodes");
  return true;
}

// Stress the RCU path: N reader threads call Select lock-free while one mutator
// thread periodically flips some peers' down state. Each flip changes the
// fingerprint, continuously triggering BuildRing + atomic publish, so the read
// side (load) and write side (store) genuinely run concurrently. peer-0 stays
// up the whole time, so the fingerprint is never empty and Select always has a
// target.
//
// The real value is under TSan/ASan: it proves the ring swap is free of data
// races / use-after-free. The assertion itself is intentionally weak (non-null
// + pointer belongs to the known peer set), because an RCU reader observes some
// past snapshot -- returning a peer that may have just been marked down is the
// expected staleness, not a bug.
bool TestConsistentHashConcurrentSelectWithPeerChanges() {
  auto upstream = MakeUpstream("test_ch_mt", {
      {"peer-0:80", 1}, {"peer-1:80", 1}, {"peer-2:80", 1},
      {"peer-3:80", 1}, {"peer-4:80", 1}, {"peer-5:80", 1}});
  auto lb = std::make_shared<vexo::gateway::ConsistentHashLB>(150, "client_ip");

  // Set of valid peer pointers for the membership check (the result must be
  // one of them). The peers vector stays structurally fixed for the whole
  // test; only the per-peer `down` atomics get flipped.
  const auto& peers = upstream->peers();
  std::unordered_set<vexo::gateway::UpstreamPeer*> valid;
  for (const auto& p : peers) valid.insert(p.get());

  constexpr int kReaders = 8;
  constexpr int kItersPerReader = 50000;
  std::atomic<bool> stop{false};
  std::atomic<bool> failed{false};

  std::vector<std::thread> readers;
  readers.reserve(kReaders);
  for (int t = 0; t < kReaders; ++t) {
    readers.emplace_back([&, t] {
      for (int i = 0; i < kItersPerReader; ++i) {
        vexo::gateway::RequestContext ctx{
            .client_ip = "10." + std::to_string(t) + ".0." +
                         std::to_string(i & 0xFF)};
        auto peer = lb->Select(*upstream, ctx);
        if (!peer || valid.find(peer.get()) == valid.end()) {
          failed.store(true, std::memory_order_relaxed);
          return;
        }
      }
    });
  }

  // Mutator: flip the down state of peers 1/3/5 to force ring rebuilds;
  // peers 0/2/4 never go down so a target always exists.
  std::thread mutator([&] {
    bool down = false;
    while (!stop.load(std::memory_order_relaxed)) {
      down = !down;
      peers[1]->state().down.store(down, std::memory_order_relaxed);
      peers[3]->state().down.store(down, std::memory_order_relaxed);
      peers[5]->state().down.store(!down, std::memory_order_relaxed);
      std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
  });

  for (auto& th : readers) th.join();
  stop.store(true, std::memory_order_relaxed);
  mutator.join();

  if (!Expect(!failed.load(std::memory_order_relaxed),
              "concurrent Select must always return a valid peer")) return false;
  Passed("TestConsistentHashConcurrentSelectWithPeerChanges");
  return true;
}

// ================================================================
// MaglevHashLB tests
// ================================================================

bool TestMaglevHashSameKeySamePeer() {
  auto upstream = MakeUpstream("test_maglev", {
      {"peer-a:80", 1}, {"peer-b:80", 1}, {"peer-c:80", 1}});
  vexo::gateway::MaglevHashLB lb(257, "client_ip");

  vexo::gateway::RequestContext ctx{.client_ip = "10.0.0.1"};
  auto first = lb.Select(*upstream, ctx);
  if (!Expect(first != nullptr, "MaglevHash must return a peer")) return false;

  for (int i = 0; i < 100; i++) {
    auto peer = lb.Select(*upstream, ctx);
    if (peer != first) {
      return Expect(false, "MaglevHash: same key must always map to same peer");
    }
  }
  Passed("TestMaglevHashSameKeySamePeer");
  return true;
}

bool TestMaglevHashExcludesDownPeers() {
  auto upstream = MakeUpstream("test_maglev", {
      {"peer-a:80", 1}, {"peer-b:80", 1}});
  upstream->peers()[0]->state().down.store(true);

  vexo::gateway::MaglevHashLB lb(257, "client_ip");
  for (int i = 0; i < 200; i++) {
    vexo::gateway::RequestContext ctx{.client_ip = "10.0.1." + std::to_string(i)};
    auto peer = lb.Select(*upstream, ctx);
    if (!peer) return Expect(false, "MaglevHash must return a peer when one is up");
    if (peer->config().name != "peer-b:80") {
      return Expect(false, "MaglevHash must exclude down peers");
    }
  }
  Passed("TestMaglevHashExcludesDownPeers");
  return true;
}

bool TestMaglevHashAllPeersDownReturnsNull() {
  auto upstream = MakeUpstream("test_maglev", {{"peer-a:80", 1}});
  upstream->peers()[0]->state().down.store(true);

  vexo::gateway::MaglevHashLB lb(257, "client_ip");
  vexo::gateway::RequestContext ctx{.client_ip = "10.0.0.1"};
  auto peer = lb.Select(*upstream, ctx);
  if (!Expect(peer == nullptr, "MaglevHash must return nullptr when all peers down"))
    return false;
  Passed("TestMaglevHashAllPeersDownReturnsNull");
  return true;
}

// ================================================================
// IPHashLB tests
// ================================================================

bool TestIPHashSameIPSamePeer() {
  auto upstream = MakeUpstream("test_iph", {
      {"peer-a:80", 1}, {"peer-b:80", 1}, {"peer-c:80", 1}});
  vexo::gateway::IPHashLB lb;

  vexo::gateway::RequestContext ctx{.client_ip = "192.168.1.100"};
  auto first = lb.Select(*upstream, ctx);
  for (int i = 0; i < 100; i++) {
    if (lb.Select(*upstream, ctx) != first) {
      return Expect(false, "IPHash: same IP must map to same peer");
    }
  }
  Passed("TestIPHashSameIPSamePeer");
  return true;
}

bool TestIPHashEmptyIPUsesFallback() {
  auto upstream = MakeUpstream("test_iph", {{"peer-a:80", 1}, {"peer-b:80", 1}});
  vexo::gateway::IPHashLB lb;

  vexo::gateway::RequestContext ctx;  // empty client_ip
  auto peer = lb.Select(*upstream, ctx);
  if (!Expect(peer != nullptr, "IPHash must use fallback for empty IP")) return false;
  Passed("TestIPHashEmptyIPUsesFallback");
  return true;
}

// ================================================================
// RoundRobinLB tests
// ================================================================

bool TestRoundRobinCycles() {
  auto upstream = MakeUpstream("test_rr", {
      {"peer-a:80", 1}, {"peer-b:80", 1}, {"peer-c:80", 1}});
  vexo::gateway::RoundRobinLB lb;

  auto p0 = lb.Select(*upstream);
  auto p1 = lb.Select(*upstream);
  auto p2 = lb.Select(*upstream);
  auto p3 = lb.Select(*upstream);

  if (!Expect(p0->config().name != p1->config().name || p1->config().name != p2->config().name,
              "RR: consecutive selects should cycle")) return false;
  Passed("TestRoundRobinCycles");
  return true;
}

bool TestRoundRobinHonorsPassiveFailTimeout() {
  auto upstream = std::make_shared<vexo::gateway::Upstream>(
      vexo::gateway::UpstreamConfig{.name = "test_rr_passive"});
  auto cooling = std::make_shared<vexo::gateway::UpstreamPeer>(
      vexo::gateway::UpstreamPeerConfig{
          .name = "cooling:80",
          .host = "127.0.0.1",
          .port = static_cast<uint16_t>(80),
          .weight = 1,
          .max_fails = 1,
          .fail_timeout = std::chrono::hours(1),
      });
  auto healthy = std::make_shared<vexo::gateway::UpstreamPeer>(
      vexo::gateway::UpstreamPeerConfig{
          .name = "healthy:80",
          .host = "127.0.0.1",
          .port = static_cast<uint16_t>(80),
          .weight = 1,
      });
  upstream->AddPeer(cooling);
  upstream->AddPeer(healthy);

  const auto now_ms = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
  cooling->OnFailure(now_ms);

  vexo::gateway::RoundRobinLB lb;
  for (int i = 0; i < 10; ++i) {
    auto peer = lb.Select(*upstream);
    if (!peer) return Expect(false, "RR must return the healthy peer");
    if (peer->config().name != "healthy:80") {
      return Expect(false, "RR must exclude peers inside passive fail_timeout");
    }
  }
  Passed("TestRoundRobinHonorsPassiveFailTimeout");
  return true;
}

// Regression guard for the passive-recovery bug: a peer that trips max_fails on
// the *proxy* path (with no active gateway health loop) must recover on its own.
// Previously the proxy set state_.down on max_fails, which made AvailableAt()
// short-circuit to false forever — the fail_timeout cooldown window was
// unreachable and the peer stayed stranded until an active checker cleared
// `down`. AvailableAt() takes now_ms explicitly, so the whole window is driven
// with synthetic timestamps (no real sleeping, fully deterministic).
bool TestPassivePeerRecoversAfterFailTimeout() {
  vexo::gateway::UpstreamPeer peer(vexo::gateway::UpstreamPeerConfig{
      .name = "passive:80",
      .host = "127.0.0.1",
      .port = static_cast<uint16_t>(80),
      .weight = 4,
      .max_fails = 2,
      .fail_timeout = std::chrono::milliseconds(100),
  });

  constexpr uint64_t t0 = 1'000'000;
  if (!Expect(peer.AvailableAt(t0), "fresh peer must be available")) return false;
  if (!Expect(peer.effective_weight() == 4,
              "fresh peer starts at config weight")) return false;

  // One failure is below max_fails: still eligible.
  peer.OnFailure(t0);
  if (!Expect(peer.AvailableAt(t0), "1 < max_fails must stay available")) return false;

  // Second failure reaches max_fails -> enters the fail_timeout cooldown.
  peer.OnFailure(t0);
  if (!Expect(!peer.state().down.load(),
              "passive max_fails must NOT set the active down flag")) return false;
  if (!Expect(!peer.AvailableAt(t0),
              "peer must be excluded at the start of the cooldown")) return false;
  if (!Expect(!peer.AvailableAt(t0 + 50),
              "peer must stay excluded mid-cooldown (50ms < 100ms)")) return false;

  // Cooldown elapsed: the peer comes back WITHOUT any active health check.
  // This is the exact re-inclusion the bug made impossible.
  if (!Expect(peer.AvailableAt(t0 + 100),
              "peer must recover once fail_timeout elapses")) return false;

  // A successful round-trip clears passive fails and climbs effective_weight
  // back so SWRR re-promotes the recovered peer.
  peer.OnSuccess();
  if (!Expect(peer.AvailableAt(t0),
              "OnSuccess must make the peer immediately available")) return false;
  if (!Expect(peer.effective_weight() == 3,
              "OnSuccess must restore effective_weight toward config weight")) {
    return false;
  }
  peer.OnSuccess();
  if (!Expect(peer.effective_weight() == 4,
              "effective_weight is capped at config weight")) return false;

  Passed("TestPassivePeerRecoversAfterFailTimeout");
  return true;
}

// ================================================================
// WeightedLeastConnectionLB tests
// ================================================================

// Picks the peer with the lowest active/weight ratio. A heavier peer must be
// chosen even at a higher raw active count, as long as its load ratio is lower.
// Constructed so an inverted comparison (picking the *most* loaded peer) fails.
bool TestWeightedLeastConnectionPicksLowestRatio() {
  auto upstream = MakeUpstream("test_wlc", {{"light:80", 1}, {"heavy:80", 3}});
  auto peers = upstream->peers();
  // light: 2 active / weight 1 = 2.00    heavy: 4 active / weight 3 = 1.33
  peers[0]->state().active.store(2);
  peers[1]->state().active.store(4);

  vexo::gateway::WeightedLeastConnectionLB lb;
  auto pick = lb.Select(*upstream);
  if (!pick) return Expect(false, "WLC must return a peer");
  if (!Expect(pick->config().name == "heavy:80",
              "WLC must pick the lower active/weight ratio (heavy), not the lower raw active"))
    return false;
  Passed("TestWeightedLeastConnectionPicksLowestRatio");
  return true;
}

// With equal weights it degenerates to plain least-connection: lowest active wins.
bool TestWeightedLeastConnectionEqualWeightLowestActive() {
  auto upstream = MakeUpstream("test_wlc", {{"a:80", 1}, {"b:80", 1}, {"c:80", 1}});
  auto peers = upstream->peers();
  peers[0]->state().active.store(5);
  peers[1]->state().active.store(1);
  peers[2]->state().active.store(9);

  vexo::gateway::WeightedLeastConnectionLB lb;
  auto pick = lb.Select(*upstream);
  if (!pick) return Expect(false, "WLC must return a peer");
  if (!Expect(pick->config().name == "b:80", "WLC equal weights must pick the lowest active"))
    return false;
  Passed("TestWeightedLeastConnectionEqualWeightLowestActive");
  return true;
}

// Down peers are excluded even when they carry the higher weight.
bool TestWeightedLeastConnectionExcludesDownPeers() {
  auto upstream = MakeUpstream("test_wlc", {{"a:80", 5}, {"b:80", 1}});
  auto peers = upstream->peers();
  peers[0]->state().down.store(true);  // a is down despite the higher weight
  peers[1]->state().active.store(7);

  vexo::gateway::WeightedLeastConnectionLB lb;
  auto pick = lb.Select(*upstream);
  if (!pick) return Expect(false, "WLC must return the only available peer");
  if (!Expect(pick->config().name == "b:80", "WLC must skip down peers")) return false;
  Passed("TestWeightedLeastConnectionExcludesDownPeers");
  return true;
}

// ================================================================
// CreateLoadBalancer factory tests
// ================================================================

bool TestCreateLoadBalancerAllAlgos() {
  const char* algos[] = {
    "round_robin", "smooth_weighted_round_robin", "least_connection",
    "weighted_least_connection", "random", "weighted_random",
    "ip_hash", "consistent_hash", "maglev_hash", "p2c"
  };
  for (const auto& algo : algos) {
    auto lb = vexo::gateway::CreateLoadBalancer(algo);
    if (!lb) {
      std::string msg = "CreateLoadBalancer must return non-null for ";
      msg += algo;
      return Expect(false, msg.c_str());
    }
  }
  auto unknown = vexo::gateway::CreateLoadBalancer("bogus");
  if (!Expect(unknown == nullptr, "unknown algo must return nullptr")) return false;
  Passed("TestCreateLoadBalancerAllAlgos");
  return true;
}

// ================================================================
// RequestContext default construction
// ================================================================

bool TestRequestContextDefaultEmpty() {
  vexo::gateway::RequestContext ctx;
  if (!Expect(ctx.client_ip.empty(), "default client_ip must be empty")) return false;
  if (!Expect(ctx.uri.empty(), "default uri must be empty")) return false;
  Passed("TestRequestContextDefaultEmpty");
  return true;
}

}  // namespace

int main() {
  int passed = 0;
  int total = 0;

#define RUN(test) do { ++total; if (test()) ++passed; } while(0)

  RUN(TestConsistentHashSameKeySamePeer);
  RUN(TestConsistentHashDifferentKeysSpread);
  RUN(TestConsistentHashWeightedDistribution);
  RUN(TestConsistentHashRingRebuildOnPeerChange);
  RUN(TestConsistentHashExcludesDownPeers);
  RUN(TestConsistentHashEmptyUpstreamReturnsNull);
  RUN(TestConsistentHashAllPeersDownReturnsNull);
  RUN(TestConsistentHashHashOnFields);
  RUN(TestConsistentHashVNodNodesBounds);
  RUN(TestConsistentHashHashRingUniqueVirtualNodes);
  RUN(TestConsistentHashConcurrentSelectWithPeerChanges);

  RUN(TestMaglevHashSameKeySamePeer);
  RUN(TestMaglevHashExcludesDownPeers);
  RUN(TestMaglevHashAllPeersDownReturnsNull);

  RUN(TestIPHashSameIPSamePeer);
  RUN(TestIPHashEmptyIPUsesFallback);

  RUN(TestRoundRobinCycles);
  RUN(TestRoundRobinHonorsPassiveFailTimeout);
  RUN(TestPassivePeerRecoversAfterFailTimeout);

  RUN(TestWeightedLeastConnectionPicksLowestRatio);
  RUN(TestWeightedLeastConnectionEqualWeightLowestActive);
  RUN(TestWeightedLeastConnectionExcludesDownPeers);

  RUN(TestCreateLoadBalancerAllAlgos);
  RUN(TestRequestContextDefaultEmpty);

  std::cout << "===========================\n";
  std::cout << passed << "/" << total << " tests passed.\n";

  return (passed == total) ? 0 : 1;
}
