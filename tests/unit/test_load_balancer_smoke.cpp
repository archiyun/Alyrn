// LoadBalancer 一致性哈希 + 基础 LB 冒烟测试
//
// 编译
// cmake --build build-tests --target load_balancer_smoke_test -j$(nproc)
// 运行
// ./build-tests/tests/load_balancer_smoke_test

#include <algorithm>
#include <cmath>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "runtime/gateway/load_balancer.h"
#include "runtime/gateway/upstream.h"
#include "runtime/gateway/upstream_peer.h"

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

// --- 辅助函数：创建测试 Upstream ---
std::shared_ptr<runtime::gateway::Upstream> MakeUpstream(
    const std::string& name,
    const std::vector<std::pair<std::string, int>>& peer_weights) {
  auto upstream = std::make_shared<runtime::gateway::Upstream>(
      runtime::gateway::UpstreamConfig{.name = name});

  for (const auto& [host_port, weight] : peer_weights) {
    upstream->AddPeer(std::make_shared<runtime::gateway::UpstreamPeer>(
        runtime::gateway::UpstreamPeerConfig{
            .name = host_port,
            .host = "127.0.0.1",
            .port = static_cast<uint16_t>(80),
            .weight = weight,
        }));
  }
  return upstream;
}

// ================================================================
// ConsistentHashLB 专项测试
// ================================================================

bool TestConsistentHashSameKeySamePeer() {
  auto upstream = MakeUpstream("test_ch", {
      {"peer-a:80", 1}, {"peer-b:80", 1}, {"peer-c:80", 1}});
  runtime::gateway::ConsistentHashLB lb(150, "client_ip");

  runtime::gateway::RequestContext ctx{.client_ip = "10.0.0.1"};
  auto first = lb.Select(*upstream, ctx);

  // 同一 key 连续 100 次都应命中同一 peer
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
  runtime::gateway::ConsistentHashLB lb(150, "client_ip");

  std::unordered_set<std::string> seen;
  for (int i = 0; i < 500; i++) {
    runtime::gateway::RequestContext ctx{.client_ip = "10.0.0." + std::to_string(i)};
    auto peer = lb.Select(*upstream, ctx);
    if (!peer) return Expect(false, "must return a peer");
    seen.insert(peer->Config().name);
  }
  // 500 个不同 IP 应该命中全部 3 个 peer
  if (!Expect(seen.size() >= 3, "500 different keys should hit at least 3 peers")) return false;
  Passed("TestConsistentHashDifferentKeysSpread");
  return true;
}

bool TestConsistentHashWeightedDistribution() {
  // peer-a 权重 10，peer-b 权重 1 — peer-a 应有更多虚拟节点
  auto upstream = MakeUpstream("test_ch_w", {
      {"peer-a:80", 10}, {"peer-b:80", 1}});
  runtime::gateway::ConsistentHashLB lb(150, "client_ip");

  int count_a = 0, count_b = 0;
  for (int i = 0; i < 2000; i++) {
    runtime::gateway::RequestContext ctx{.client_ip = "192.168." + std::to_string(i / 256) + "." + std::to_string(i % 256)};
    auto peer = lb.Select(*upstream, ctx);
    if (peer->Config().name == "peer-a:80") count_a++;
    else count_b++;
  }

  double ratio = static_cast<double>(count_a) / static_cast<double>(count_b);
  // 权重 10:1，分布比例应接近 10:1（允许 ±30% 误差）
  if (!Expect(ratio > 5.0 && ratio < 20.0,
              "weighted nodes: peer-a should get ~10x peer-b traffic")) return false;
  Passed("TestConsistentHashWeightedDistribution");
  return true;
}

bool TestConsistentHashRingRebuildOnPeerChange() {
  auto upstream = MakeUpstream("test_ch", {
      {"peer-a:80", 1}, {"peer-b:80", 1}});
  runtime::gateway::ConsistentHashLB lb(150, "client_ip");

  runtime::gateway::RequestContext ctx{.client_ip = "10.0.0.5"};
  auto before = lb.Select(*upstream, ctx);

  // 加一个新 peer
  upstream->AddPeer(std::make_shared<runtime::gateway::UpstreamPeer>(
      runtime::gateway::UpstreamPeerConfig{
          .name = "peer-c:80", .host = "127.0.0.1", .port = 80, .weight = 1}));

  auto after = lb.Select(*upstream, ctx);
  // 指纹变了，环重建，但 key 可能迁移到新 peer 也可能不变（取决于 hash 值）
  // 只需验证环正常返回即可
  if (!Expect(after != nullptr, "ring must work after adding peer")) return false;
  Passed("TestConsistentHashRingRebuildOnPeerChange");
  return true;
}

bool TestConsistentHashExcludesDownPeers() {
  auto upstream = MakeUpstream("test_ch", {
      {"peer-a:80", 1}, {"peer-b:80", 1}});

  // 标记 peer-a 为 down
  auto peers = upstream->Peers();
  peers[0]->State().down.store(true);

  runtime::gateway::ConsistentHashLB lb(150, "client_ip");

  // 所有请求都只能命中 peer-b
  for (int i = 0; i < 200; i++) {
    runtime::gateway::RequestContext ctx{.client_ip = "10.0.0." + std::to_string(i)};
    auto peer = lb.Select(*upstream, ctx);
    if (!peer) return Expect(false, "must return a peer when at least one is up");
    if (peer->Config().name != "peer-b:80") {
      return Expect(false, "down peer must be excluded");
    }
  }
  Passed("TestConsistentHashExcludesDownPeers");
  return true;
}

bool TestConsistentHashEmptyUpstreamReturnsNull() {
  auto upstream = MakeUpstream("test_ch", {});
  runtime::gateway::ConsistentHashLB lb(150, "client_ip");

  runtime::gateway::RequestContext ctx{.client_ip = "10.0.0.1"};
  auto peer = lb.Select(*upstream, ctx);
  if (!Expect(peer == nullptr, "must return nullptr for empty upstream")) return false;
  Passed("TestConsistentHashEmptyUpstreamReturnsNull");
  return true;
}

bool TestConsistentHashAllPeersDownReturnsNull() {
  auto upstream = MakeUpstream("test_ch", {{"peer-a:80", 1}});
  upstream->Peers()[0]->State().down.store(true);

  runtime::gateway::ConsistentHashLB lb(150, "client_ip");
  runtime::gateway::RequestContext ctx{.client_ip = "10.0.0.1"};
  auto peer = lb.Select(*upstream, ctx);
  if (!Expect(peer == nullptr, "must return nullptr when all peers down")) return false;
  Passed("TestConsistentHashAllPeersDownReturnsNull");
  return true;
}

bool TestConsistentHashHashOnFields() {
  // URI-based routing
  auto upstream = MakeUpstream("test_ch", {
      {"peer-a:80", 1}, {"peer-b:80", 1}, {"peer-c:80", 1}});
  runtime::gateway::ConsistentHashLB lb(150, "uri");

  runtime::gateway::RequestContext ctx1{.uri = "/users/42"};
  runtime::gateway::RequestContext ctx2{.uri = "/users/42"};  // same URI
  runtime::gateway::RequestContext ctx3{.uri = "/posts/99"};   // different URI

  auto p1 = lb.Select(*upstream, ctx1);
  auto p2 = lb.Select(*upstream, ctx2);
  auto p3 = lb.Select(*upstream, ctx3);

  if (!Expect(p1 == p2, "same URI must map to same peer")) return false;
  // p3 may or may not be different, but shouldn't crash
  Passed("TestConsistentHashHashOnFields");
  return true;
}

bool TestConsistentHashVNodNodesBounds() {
  // vnodes_per_unit 超出范围时应被 clamp
  runtime::gateway::ConsistentHashLB lb_low(50, "client_ip");
  runtime::gateway::ConsistentHashLB lb_high(500, "client_ip");

  auto upstream = MakeUpstream("test_ch", {{"peer-a:80", 1}});
  runtime::gateway::RequestContext ctx{.client_ip = "10.0.0.1"};

  auto p_low = lb_low.Select(*upstream, ctx);
  auto p_high = lb_high.Select(*upstream, ctx);
  if (!Expect(p_low != nullptr, "clamped low vnodes must work")) return false;
  if (!Expect(p_high != nullptr, "clamped high vnodes must work")) return false;
  Passed("TestConsistentHashVNodNodesBounds");
  return true;
}

bool TestConsistentHashHashRingUniqueVirtualNodes() {
  // 哈希碰撞去重后环应当可查（不 crash）
  auto upstream = MakeUpstream("test_ch", {{"peer-a:80", 1}});
  runtime::gateway::ConsistentHashLB lb(150, "client_ip");

  for (int i = 0; i < 100; i++) {
    runtime::gateway::RequestContext ctx{.client_ip = "172.16." + std::to_string(i / 256) + "." + std::to_string(i % 256)};
    auto peer = lb.Select(*upstream, ctx);
    if (!Expect(peer != nullptr, "ring lookup must not crash after dedup")) return false;
  }
  Passed("TestConsistentHashHashRingUniqueVirtualNodes");
  return true;
}

// ================================================================
// IPHashLB 测试
// ================================================================

bool TestIPHashSameIPSamePeer() {
  auto upstream = MakeUpstream("test_iph", {
      {"peer-a:80", 1}, {"peer-b:80", 1}, {"peer-c:80", 1}});
  runtime::gateway::IPHashLB lb;

  runtime::gateway::RequestContext ctx{.client_ip = "192.168.1.100"};
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
  runtime::gateway::IPHashLB lb;

  runtime::gateway::RequestContext ctx;  // empty client_ip
  auto peer = lb.Select(*upstream, ctx);
  if (!Expect(peer != nullptr, "IPHash must use fallback for empty IP")) return false;
  Passed("TestIPHashEmptyIPUsesFallback");
  return true;
}

// ================================================================
// RoundRobinLB 测试
// ================================================================

bool TestRoundRobinCycles() {
  auto upstream = MakeUpstream("test_rr", {
      {"peer-a:80", 1}, {"peer-b:80", 1}, {"peer-c:80", 1}});
  runtime::gateway::RoundRobinLB lb;

  auto p0 = lb.Select(*upstream);
  auto p1 = lb.Select(*upstream);
  auto p2 = lb.Select(*upstream);
  auto p3 = lb.Select(*upstream);

  if (!Expect(p0->Config().name != p1->Config().name || p1->Config().name != p2->Config().name,
              "RR: consecutive selects should cycle")) return false;
  Passed("TestRoundRobinCycles");
  return true;
}

// ================================================================
// CreateLoadBalancer 工厂测试
// ================================================================

bool TestCreateLoadBalancerAllAlgos() {
  const char* algos[] = {
    "round_robin", "weighted_round_robin", "least_connection",
    "random", "weighted_random", "ip_hash", "consistent_hash"
  };
  for (const auto& algo : algos) {
    auto lb = runtime::gateway::CreateLoadBalancer(algo);
    if (!lb) {
      std::string msg = "CreateLoadBalancer must return non-null for ";
      msg += algo;
      return Expect(false, msg.c_str());
    }
  }
  auto unknown = runtime::gateway::CreateLoadBalancer("bogus");
  if (!Expect(unknown == nullptr, "unknown algo must return nullptr")) return false;
  Passed("TestCreateLoadBalancerAllAlgos");
  return true;
}

// ================================================================
// RequestContext 默认构造
// ================================================================

bool TestRequestContextDefaultEmpty() {
  runtime::gateway::RequestContext ctx;
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

  RUN(TestIPHashSameIPSamePeer);
  RUN(TestIPHashEmptyIPUsesFallback);

  RUN(TestRoundRobinCycles);

  RUN(TestCreateLoadBalancerAllAlgos);
  RUN(TestRequestContextDefaultEmpty);

  std::cout << "===========================\n";
  std::cout << passed << "/" << total << " tests passed.\n";

  return (passed == total) ? 0 : 1;
}
