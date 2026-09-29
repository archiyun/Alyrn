// SPDX-License-Identifier: MIT
//
// Randomized robustness test for net::ParseIpAddress and Endpoint formatting.
// Adversarial inputs (embedded nulls, over-long strings, malformed IPs) must
// never crash or trip UB; a parse that succeeds must round-trip: its canonical
// text reparses to an equal Endpoint, and WithPort replaces only the port.
// Fixed seeds keep it deterministic. Under the sanitizer build this also guards
// the parser's memory handling.

#include <cstdint>
#include <cstdio>
#include <random>
#include <string>

#include "alyrn/net/endpoint.h"

namespace {

using alyrn::net::Endpoint;
using alyrn::net::ParseIpAddress;

bool RoundTrip(const std::string& ip, std::uint16_t port) {
  auto endpoint = ParseIpAddress(ip, port);
  if (!endpoint.HasValue()) return true;  // rejection is a valid outcome

  const std::string canonical = endpoint->ToIp();
  (void)endpoint->ToIpPort();  // must not crash on a parsed (v4/v6) address
  if (endpoint->ToPort() != port) {
    std::printf("FAIL port: ip='%s' port=%u got=%u\n", ip.c_str(), port, endpoint->ToPort());
    return false;
  }
  auto again = ParseIpAddress(canonical, port);
  if (!again.HasValue() || !(*again == *endpoint)) {
    std::printf("FAIL round-trip: ip='%s' canonical='%s'\n", ip.c_str(), canonical.c_str());
    return false;
  }
  const std::uint16_t other = static_cast<std::uint16_t>(port ^ 0x5a5a);
  auto with_port = endpoint->WithPort(other);
  if (with_port.ToPort() != other || with_port.ToIp() != canonical) {
    std::printf("FAIL WithPort: ip='%s'\n", ip.c_str());
    return false;
  }
  return true;
}

bool FixedCases() {
  const char* cases[] = {
      "",         ".",         ":",          "::",          "::1",
      "0.0.0.0",  "255.255.255.255",         "256.1.1.1",   "1.2.3.4.5",
      "1.2.3",    "127.0.0.1 ", " 127.0.0.1", "0x7f.0.0.1",  "::ffff:127.0.0.1",
      "fe80::1%eth0", "1::2::3", "12345678901234567890",     "999999999999999999999999",
      "[::1]",    "1.2.3.04",  "1.2.3.4\n",  "01.02.03.04"};
  for (const char* c : cases) {
    if (!RoundTrip(c, 8080)) return false;
  }
  std::string long_digits(5000, '1');
  if (!RoundTrip(long_digits, 8080)) return false;
  std::string embedded_null("127.0.0.1");
  embedded_null.push_back('\0');
  embedded_null += "junk";
  return RoundTrip(embedded_null, 80);
}

bool RandomBattery(std::uint64_t seed, std::uint64_t iters) {
  std::mt19937_64 rng(seed);
  static constexpr char kPool[] = "0123456789.:abcdefABCDEF[]xX/%gG \t-";
  for (std::uint64_t i = 0; i < iters; ++i) {
    const std::size_t len = rng() % ((i % 97 == 0) ? 4100 : 24);
    std::string text;
    text.reserve(len);
    for (std::size_t k = 0; k < len; ++k) text.push_back(kPool[rng() % (sizeof(kPool) - 1)]);
    if (rng() % 50 == 0) text.push_back('\0');
    if (!RoundTrip(text, static_cast<std::uint16_t>(rng()))) return false;
  }
  return true;
}

}  // namespace

int main() {
  if (!FixedCases()) return 1;
  for (std::uint64_t seed = 1; seed <= 4; ++seed) {
    if (!RandomBattery(seed, 100000)) return 1;
  }
  std::printf("endpoint parse fuzz: PASS\n");
  return 0;
}
