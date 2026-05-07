#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>

namespace runtime::gateway {

struct BackendConfig {
  std::string id;     // e.g. "127.0.0.1:8080"
  std::string host;
  uint16_t port{0U};
  int weight{1};

  int max_fails{3};
  std::chrono::milliseconds fail_timeout{10000};
};

// runtime state
struct BackendRuntimeState {
  std::atomic<bool> healthy{true};
  std::atomic<int> active_requests{0};
  std::atomic<uint64_t> total_requests{0U};
  std::atomic<uint64_t> fail_count{0U};
  std::atomic<uint64_t> last_fail_time_ms{0U};
};

class Backend {
public:
  Backend(std::string host, uint16_t port, int weight = 1)
    : config_{
        .id = host + ":" + std::to_string(port),
        .host = std::move(host),
        .port = port,
        .weight = weight,
    } {}

  BackendConfig config_;
  BackendRuntimeState state_;
};

}  // namespace runtime::gateway
