// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>
#include <cstdlib>
#include <string>
#include <string_view>

namespace coropact_bench {

inline constexpr std::size_t kResponseBodySize = 512;
inline constexpr std::size_t kRequestBufferSize = 16 * 1024;

inline const std::string& Response() {
  static const std::string response = [] {
    std::string value =
        "HTTP/1.1 200 OK\r\n"
        "Server: unified-http-bench\r\n"
        "Content-Type: text/plain\r\n"
        "Content-Length: 512\r\n"
        "Connection: keep-alive\r\n"
        "\r\n";
    value.append(kResponseBodySize, 'x');
    return value;
  }();
  return response;
}

inline bool HasHeaderTerminator(const char* bytes, std::size_t size) {
  for (std::size_t i = 3; i < size; ++i) {
    if (bytes[i - 3] == '\r' && bytes[i - 2] == '\n' && bytes[i - 1] == '\r' &&
        bytes[i] == '\n') {
      return true;
    }
  }
  return false;
}

inline int EnvInt(const char* key, int fallback) {
  const char* value = std::getenv(key);
  return value != nullptr ? std::atoi(value) : fallback;
}

inline std::size_t EnvSize(const char* key, std::size_t fallback) {
  const char* value = std::getenv(key);
  if (value == nullptr) return fallback;
  char* end = nullptr;
  const auto parsed = std::strtoull(value, &end, 10);
  return end != value ? static_cast<std::size_t>(parsed) : fallback;
}

}  // namespace coropact_bench
