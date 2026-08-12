// SPDX-License-Identifier: MIT
#pragma once

#include <cctype>
#include <cstddef>
#include <cstdlib>
#include <string>
#include <string_view>

namespace coropact_bench {

inline constexpr std::size_t kResponseBodySize = 512;
inline constexpr std::size_t kRequestBufferSize = 16 * 1024;

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

inline std::string_view EnvString(const char* key) {
  const char* value = std::getenv(key);
  return value != nullptr ? std::string_view(value) : std::string_view{};
}

inline std::size_t ResponseBodySize() {
  return EnvSize("RESPONSE_BODY", kResponseBodySize);
}

inline const std::string& Response() {
  static const std::string response = [] {
    const std::size_t body = ResponseBodySize();
    std::string value =
        "HTTP/1.1 200 OK\r\n"
        "Server: unified-http-bench\r\n"
        "Content-Type: text/plain\r\n"
        "Content-Length: ";
    value += std::to_string(body);
    value +=
        "\r\n"
        "Connection: keep-alive\r\n"
        "\r\n";
    value.append(body, 'x');
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

// Returns one-past-end index of the header block, or npos if incomplete.
inline std::size_t HeaderTerminatorEnd(const char* bytes, std::size_t size) {
  for (std::size_t i = 3; i < size; ++i) {
    if (bytes[i - 3] == '\r' && bytes[i - 2] == '\n' && bytes[i - 1] == '\r' &&
        bytes[i] == '\n') {
      return i + 1;
    }
  }
  return static_cast<std::size_t>(-1);
}

// Parses Content-Length from a complete header block. Missing/invalid => 0.
inline std::size_t ParseContentLength(const char* bytes, std::size_t size) {
  constexpr std::string_view kKey = "content-length:";
  for (std::size_t i = 0; i + kKey.size() < size; ++i) {
    if (i > 0 && bytes[i - 1] != '\n') {
      continue;
    }
    bool match = true;
    for (std::size_t j = 0; j < kKey.size(); ++j) {
      const unsigned char expected = static_cast<unsigned char>(kKey[j]);
      const unsigned char actual = static_cast<unsigned char>(bytes[i + j]);
      if (std::tolower(actual) != expected) {
        match = false;
        break;
      }
    }
    if (!match) {
      continue;
    }
    std::size_t p = i + kKey.size();
    while (p < size && (bytes[p] == ' ' || bytes[p] == '\t')) {
      ++p;
    }
    std::size_t value = 0;
    bool any = false;
    while (p < size && bytes[p] >= '0' && bytes[p] <= '9') {
      any = true;
      const std::size_t digit = static_cast<std::size_t>(bytes[p] - '0');
      if (value > (static_cast<std::size_t>(-1) - digit) / 10) {
        return 0;
      }
      value = value * 10 + digit;
      ++p;
    }
    return any ? value : 0;
  }
  return 0;
}

}  // namespace coropact_bench
