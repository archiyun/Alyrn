// Copyright (c) 2026 Aresna
// SPDX-License-Identifier: MIT
#pragma once

#include "runtime/http/http_types.h"

#include <array>
#include <cctype>
#include <cstddef>
#include <ctime>
#include <memory_resource>
#include <string>
#include <string_view>

namespace runtime::http::detail {

inline constexpr std::string_view kServerSignature = "aresna/0.1";

inline void ToLower(std::string& s) noexcept {
  for (char& c : s) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
}

inline std::string LowerCopy(std::string_view sv) {
  std::string out{sv};
  ToLower(out);
  return out;
}

// pmr 版本: 返回 HttpString, allocator 取自传入 resource. 与 HttpRequest
// 的 headers_ map 的 key 类型一致, 避免 unordered_map::find 因为 hash
// 类型不透明而拒绝异类查找.
inline HttpString LowerCopy(std::string_view sv,
                             std::pmr::memory_resource* res) {
  HttpString out{sv, res};
  for (char& c : out) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }
  return out;
}

inline std::string Trim(std::string_view sv) {
  std::size_t begin{0};
  std::size_t end{sv.size()};
  while (begin < end && sv[begin] == ' ') ++begin;
  while (begin < end && sv[end - 1] == ' ') --end;
  return std::string(sv.substr(begin, end - begin));
}

inline HttpString Trim(std::string_view sv,
                        std::pmr::memory_resource* res) {
  std::size_t begin{0};
  std::size_t end{sv.size()};
  while (begin < end && sv[begin] == ' ') ++begin;
  while (begin < end && sv[end - 1] == ' ') --end;
  return HttpString{sv.substr(begin, end - begin), res};
}

// Response headers that the framework manages and handlers must not set.
// Covers framing (Content-Length, Transfer-Encoding), hop-by-hop headers
// (RFC 9110 §7.6.1), and pseudo-headers (HTTP/2 RFC 7540 §8.1.2.2).
// Caller is responsible for passing a lowercased key.
inline bool IsRestrictedResponseHeader(std::string_view lower_key) noexcept {
  if (!lower_key.empty() && lower_key.front() == ':') return true;  // HTTP/2 pseudo
  return lower_key == "content-length"      ||
         lower_key == "transfer-encoding"   ||
         lower_key == "connection"          ||
         lower_key == "keep-alive"          ||
         lower_key == "upgrade"             ||
         lower_key == "te"                  ||
         lower_key == "trailer"             ||
         lower_key == "proxy-connection"    ||
         lower_key == "proxy-authenticate"  ||
         lower_key == "proxy-authorization" ||
         lower_key == "date"                ||
         lower_key == "server";
}

// IMF-fixdate (RFC 9110 §5.6.7): "Sun, 06 Nov 1994 08:49:37 GMT".
// std::gmtime is not thread-safe; use gmtime_r on POSIX.
// A real production server would cache this per-second; left as a TODO.
inline std::string FormatHttpDate(std::time_t t) {
  std::tm tm{};
  ::gmtime_r(&t, &tm);
  std::array<char, 32> buf{};
  const std::size_t n =
      std::strftime(buf.data(), buf.size(), "%a, %d %b %Y %H:%M:%S GMT", &tm);
  return std::string(buf.data(), n);
}

inline std::string FormatHttpDateNow() {
  return FormatHttpDate(std::time(nullptr));
}

}  // namespace runtime::http::detail
