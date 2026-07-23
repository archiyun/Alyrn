// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "vexo/gateway/proxy_pass.h"

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstring>

namespace vexo::gateway {
namespace {

bool AsciiCaseEqual(std::string_view lhs, std::string_view rhs) {
  if (lhs.size() != rhs.size()) return false;
  for (std::size_t i = 0; i < lhs.size(); ++i) {
    unsigned char a = static_cast<unsigned char>(lhs[i]);
    unsigned char b = static_cast<unsigned char>(rhs[i]);
    if (a >= 'A' && a <= 'Z') a = static_cast<unsigned char>(a + ('a' - 'A'));
    if (b >= 'A' && b <= 'Z') b = static_cast<unsigned char>(b + ('a' - 'A'));
    if (a != b) return false;
  }
  return true;
}

bool NamedByConnection(std::string_view header, std::string_view connection) {
  while (!connection.empty()) {
    const auto comma = connection.find(',');
    auto token = connection.substr(0, comma);
    while (!token.empty() && (token.front() == ' ' || token.front() == '\t')) {
      token.remove_prefix(1);
    }
    while (!token.empty() && (token.back() == ' ' || token.back() == '\t')) {
      token.remove_suffix(1);
    }
    if (AsciiCaseEqual(header, token)) return true;
    if (comma == std::string_view::npos) break;
    connection.remove_prefix(comma + 1);
  }
  return false;
}

bool IsHopByHop(std::string_view header) {
  static constexpr std::array<std::string_view, 9> kHeaders = {
      "connection",
      "keep-alive",
      "proxy-connection",
      "proxy-authenticate",
      "proxy-authorization",
      "te",
      "trailer",
      "transfer-encoding",
      "upgrade",
  };
  return std::find(kHeaders.begin(), kHeaders.end(), header) != kHeaders.end();
}

void AppendHeader(std::string& out, std::string_view name, std::string_view value) {
  out += name;
  out += ": ";
  out += value;
  out += "\r\n";
}

}  // namespace

uint64_t ProxyPass::NowMs() {
  return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                   std::chrono::steady_clock::now().time_since_epoch())
                                   .count());
}

bool ProxyPass::IsIdempotent(vexo::http::Method method) {
  using vexo::http::Method;
  switch (method) {
    case Method::Get:
    case Method::Head:
    case Method::Put:
    case Method::Delete:
    case Method::Options:
    case Method::Trace:
      return true;
    default:
      return false;
  }
}

std::shared_ptr<UpstreamPeer> ProxyPass::SelectFailoverPeer(Upstream& upstream,
                                                            const UpstreamPeer& current) {
  const uint64_t now_ms = NowMs();
  for (const auto& peer : upstream.peers()) {
    if (peer.get() == &current) continue;
    if (peer->AvailableAt(now_ms)) return peer;
  }
  return nullptr;
}

ProxyPass::ResponseState ProxyPass::RewriteHeaders(std::string_view raw_headers, std::string& out,
                                                   vexo::http::Method request_method) {
  ResponseState state;
  out.reserve(raw_headers.size() + 64);

  std::size_t pos = 0;
  const std::size_t first_cr = raw_headers.find('\r');
  if (first_cr != std::string_view::npos) {
    std::string_view first_line = raw_headers.substr(0, first_cr);
    out += first_line;
    out += "\r\n";
    if (first_line.size() >= 12 && first_line[8] == ' ') {
      auto code_sv = first_line.substr(9, 3);
      std::from_chars(code_sv.data(), code_sv.data() + code_sv.size(), state.status);
    }
    pos = first_cr + 2;
  }

  const bool no_body = request_method == vexo::http::Method::Head || state.status == 204 ||
                       state.status == 304 || (state.status >= 100 && state.status < 200);
  if (no_body) {
    state.framing = BodyFraming::kNoBody;
    state.body_remaining = 0;
    state.upstream_keepalive = true;
  }

  while (pos < raw_headers.size()) {
    const std::size_t eol = raw_headers.find('\n', pos);
    if (eol == std::string_view::npos) break;
    std::string_view line = raw_headers.substr(pos, eol - pos);
    if (!line.empty() && line.back() == '\r') line.remove_suffix(1);
    pos = eol + 1;

    const auto colon = line.find(':');
    if (colon != std::string_view::npos && AsciiCaseEqual(line.substr(0, colon), "server")) {
      out += "Server: runtime-gateway\r\n";
    } else {
      out += line;
      out += "\r\n";
    }

    if (line.empty()) break;
    if (no_body) continue;

    if (colon == std::string_view::npos) continue;
    std::string_view name = line.substr(0, colon);
    std::string_view value = line.substr(colon + 1);
    while (!value.empty() && (value.front() == ' ' || value.front() == '\t')) {
      value.remove_prefix(1);
    }

    if (AsciiCaseEqual(name, "content-length")) {
      uint64_t n = 0;
      auto [ptr, ec] = std::from_chars(value.data(), value.data() + value.size(), n);
      if (ec == std::errc{}) {
        state.framing = BodyFraming::kContentLength;
        state.body_remaining = n;
      }
    } else if (AsciiCaseEqual(name, "transfer-encoding")) {
      for (size_t i = 0; i + 7 <= value.size(); ++i) {
        if (AsciiCaseEqual(value.substr(i, 7), "chunked")) {
          state.framing = BodyFraming::kChunked;
          break;
        }
      }
    } else if (AsciiCaseEqual(name, "connection")) {
      if (AsciiCaseEqual(value, "close")) state.upstream_keepalive = false;
    }
  }
  return state;
}

std::string ProxyPass::BuildRequest(const vexo::http::HttpRequest& req, const UpstreamPeer& peer,
                                    ForwardedHeaderContext forwarded) {
  std::string out;
  BuildRequestInto(req, peer, out, forwarded);
  return out;
}

void ProxyPass::BuildRequestInto(const vexo::http::HttpRequest& req, const UpstreamPeer& peer,
                                 std::string& out, ForwardedHeaderContext forwarded) {
  out.clear();
  out.reserve(256);
  out += vexo::http::MethodToString(req.method());
  out += ' ';
  out += req.path().empty() ? "/" : req.path();
  if (!req.query().empty()) {
    out += '?';
    out += req.query();
  }
  out += " HTTP/1.1\r\n";

  const std::string_view connection = req.connection();
  const std::string_view original_host = req.host();
  std::string_view previous_xff;
  std::string_view previous_via;
  std::string_view existing_request_id;

  for (const auto& [k, v] : req.headers()) {
    if (k == "host" || IsHopByHop(k) || NamedByConnection(k, connection)) {
      continue;
    }
    if (k == "x-forwarded-for") {
      previous_xff = v;
      continue;
    }
    if (k == "via") {
      previous_via = v;
      continue;
    }
    if (k == "x-request-id") {
      existing_request_id = v;
      continue;
    }
    if (k == "x-real-ip" || k == "x-forwarded-proto" || k == "x-forwarded-host" ||
        k == "x-forwarded-port") {
      continue;
    }
    AppendHeader(out, k, v);
  }

  if (!previous_xff.empty() && !forwarded.client_ip.empty()) {
    out += "x-forwarded-for: ";
    out += previous_xff;
    out += ", ";
    out += forwarded.client_ip;
    out += "\r\n";
  } else if (!previous_xff.empty()) {
    AppendHeader(out, "x-forwarded-for", previous_xff);
  } else if (!forwarded.client_ip.empty()) {
    AppendHeader(out, "x-forwarded-for", forwarded.client_ip);
  }
  if (!forwarded.client_ip.empty()) {
    AppendHeader(out, "x-real-ip", forwarded.client_ip);
  }
  if (!forwarded.scheme.empty()) {
    AppendHeader(out, "x-forwarded-proto", forwarded.scheme);
  }
  if (!original_host.empty()) {
    AppendHeader(out, "x-forwarded-host", original_host);
  }

  if (!previous_via.empty() && !forwarded.gateway_name.empty()) {
    out += "via: ";
    out += previous_via;
    out += ", 1.1 ";
    out += forwarded.gateway_name;
    out += "\r\n";
  } else if (!previous_via.empty()) {
    AppendHeader(out, "via", previous_via);
  } else if (!forwarded.gateway_name.empty()) {
    out += "via: 1.1 ";
    out += forwarded.gateway_name;
    out += "\r\n";
  }

  if (!existing_request_id.empty()) {
    AppendHeader(out, "x-request-id", existing_request_id);
  } else if (!forwarded.request_id.empty()) {
    AppendHeader(out, "x-request-id", forwarded.request_id);
  }

  AppendHeader(out, "host", peer.host_port());
  out += "connection: keep-alive\r\n\r\n";
  if (!req.body().empty()) out += req.body();
}

}  // namespace vexo::gateway
