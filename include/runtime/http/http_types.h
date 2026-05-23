// Copyright (c) 2026 Aresna
// SPDX-License-Identifier: MIT
#pragma once

#include <array>
#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace runtime::http {

using HttpString = std::pmr::string;
template <typename K, typename V>
using HttpMap = std::pmr::unordered_map<K, V>;
template <typename T>
using HttpVector = std::pmr::vector<T>;

// Shared enums used across the HTTP layer.
enum class Method : uint8_t{
  Invalid,
  Get,
  Post,
  Put,
  Delete,
  Head,
  Options,
  Patch,
  Connect,
  Trace,
};

// HTTP protocol versions supported by the parser.
enum class Version : uint8_t{
  Unknown,
  Http10 = 1,
  Http11,
  Http20, // implemented through Http2Session/nghttp2, not HttpContext parser
  Http30, // not implemented
};

enum class StatusCode : uint16_t {
  Continue                    = 100,
  SwitchingProtocols          = 101,

  Ok                          = 200,
  Created                     = 201,
  NoContent                   = 204,
  PartialContent              = 206,

  MovedPermanently            = 301,
  Found                       = 302,
  SeeOther                    = 303,
  NotModified                 = 304,

  BadRequest                  = 400,
  Unauthorized                = 401,
  Forbidden                   = 403,
  NotFound                    = 404,
  MethodNotAllowed            = 405,
  RequestTimeout              = 408,
  PayloadTooLarge             = 413,
  UriTooLong                  = 414,
  UnsupportedMediaType        = 415,

  TooManyRequests             = 429,
  RequestHeaderFieldsTooLarge = 431,

  InternalServerError         = 500,
  NotImplemented              = 501,
  BadGateway                  = 502,
  ServiceUnavailable          = 503,
  GatewayTimeout              = 504,
  HttpVersionNotSupported     = 505,
};

// Returns the wire-format name for an HTTP method.
std::string_view MethodToString(Method m) noexcept;

// Returns the standard reason phrase for a status code.
std::string_view StatusMessage(StatusCode code) noexcept;

// All request methods accepted by the HTTP/1.x parser.
// Order is arbitrary; the parser iterates and matches against MethodToString.
// MethodToString is the single source of truth for the wire-format spelling.
inline constexpr std::array<Method, 9> kAllRequestMethods = {
    Method::Get,    Method::Post,   Method::Put,    Method::Delete,
    Method::Head,   Method::Options, Method::Patch,
    Method::Connect, Method::Trace,
};

// A single captured path parameter, e.g. for "/users/:id" with "/users/42"
// the entry is {"id", "42"}. Lives in http_types.h (not router.h) so both
// Router and HttpRequest can reference it without a circular include.
struct PathParam {
  std::string key;
  std::string value;
};

} // namespace runtime::http
