// Copyright (c) 2026 Aresna
// SPDX-License-Identifier: MIT
#pragma once

#include <string_view>

namespace runtime::http {

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
};

// HTTP protocol versions supported by the parser.
enum class Version : uint8_t{
  Unknown,
  Http10 = 1,
  Http11,
  Http20, // implemented through Http2Session/nghttp2, not HttpContext parser
  Http30, // not implement
};

enum class StatusCode : uint16_t {
  SwitchingProtocols  = 101,

  Ok                  = 200,
  Created             = 201,
  NoContent           = 204,

  MovedPermanently    = 301,
  Found               = 302,
  SeeOther            = 303,
  NotModified         = 304,

  BadRequest          = 400,
  Unauthorized        = 401,
  Forbidden           = 403,
  NotFound            = 404,
  MethodNotAllowed    = 405,
  RequestTimeout      = 408,
  TooManyRequests     = 429,

  InternalServerError = 500,
  NotImplemented      = 501,
  BadGateway          = 502,
  ServiceUnavailable  = 503,
  GatewayTimeout      = 504,
};

// Returns the wire-format name for an HTTP method.
std::string_view MethodToString(Method m) noexcept;

// Returns the standard reason phrase for a status code.
std::string_view StatusMessage(StatusCode code) noexcept;

} // namespace runtime::http
