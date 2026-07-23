// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>

#include "coropact/http/http_types.h"

namespace coropact::http {

// Result of one incremental HTTP parse step. Distinct error variants let
// servers emit accurate status codes instead of a blanket 400.
enum class ParseStatus : uint8_t {
  Continue,         // Need more bytes; call again on next read.
  GotAll,           // A complete request is available.
  BadRequest,       // 400 - malformed framing (bad line, dup CL, TE present...)
  UriTooLong,       // 414 - request line / URI over the cap.
  HeaderTooLarge,   // 431 - header line/count/total bytes over the cap.
  PayloadTooLarge,  // 413 - Content-Length declared value over the cap.
  BadMethod,        // 501 - method token not recognized.
  BadVersion,       // 505 - HTTP version token not supported.
};

// Maps a ParseStatus error to the HTTP status code a server should emit.
// Callers should only invoke this for error variants (not Continue/GotAll).
StatusCode ParseStatusToStatusCode(ParseStatus s) noexcept;

}  // namespace coropact::http
