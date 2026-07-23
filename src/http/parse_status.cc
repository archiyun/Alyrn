// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "coropact/http/parse_status.h"

namespace coropact::http {

StatusCode ParseStatusToStatusCode(ParseStatus s) noexcept {
  switch (s) {
    case ParseStatus::UriTooLong:
      return StatusCode::UriTooLong;
    case ParseStatus::HeaderTooLarge:
      return StatusCode::RequestHeaderFieldsTooLarge;
    case ParseStatus::PayloadTooLarge:
      return StatusCode::PayloadTooLarge;
    case ParseStatus::BadMethod:
      return StatusCode::NotImplemented;
    case ParseStatus::BadVersion:
      return StatusCode::HttpVersionNotSupported;
    case ParseStatus::BadRequest:
    case ParseStatus::Continue:
    case ParseStatus::GotAll:
      break;
  }
  return StatusCode::BadRequest;
}

}  // namespace coropact::http
