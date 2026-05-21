// Copyright (c) 2026 Aresna
// SPDX-License-Identifier: MIT
#include "runtime/http/http_types.h"

namespace runtime::http {

std::string_view MethodToString(Method m) noexcept {
  switch (m) {
    case Method::Get:     return "GET";
    case Method::Post:    return "POST";
    case Method::Put:     return "PUT";
    case Method::Delete:  return "DELETE";
    case Method::Head:    return "HEAD";
    case Method::Options: return "OPTIONS";
    case Method::Patch:   return "PATCH";
    case Method::Connect: return "CONNECT";
    case Method::Trace:   return "TRACE";
    case Method::Invalid: return "INVALID";
  }
  return "INVALID";
}

std::string_view StatusMessage(StatusCode code) noexcept {
  switch (code) {
    case StatusCode::Continue:                    return "Continue";
    case StatusCode::SwitchingProtocols:          return "Switching Protocols";

    case StatusCode::Ok:                          return "OK";
    case StatusCode::Created:                     return "Created";
    case StatusCode::NoContent:                   return "No Content";
    case StatusCode::PartialContent:              return "Partial Content";

    case StatusCode::MovedPermanently:            return "Moved Permanently";
    case StatusCode::Found:                       return "Found";
    case StatusCode::SeeOther:                    return "See Other";
    case StatusCode::NotModified:                 return "Not Modified";

    case StatusCode::BadRequest:                  return "Bad Request";
    case StatusCode::Unauthorized:                return "Unauthorized";
    case StatusCode::Forbidden:                   return "Forbidden";
    case StatusCode::NotFound:                    return "Not Found";
    case StatusCode::MethodNotAllowed:            return "Method Not Allowed";
    case StatusCode::RequestTimeout:              return "Request Timeout";
    case StatusCode::PayloadTooLarge:             return "Payload Too Large";
    case StatusCode::UriTooLong:                  return "URI Too Long";
    case StatusCode::UnsupportedMediaType:        return "Unsupported Media Type";
    case StatusCode::TooManyRequests:             return "Too Many Requests";
    case StatusCode::RequestHeaderFieldsTooLarge: return "Request Header Fields Too Large";

    case StatusCode::InternalServerError:         return "Internal Server Error";
    case StatusCode::NotImplemented:              return "Not Implemented";
    case StatusCode::BadGateway:                  return "Bad Gateway";
    case StatusCode::ServiceUnavailable:          return "Service Unavailable";
    case StatusCode::GatewayTimeout:              return "Gateway Timeout";
    case StatusCode::HttpVersionNotSupported:     return "HTTP Version Not Supported";
  }
  return "Unknown";
}

}  // namespace runtime::http
