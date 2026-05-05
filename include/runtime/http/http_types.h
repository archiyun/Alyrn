#pragma once

#include <string_view>

namespace runtime::http {

// Shared enums used across the HTTP layer.
enum class Method {
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
enum class Version {
  Unknown,
  Http10,
  Http11,
};

enum class StatusCode : uint16_t {
  Ok = 200,
  Created = 201,
  NoContent = 204,
  BadRequest = 400,
  Forbidden = 403,
  NotFound = 404,
  MethodNotAllowed = 405,
  RequestTimeout = 408,
  InternalServerError = 500,
};

// Returns the wire-format name for an HTTP method.
std::string_view MethodToString(Method m);

// Returns the standard reason phrase for a status code.
std::string_view StatusMessage(StatusCode code);

} // namespace runtime::http
