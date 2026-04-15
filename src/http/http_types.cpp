#include "runtime/http/http_types.h"

namespace runtime::http {

std::string_view MethodToString(Method m) {
  switch (m) {
    case Method::Get:
      return "GET";
    case Method::Post:
      return "POST";
    case Method::Put:
      return "PUT";
    case Method::Delete:
      return "DELETE";
    case Method::Head:
      return "HEAD";
    case Method::Options:
      return "OPTIONS";
    case Method::Patch:
      return "PATCH";
    default:
      return "INVALID";
  }
}

std::string_view StatusMessage(StatusCode code) {
  switch (code) {
    case StatusCode::Ok:
      return "OK";
    case StatusCode::Created:
      return "Created";
    case StatusCode::NoContent:
      return "No Content";
    case StatusCode::BadRequest:
      return "Bad Request";
    case StatusCode::Forbidden:
      return "Forbidden";
    case StatusCode::NotFound:
      return "Not Found";
    case StatusCode::MethodNotAllowed:
      return "Method Not Allowed";
    case StatusCode::RequestTimeout:
      return "Request Timeout";
    case StatusCode::InternalServerError:
      return "Internal Server Error";
    default:
      return "Unknown";
  }
}

}  // namespace runtime::http
