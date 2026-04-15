#pragma once

#include "runtime/http/http_types.h"

#include <string>
#include <unordered_map>

namespace runtime::http {

// HttpResponse stores one HTTP response and serializes it into an HTTP/1.1
// message.
class HttpResponse {
public:
  // When close_connection is true, the caller is expected to close the
  // connection after sending the response.
  explicit HttpResponse(bool close_connection);

  void SetStatusCode(StatusCode code);
  void SetBody(std::string body);
  void SetContentType(std::string_view content_type);
  void AddHeader(std::string_view key, std::string_view value);
  void SetCloseConnection(bool close);

  bool CloseConnection() const;

  // Serializes the response in HTTP/1.1 format.
  // This implementation always emits Content-Length and does not support
  // chunked transfer encoding.
  std::string ToString() const;
private:
  StatusCode status_code_{StatusCode::Ok};
  bool close_connection_{true};
  std::unordered_map<std::string, std::string> headers_;
  std::string body_;
};

}  // namespace runtime::http
