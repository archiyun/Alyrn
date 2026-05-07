#include "runtime/http/http_types.h"

#include <array>
#include <string_view>
namespace runtime::http {

std::string_view MethodToString(Method m) noexcept {
  static constexpr std::array table = {
      std::pair{Method::Get,     std::string_view{"GET"}},
      std::pair{Method::Post,    std::string_view{"POST"}},
      std::pair{Method::Put,     std::string_view{"PUT"}},
      std::pair{Method::Delete,  std::string_view{"DELETE"}},
      std::pair{Method::Head,    std::string_view{"HEAD"}},
      std::pair{Method::Options, std::string_view{"OPTIONS"}},
      std::pair{Method::Patch,   std::string_view{"PATCH"}},
  };

  for (const auto& [method, name] : table) {
    if (method == m) {
      return name;
    }
  }

  return "INVALID";
}

std::string_view StatusMessage(StatusCode code) noexcept {
  static constexpr std::array table = {
      std::pair{StatusCode::SwitchingProtocols,  std::string_view{"Switching Protocols"}},
      std::pair{StatusCode::Ok,                  std::string_view{"OK"}},
      std::pair{StatusCode::Created,             std::string_view{"Created"}},
      std::pair{StatusCode::NoContent,           std::string_view{"No Content"}},
      std::pair{StatusCode::MovedPermanently,    std::string_view{"Moved Permanently"}},
      std::pair{StatusCode::Found,               std::string_view{"Found"}},
      std::pair{StatusCode::SeeOther,            std::string_view{"See Other"}},
      std::pair{StatusCode::NotModified,         std::string_view{"Not Modified"}},
      std::pair{StatusCode::BadRequest,          std::string_view{"Bad Request"}},
      std::pair{StatusCode::Unauthorized,        std::string_view{"Unauthorized"}},
      std::pair{StatusCode::Forbidden,           std::string_view{"Forbidden"}},
      std::pair{StatusCode::NotFound,            std::string_view{"Not Found"}},
      std::pair{StatusCode::MethodNotAllowed,    std::string_view{"Method Not Allowed"}},
      std::pair{StatusCode::RequestTimeout,      std::string_view{"Request Timeout"}},
      std::pair{StatusCode::InternalServerError, std::string_view{"Internal Server Error"}},
      std::pair{StatusCode::NotImplemented,      std::string_view{"Not Implemented"}},
      std::pair{StatusCode::BadGateway,          std::string_view{"Bad Gateway"}},
      std::pair{StatusCode::ServiceUnavailable,  std::string_view{"Service Unavailable"}},
      std::pair{StatusCode::GatewayTimeout,      std::string_view{"Gateway Timeout"}},
  };

  for (const auto& [status_code, message] : table) {
    if (status_code == code) {
      return message;
    }
  }

  return "Unknown";
}

}  // namespace runtime::http