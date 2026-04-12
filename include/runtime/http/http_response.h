#pragma once

#include "runtime/http/http_types.h"

#include <string>
#include <unordered_map>

namespace runtime::http {

class HttpResponse {
public:
    explicit HttpResponse(bool close_connection);

    void SetStatusCode(StatusCode code);
    void SetBody(std::string body);
    void SetContentType(std::string_view content_type);
    void AddHeader(std::string_view key, std::string_view value);
    void SetCloseConnection(bool close);

    bool CloseConnection() const;

    // 序列化成 HTTP1.1 报文
    std::string ToString() const;
private:
    StatusCode status_code_ { StatusCode::Ok };
    bool close_connection_ { true };
    std::unordered_map<std::string, std::string> headers_;
    std::string body_;
};
}