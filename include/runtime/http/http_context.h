#pragma once

#include "runtime/http/http_request.h"
#include "runtime/net/buffer.h"
#include "runtime/time/timestamp.h"

namespace runtime::http {
/**
 * 目的:
 * 将Buffer字节流， 按HTTP/1.1协议状态机， 填进 httpRequest
 * 只是一个解析器。
 */
class HttpContext {
public:
    enum class ParseState {
        ExpectRequestLine,
        ExpectHeaders,
        ExpectBody,
        GotAll,
    };

    bool ParseRequest(runtime::net::Buffer &buf, runtime::time::Timestamp ts);

    bool GotAll() const { return state_ == ParseState::GotAll; }
    void Reset();

    const HttpRequest &Request() const { return request_; }
    HttpRequest &MutableRequest() { return request_; }

private:
    bool ProcessRequestLine(std::string_view line);
    bool ProcessHeaderLine(std::string_view line);

    bool ParseMethod(std::string_view method_sv);
    bool ParseVersion(std::string_view version_sv);

    ParseState state_ { ParseState::ExpectRequestLine };
    HttpRequest request_;
    std::size_t body_bytes_expected_ {0};
};
}