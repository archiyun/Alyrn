#pragma once

#include "runtime/http/http_request.h"
#include "runtime/net/buffer.h"
#include "runtime/time/timestamp.h"

namespace runtime::http {
/**
 * HttpContext:
 * 负责将 TcpConnection 上的字节流增量解析成一个http请求
 * 只做协议解析， 翻译: bytes -> HttpRequest
 * 
 * 约束:
 * - 面向 HTTP/1.0 and HTTP/1.1
 * - 依赖调用方 按连接维度持有上下文
 * - 解析成功后 由上层决定是否 "Reset()" 进入下一轮请求 
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