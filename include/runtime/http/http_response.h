#pragma once

#include "runtime/http/http_types.h"

#include <string>
#include <unordered_map>

namespace runtime::http {

/**
 * HttpResponse:
 * 描述一条 HTTP 响应，序列化成 HTTP/1.1 文本报文。
 *
 * 负责:
 * - 保存状态码、响应头和响应体
 * - 根据 body 自动生成 Content-Length
 * - 根据 close_connection_ 生成 Connection 头
 *
 * 不负责:
 * - socket 发送
 * - chunked 编码
 * - 压缩、缓存等高级 HTTP 特性
 */
class HttpResponse {
public:
    // close_connection = true 表示响应发送后由上层关闭连接
    explicit HttpResponse(bool close_connection);

    void SetStatusCode(StatusCode code);
    void SetBody(std::string body);
    void SetContentType(std::string_view content_type);
    void AddHeader(std::string_view key, std::string_view value);
    void SetCloseConnection(bool close);

    bool CloseConnection() const;

    // 按 HTTP/1.1 格式生成完整报文。
    // 总是输出 Content-Length，暂不支持 chunked transfer encoding。
    std::string ToString() const;
private:
    StatusCode status_code_ { StatusCode::Ok };
    bool close_connection_  { true };
    std::unordered_map<std::string, std::string> headers_;
    std::string body_;
};
} // namespace runtime::http