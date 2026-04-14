#pragma once

#include <string_view>

namespace runtime::http {

/**
 * 共享信息
 * HTTP 请求方法， 版本， 常见状态码
 */
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

// 目前支持 http1.0, 1.1
enum class Version {
    Unknown,
    Http10,
    Http11,
};

enum class StatusCode : int {
    Ok                  = 200,
    Created             = 201,
    NoContent           = 204,
    BadRequest          = 400,
    Forbidden           = 403,
    NotFound            = 404,
    MethodNotAllowed    = 405,
    RequestTimeout      = 408,
    InternalServerError = 500,  
};

// http_types.cpp
std::string_view MethodToString(Method m);
std::string_view StatusMessage(StatusCode code);
} // namespace runtime::http