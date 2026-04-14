#include "runtime/http/http_context.h"

#include <algorithm> 
#include <charconv>

namespace runtime::http {
// 请求行: 字符串映射到内部枚举
// 只接受状态机中支持实现的请求方法， 其余视为非法。
bool HttpContext::ParseMethod(std::string_view method_sv) {
  if (method_sv == "GET")
    request_.SetMethod(Method::Get);
  else if (method_sv == "POST")
    request_.SetMethod(Method::Post);
  else if (method_sv == "PUT")
    request_.SetMethod(Method::Put);
  else if (method_sv == "DELETE")
    request_.SetMethod(Method::Delete);
  else if (method_sv == "HEAD")
    request_.SetMethod(Method::Head);
  else if (method_sv == "OPTIONS")
    request_.SetMethod(Method::Options);
  else if (method_sv == "PATCH")
    request_.SetMethod(Method::Patch);
  else
    return false; // 未知方法
  return true;
}

// 解析 HTTP 版本字符串
// 目前仅支持 HTTP/1.0 HTTP/1.1
bool HttpContext::ParseVersion(std::string_view version_sv) {
  if (version_sv == "HTTP/1.1")
    request_.SetVersion(Version::Http11);
  else if (version_sv == "HTTP/1.0")
    request_.SetVersion(Version::Http10);
  else
    return false;
  return true;
}

// 增量解析 Buffer 的 HTTP请求。注意 TCP粘包拆包 和 返回值的处理
// 
// 返回值语义:
// - true: 当前输入合法解析可能完成， 或者数据不够。
// - false: 请求的字节流格式是非法， 由上层返回400
// 
// 注意:
// - 这个函数会边解析边消费 buf 已确认的字节数。
// - 一个请求完整解析后， 状态进入 GotAll, 由上层决定是否 Reset 
bool HttpContext::ParseRequest(runtime::net::Buffer &buf,
                               runtime::time::Timestamp ts) {
  while (state_ != ParseState::GotAll) {

    if (state_ == ParseState::ExpectRequestLine ||
        state_ == ParseState::ExpectHeaders) {
      // 请求行和header 按照 CRLF 分行解析
      // 半包问题： 如果当前行不完整，那么先不处理 保留现行数据交给下一次一起处理
      const char *begin = buf.Peek();
      const char *end = begin + buf.ReadableBytes();
      const char *crlf = std::search(begin, end, "\r\n", "\r\n" + 2);

      if (crlf == end)
        return true; 

      std::string_view line(begin, crlf - begin); // 一次完整的行
      buf.RetrieveUntil(crlf + 2);                // 消费这行 + "\r\n"

      if (state_ == ParseState::ExpectRequestLine) {
        if (!ProcessRequestLine(line))
          return false;
        state_ = ParseState::ExpectHeaders;
      } else {
        // 空行 = headers 结束
        // - 没有 body: 当前请求已完整
        // - 有 body: 进入 body 读取阶段
        if (line.empty()) {
          if (body_bytes_expected_ == 0) {
            request_.SetReceiveTime(ts);
            state_ = ParseState::GotAll;
          } else {
            state_ = ParseState::ExpectBody;
          }
        } else {
          if (!ProcessHeaderLine(line))
            return false;
        }
      }
    } else if (state_ == ParseState::ExpectBody) {
      // body 必须等到足够的字节数全部到齐后再一次取出
      if (buf.ReadableBytes() < body_bytes_expected_)
        return true;

      request_.SetBody(buf.RetrieveAsString(body_bytes_expected_));
      request_.SetReceiveTime(ts);
      state_ = ParseState::GotAll;
    }
  }
  return true;
}


/**
 * 解析请求行:
 *   METHOD SP URI SP VERSION
 *
 * 例如:
 *   GET /path?query HTTP/1.1
 *
 * 这里只拆出 method / uri / version 三段，
 * path 和 query 的进一步拆分在函数内部完成。
 */
bool HttpContext::ProcessRequestLine(std::string_view line) {
  const auto m_end = line.find(' ');
  if (m_end == std::string_view::npos)
    return false;

  const auto uri_end = line.find(' ', m_end + 1);
  if (uri_end == std::string_view::npos)
    return false;

  const std::string_view method_sv = line.substr(0, m_end);
  const std::string_view uri_sv = line.substr(m_end + 1, uri_end - m_end - 1);
  const std::string_view version_sv = line.substr(uri_end + 1);

  if (!ParseMethod(method_sv)) {
    return false;
  }
  if (!ParseVersion(version_sv)) {
    return false;
  }
  // URI 中 '?' 之前为path, 之后视为原始 query 字符串.
  const auto q_pos = uri_sv.find('?');
  if (q_pos == std::string_view::npos) {
    request_.SetPath(std::string(uri_sv));
    request_.SetQuery("");
  } else {
    request_.SetPath(std::string(uri_sv.substr(0, q_pos)));
    request_.SetQuery(std::string(uri_sv.substr(q_pos + 1)));
  }

  return true;
}

// 解析单行 header: "Field: Value"
//
// - header 名会在 AddHeader() 归一化
// - 当前实现只依赖 Content-Length 来决定是否读取 body
bool HttpContext::ProcessHeaderLine(std::string_view line) {
  const auto colon = line.find(":");
  if (colon == std::string_view::npos)
    return false;

  std::string_view field = line.substr(0, colon);
  std::string_view value = line.substr(colon + 1);

  // 兼容常见的 "Field: Value" 写法，去掉 value 前导空格
  while (!value.empty() && value.front() == ' ')
    value.remove_prefix(1);

  if (field.empty())
    return false;

  request_.AddHeader(field, value);

  // 
  const auto cl = request_.GetHeader("content-length");
  if (!cl.empty()) {
    std::size_t len = 0;
    const auto [ptr, ec] =
        std::from_chars(cl.data(), cl.data() + cl.size(), len);
    if (ec != std::errc{}) return false;
    // 当前实现对请求体限制在 8 MB, 避免异常大包长期占用内存
    if (len > 8 * 1024 * 1024) return false; 
    body_bytes_expected_ = len;
  }
  return true;
}

// 为 keep-alive 场景重置解析器状态
void HttpContext::Reset() {
  state_ = ParseState::ExpectRequestLine;
  body_bytes_expected_ = 0;
  request_.Reset();
}

} // namespace runtime::http