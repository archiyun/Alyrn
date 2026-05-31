// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "runtime/http/http_request.h"

#include <utility>

#include "header_utils.h"
#include "runtime/http/http_types.h"

namespace runtime::http {

HttpRequest::HttpRequest()
    : pool_{runtime::memory::Pool::Create()},
      res_{std::make_unique<runtime::memory::PoolResource>(*pool_)},
      path_{res_.get()},
      query_{res_.get()},
      body_{res_.get()},
      headers_{res_.get()} {}

void HttpRequest::AddHeader(std::string_view field, std::string_view value) {
  headers_.emplace(detail::LowerCopy(field, res_.get()),
                   detail::Trim(value,     res_.get()));
}

std::string_view HttpRequest::header(std::string_view field) const {
  const auto key = detail::LowerCopy(field, res_.get());
  const auto it  = headers_.find(key);
  if (it == headers_.end()) {
    return {};
  }
  return it->second;
}

void HttpRequest::set_header(std::string_view field, std::string_view value) {
  auto key  = detail::LowerCopy(field, res_.get());
  auto val  = detail::Trim(value,      res_.get());
  auto [it, inserted] = headers_.try_emplace(std::move(key), std::move(val));
  if (!inserted) {
    it->second = std::move(val);
  }
}

bool HttpRequest::RemoveHeader(std::string_view field) {
  const auto key = detail::LowerCopy(field, res_.get());
  return headers_.erase(key) > 0;
}

bool HttpRequest::keep_alive() const {
  const auto conn = header("connection");
  if (static_cast<uint8_t>(version_) < static_cast<uint8_t>(Version::Http10)) {
    // HTTP/1.1 / 2 / 3 keeps connections alive by default unless the client sends
    // Connection: close.
    return conn != "close";
  }
  return conn == "keep-alive";
}

void HttpRequest::Reset() {
  // moved-from 状态 (pool_/res_ 为空, 例如 TakeRequest 之后): 必须原地
  // 析构 + placement-new, 不能走 move-assign. 原因: pmr 容器内部缓存
  // 了 allocator 的裸指针, 但 polymorphic_allocator 的
  // propagate_on_container_move_assignment = false, move-assign 不会
  // 更新这些指针, 旧的 PoolResource 一旦被 TakeRequest 的接收者释放,
  // 容器内的 allocator 就悬空, 下一次 AddHeader 会在 vtable 派发处崩.
  if (pool_ == nullptr) {
    this->~HttpRequest();
    new (this) HttpRequest();
    return;
  }

  method_  = Method::Invalid;
  version_ = Version::Unknown;

  // 销毁顺序: pmr 容器先析构 (do_deallocate 是 no-op 不真释放),
  // 然后 Pool::Reset 一次性回收 arena, 最后 placement-new 重建.
  // path_params_ 用普通 std::allocator, 单独 clear 即可.
  path_params_.clear();
  headers_.~HttpMap<HttpString, HttpString>();
  body_.~HttpString();
  query_.~HttpString();
  path_.~HttpString();

  pool_->Reset();

  new (&path_)    HttpString{res_.get()};
  new (&query_)   HttpString{res_.get()};
  new (&body_)    HttpString{res_.get()};
  new (&headers_) HttpMap<HttpString, HttpString>{res_.get()};

  receive_time_ = {};
}

}  // namespace runtime::http
