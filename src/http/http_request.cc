// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "coropact/http/http_request.h"

#include <utility>

#include "header_utils.h"
#include "coropact/http/http_types.h"

namespace coropact::http {

HttpRequest::HttpRequest()
    : pool_{coropact::memory::Pool::Create()},
      res_{std::make_unique<coropact::memory::PoolResource>(*pool_)},
      path_{res_.get()},
      query_{res_.get()},
      body_{res_.get()},
      headers_{res_.get()} {}

void HttpRequest::AddHeader(std::string_view field, std::string_view value) {
  AddHeaderLowered(MakeHeaderKey(field), value);
}

HttpString HttpRequest::MakeHeaderKey(std::string_view field) const {
  return detail::LowerCopy(field, res_.get());
}

void HttpRequest::AddHeaderLowered(HttpString field, std::string_view value) {
  auto [it, inserted] =
      headers_.emplace(std::move(field), detail::Trim(value, res_.get()));
  if (inserted) {
    CacheHeader(it->first, it->second);
  }
}

std::string_view HttpRequest::header(std::string_view field) const {
  const HeaderNameEqual equal;
  if (equal(field, "host")) return host_;
  if (equal(field, "connection")) return connection_;
  if (equal(field, "content-length")) return content_length_;

  const auto it = headers_.find(field);
  if (it == headers_.end()) {
    return {};
  }
  return it->second;
}

void HttpRequest::set_header(std::string_view field, std::string_view value) {
  auto val = detail::Trim(value, res_.get());
  auto it = headers_.find(field);
  if (it == headers_.end()) {
    auto key = detail::LowerCopy(field, res_.get());
    it = headers_.emplace(std::move(key), std::move(val)).first;
  } else {
    it->second = std::move(val);
  }
  CacheHeader(it->first, it->second);
}

bool HttpRequest::RemoveHeader(std::string_view field) {
  const auto it = headers_.find(field);
  if (it == headers_.end()) return false;
  ClearCachedHeader(it->first);
  headers_.erase(it);
  return true;
}

bool HttpRequest::keep_alive() const {
  const auto conn = connection_;
  if (version_ == Version::Http11) {
    return conn != "close";
  }
  if (version_ == Version::Http10) {
    return conn == "keep-alive";
  }
  return false;
}

void HttpRequest::CacheHeader(std::string_view field, std::string_view value) {
  if (field == "host") {
    host_ = value;
  } else if (field == "connection") {
    connection_ = value;
  } else if (field == "content-length") {
    content_length_ = value;
  }
}

void HttpRequest::ClearCachedHeader(std::string_view field) {
  if (field == "host") {
    host_ = {};
  } else if (field == "connection") {
    connection_ = {};
  } else if (field == "content-length") {
    content_length_ = {};
  }
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
  headers_.~HeaderMap();
  body_.~HttpString();
  query_.~HttpString();
  path_.~HttpString();

  pool_->Reset();

  new (&path_)    HttpString{res_.get()};
  new (&query_)   HttpString{res_.get()};
  new (&body_)    HttpString{res_.get()};
  new (&headers_) HeaderMap{res_.get()};

  host_ = {};
  connection_ = {};
  content_length_ = {};
  receive_time_ = {};
}

}  // namespace coropact::http
