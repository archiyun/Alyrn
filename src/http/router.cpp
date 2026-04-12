#include "runtime/http/router.h"

namespace runtime::http {
    
void Router::Add(Method method, std::string_view path, Handler handler) {
    routes_[RouteKey{method, std::string(path)}] = std::move(handler);
}

void Router::Get(std::string_view path, Handler handler) {
    Add(Method::Get, path, std::move(handler));
}

void Router::Post(std::string_view path, Handler handler) {
    Add(Method::Post, path, std::move(handler));
}

void Router::Put(std::string_view path, Handler handler) {
    Add(Method::Put, path, std::move(handler));
}

void Router::Delete(std::string_view path, Handler handler) {
    Add(Method::Delete, path, std::move(handler));
}

std::optional<Handler> Router::Match(Method method, std::string_view path, bool &method_match) const {
    method_match = false;

    // 精确命中
    const auto it = routes_.find(RouteKey{method, std::string(path)});
    if (it != routes_.end()) {
        return it->second;
    }

    // path存在 method 不对 -> 405
    for (const auto &[key, _] : routes_) {
        if (key.path == path) {
            method_match = true;
            return std::nullopt;
        }
    }

    // path 完全不存在 -> 404
    return std::nullopt;
}
}