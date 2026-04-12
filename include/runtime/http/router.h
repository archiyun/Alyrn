#pragma once

#include "runtime/http/http_request.h"
#include "runtime/http/http_response.h"
#include "runtime/http/http_types.h"

#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

namespace runtime::http {

using Handler = std::function<void(const HttpRequest&, HttpResponse&)>;

struct RouteMatch {
    Handler handler;
    std::unordered_map<std::string, std::string> params; // {"id": 42}
};

class Router {
public:
    void Add(Method method, std::string_view path, Handler handler);
    void Get(std::string_view path, Handler handler);
    void Post(std::string_view path, Handler handler);
    void Put(std::string_view path, Handler handler);
    void Delete(std::string_view path, Handler handler);

    // nullopt = 404, method_matched = true = 405
    std::optional<Handler> Match(Method method, std::string_view path, bool &method_matched) const;
private:
    struct RouteKey {
        Method method;
        std::string path;
        bool operator==(const RouteKey &obj) const noexcept {
            return method == obj.method && path == obj.path;
        }
    };

    struct RouteKeyHash {
        std::size_t operator()(const RouteKey &obj) const noexcept {
            const std::size_t h1 = std::hash<int>{}(static_cast<int>(obj.method));
            const std::size_t h2 = std::hash<std::string>{}(obj.path);
            return h1 ^ (h2 << 1);
        }
    };
private:
    std::unordered_map<RouteKey, Handler, RouteKeyHash> routes_;
};
}  // namespace runtime::http
