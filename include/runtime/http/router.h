#pragma once

#include "runtime/http/http_request.h"
#include "runtime/http/http_response.h"
#include "runtime/http/http_types.h"

#include <memory>
#include <functional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace runtime::http {

// 路由命中后执行的业务处理函数
// Handler 只负责处理一条已完成解析的请求
using Handler = std::function<void(const HttpRequest&, HttpResponse&)>;

// 预留给参数命中结果结构
// 当前 给 Router 只支持精确路径匹配， 未使用 params
struct RouteMatch {
    Handler handler{nullptr};
    std::unordered_map<std::string, std::string> params;
    bool path_matched{false}; // true -> 存在路径 但 method 不匹配
                              // false 
};

class Router {
public:
    // 注册一条 method + path 的精确匹配路由
    void Add(Method method, std::string_view path, Handler handler);
    
    void Get(std::string_view path, Handler handler);
    void Post(std::string_view path, Handler handler);
    void Put(std::string_view path, Handler handler);
    void Delete(std::string_view path, Handler handler);

    // 返回 RouteMatch：
    //   .handler != null              → 200，params 里是路径参数
    //   .handler == null, path_matched → 405
    //   .handler == null, !path_matched → 404
    RouteMatch Match(Method method, std::string_view path) const;
private:

    struct RouteTrieNode {
        // "静态段" -> static_child
        std::unordered_map<std::string, std::unique_ptr<RouteTrieNode>> static_child;
    
        // "参数端" -> param_child
        std::unique_ptr<RouteTrieNode> param_child;
        std::string param_name;

        // 检查方法是否合理
        std::unordered_map<Method, Handler> handlers;
    };  

    static std::vector<std::string_view> SplitPath(std::string_view path);
    static bool IsParamSegment(std::string_view seg);
    static std::string ExtractParamName(std::string_view seg);

    bool MatchNode(const RouteTrieNode *node,
                   const std::vector<std::string_view> &segment,
                   std::size_t index,
                   Method method,
                   RouteMatch &result) const;
private:
    RouteTrieNode root_;
};

}  // namespace runtime::http
