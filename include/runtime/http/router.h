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

// Handler is the business callback executed after a request has been parsed.
using Handler = std::function<void(const HttpRequest&, HttpResponse&)>;

// RouteMatch describes the router result for one method/path lookup.
struct RouteMatch {
  Handler handler{nullptr};
  std::unordered_map<std::string, std::string> params;
  bool path_matched{false};
};

// Router stores HTTP routes in a trie and supports static segments and
// parameter segments such as "/users/:id".
class Router {
public:
  void Add(Method method, std::string_view path, Handler handler);
  void Get(std::string_view path, Handler handler);
  void Post(std::string_view path, Handler handler);
  void Put(std::string_view path, Handler handler);
  void Delete(std::string_view path, Handler handler);

  // Returns a RouteMatch.
  // - handler != null and path_matched == true means a route was found
  // - handler == null and path_matched == true means 405 Method Not Allowed
  // - handler == null and path_matched == false means 404 Not Found
  RouteMatch Match(Method method, std::string_view path) const;
private:
  struct RouteTrieNode {
    std::unordered_map<std::string, std::unique_ptr<RouteTrieNode>>
        static_child;
    std::unique_ptr<RouteTrieNode> param_child;
    std::string param_name;
    std::unordered_map<Method, Handler> handlers;
  };

  static std::vector<std::string_view> SplitPath(std::string_view path);
  static bool IsParamSegment(std::string_view seg);
  static std::string ExtractParamName(std::string_view seg);

  bool MatchNode(const RouteTrieNode* node,
                 const std::vector<std::string_view>& segments,
                 std::size_t index,
                 Method method,
                 RouteMatch& result) const;
private:
  RouteTrieNode root_;
};

}  // namespace runtime::http
