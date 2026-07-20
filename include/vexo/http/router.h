// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <source_location>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "vexo/http/http_request.h"
#include "vexo/http/http_response.h"
#include "vexo/http/http_types.h"

namespace vexo::http {

namespace detail {

// Transparent hash wrapper so unordered_map<std::string, ...> can be looked up
// with std::string_view without constructing a temporary std::string.
// C++23 heterogeneous lookup requires is_transparent on BOTH Hash and KeyEqual;
// std::hash<std::string_view> alone does not carry that tag, hence this wrapper.
struct TransparentStringHash {
  using is_transparent = void;
  std::size_t operator()(std::string_view v) const noexcept {
    return std::hash<std::string_view>{}(v);
  }
  std::size_t operator()(const std::string& s) const noexcept {
    return std::hash<std::string_view>{}(s);
  }
  std::size_t operator()(const char* s) const noexcept {
    return std::hash<std::string_view>{}(s);
  }
};

}  // namespace detail

// Handler is the business callback executed after a request has been parsed.
using Handler = std::function<void(const HttpRequest&, HttpResponse&)>;

// RouteMatch describes the router result for one method/path lookup.
struct RouteMatch {
  Handler handler{};
  std::vector<PathParam> params;
  bool path_matched{false};
};

// Router stores HTTP routes in a trie and supports static segments and
// parameter segments such as "/users/:id".
class Router {
public:
  // Registration is fail-fast: any structural error (bad path, empty handler,
  // param-name conflict, duplicate (method, path)) aborts the process and
  // logs the caller's file:line via std::source_location. Routes are a
  // startup-time invariant — there is no runtime recovery path for a
  // misconfigured table, so we surface the bug immediately.
  void Add(Method method, std::string_view path, Handler handler,
           std::source_location loc = std::source_location::current());
  void Get(std::string_view path, Handler handler,
           std::source_location loc = std::source_location::current());
  void Post(std::string_view path, Handler handler,
            std::source_location loc = std::source_location::current());
  void Put(std::string_view path, Handler handler,
           std::source_location loc = std::source_location::current());
  void Delete(std::string_view path, Handler handler,
              std::source_location loc = std::source_location::current());

  // Returns a RouteMatch.
  // - handler != null and path_matched == true means a route was found
  // - handler == null and path_matched == true means 405 Method Not Allowed
  // - handler == null and path_matched == false means 404 Not Found
  RouteMatch Match(Method method, std::string_view path) const;
private:
  struct RouteTrieNode {
    std::unordered_map<std::string, std::unique_ptr<RouteTrieNode>,
                       detail::TransparentStringHash, std::equal_to<>>
        static_child;
    std::unique_ptr<RouteTrieNode> param_child;
    std::string param_name;
    std::unordered_map<Method, Handler> handlers;
  };

  // Canonical path segmentation. The ONLY function in the project that
  // splits a route path into segments. Both Add() and Match() MUST use it,
  // otherwise registered routes can become unreachable.
  //
  // Normalization rules (silent, by design):
  //   - leading '/' stripped
  //   - empty segments dropped: "//foo" → ["foo"], "/foo/" → ["foo"]
  //   - trailing '/' has no semantic effect: "/foo/" matches the same as "/foo"
  //
  // If you need to distinguish "/foo" from "/foo/", do not patch this
  // function — add a separate strict matcher.
  static void SplitPath(std::string_view path, std::vector<std::string_view>& segments);

  static bool IsParamSegment(std::string_view seg);
  static void ExtractParamName(std::string_view seg, std::string& param_name);

  bool MatchNode(const RouteTrieNode* node,
                 const std::vector<std::string_view>& segments,
                 std::size_t index,
                 Method method,
                 RouteMatch& result) const;
  RouteTrieNode root_;
};

}  // namespace vexo::http
