// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "runtime/http/router.h"

#include <iostream>
#include <sstream>
#include <utility>

#include "runtime/log/logger.h"
namespace runtime::http {

namespace {
[[noreturn]] void RouteFail(std::string_view reason,
                            std::string_view path,
                            std::source_location loc) {
  // Build the message once and emit it twice: directly to stderr (so the
  // diagnostic survives even when the global logger has not been initialized
  // — route registration usually happens before logger setup — and so that
  // EXPECT_DEATH can match against the subprocess stderr), and through the
  // logger for production deployments that route FATAL to a real sink.
  std::ostringstream oss;
  oss << "router: " << reason
      << " (path=\"" << path << "\")"
      << " at " << loc.file_name() << ":" << loc.line();
  const std::string msg = oss.str();
  std::cerr << msg << '\n';
  LOG_FATAL() << msg;
  std::abort();
}
} // namespace

void Router::SplitPath(std::string_view path, std::vector<std::string_view>& segments) {
  std::size_t start = 0;
  const std::size_t n = path.size();

  if (n != 0 && path[0] == '/')
    start = 1;
  while (start < n) {
    auto end = path.find('/', start);
    if (end == std::string_view::npos)
      end = n;
    if (end > start) {
      segments.push_back(path.substr(start, end - start));
    }
    start = end + 1;
  }
}

bool Router::IsParamSegment(std::string_view seg) {
  return !seg.empty() && seg[0] == ':';
}

void Router::ExtractParamName(std::string_view seg, std::string& param_name) {
  param_name = std::string(seg.substr(1));
}

void Router::Add(Method method, std::string_view path, Handler handler,
                 std::source_location loc) {
  if (path.empty() || path.front() != '/')
    RouteFail("path must start with '/'", path, loc);
  if (!handler)
    RouteFail("handler must not be empty", path, loc);

  std::vector<std::string_view> segments;
  SplitPath(path, segments);
  RouteTrieNode* node = &root_;

  for (const auto& seg : segments) {
    if (IsParamSegment(seg)) {
      std::string param_name;
      ExtractParamName(seg, param_name);

      // ":" with no name following is a typo, not a wildcard.
      if (param_name.empty())
        RouteFail("empty parameter name after ':'", path, loc);

      if (!node->param_child) {
        node->param_child = std::make_unique<RouteTrieNode>();
        node->param_child->param_name = std::move(param_name);
      } else if (node->param_child->param_name != param_name) {
        // Same depth already bound to a different param name — refusing this
        // is the only way to keep the trie unambiguous; capturing both would
        // silently shadow one route under the other.
        std::ostringstream oss;
        oss << "param name conflict at same depth (existing '"
            << node->param_child->param_name
            << "' vs new '" << param_name << "')";
        RouteFail(oss.str(), path, loc);
      }
      node = node->param_child.get();
    } else {
      auto& child = node->static_child[std::string(seg)];
      if (!child)
        child = std::make_unique<RouteTrieNode>();
      node = child.get();
    }
  }

  // Same (method, path) registered twice: previously silently overwrote the
  // earlier handler, which hid double-registration bugs across modules.
  if (node->handlers.find(method) != node->handlers.end())
    RouteFail("duplicate registration for the same (method, path)", path, loc);

  node->handlers[method] = std::move(handler);
}

void Router::Get(std::string_view path, Handler handler,
                 std::source_location loc) {
  Add(Method::Get, path, std::move(handler), loc);
}

void Router::Post(std::string_view path, Handler handler,
                  std::source_location loc) {
  Add(Method::Post, path, std::move(handler), loc);
}

void Router::Put(std::string_view path, Handler handler,
                 std::source_location loc) {
  Add(Method::Put, path, std::move(handler), loc);
}

void Router::Delete(std::string_view path, Handler handler,
                    std::source_location loc) {
  Add(Method::Delete, path, std::move(handler), loc);
}

bool Router::MatchNode(const RouteTrieNode* node,
                       const std::vector<std::string_view>& segments,
                       std::size_t index,
                       Method method,
                       RouteMatch& result) const {
  if (index == segments.size()) {
    if (node->handlers.empty())
      return false;

    result.path_matched = true;

    const auto it = node->handlers.find(method);
    if (it != node->handlers.end()) {
      result.handler = it->second;
    }
    return true;
  }

  const std::string_view seg = segments[index];

  // Prefer static segments over parameter segments so "/users/me" does not get
  // captured by "/users/:id" when both routes exist. Heterogeneous find avoids
  // constructing a temporary std::string on every segment lookup.
  const auto sit = node->static_child.find(seg);
  if (sit != node->static_child.end() &&
      MatchNode(sit->second.get(), segments, index + 1, method, result)) {
    return true;
  }

  if (node->param_child) {
    const std::size_t saved = result.params.size();
    result.params.push_back({node->param_child->param_name, std::string(seg)});
    if (MatchNode(node->param_child.get(), segments, index + 1, method,
                  result)) {
      return true;
    }
    result.params.resize(saved);
  }
  return false;
}

RouteMatch Router::Match(Method method, std::string_view path) const {
  std::vector<std::string_view> segments;
  SplitPath(path, segments);
  RouteMatch result;
  MatchNode(&root_, segments, 0, method, result);
  return result;
}

}  // namespace runtime::http
