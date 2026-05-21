// Copyright (c) 2026 Aresna
// SPDX-License-Identifier: MIT
#include "runtime/http/router.h"

#include <utility>
namespace runtime::http {

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

void Router::Add(Method method, std::string_view path, Handler handler) {
  std::vector<std::string_view> segments;
  SplitPath(path, segments);
  RouteTrieNode* node = &root_;

  for (const auto& seg : segments) {
    if (IsParamSegment(seg)) {
      std::string param_name;
      ExtractParamName(seg, param_name);

      if (param_name.empty()) return;

      if (!node->param_child) {
        node->param_child = std::make_unique<RouteTrieNode>();
        node->param_child->param_name = std::move(param_name);
      } else if (node->param_child->param_name != param_name) {
        return;
      }
      node = node->param_child.get();
    } else {
      auto& child = node->static_child[std::string(seg)];
      if (!child)
        child = std::make_unique<RouteTrieNode>();
      node = child.get();
    }
  }
  node->handlers[method] = std::move(handler);
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
