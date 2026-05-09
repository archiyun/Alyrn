#pragma once

#include "runtime/http/http_request.h"
#include "runtime/http/http_response.h"

#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>
#include <unordered_map>

namespace runtime::gateway {

inline std::string_view MimeType(const std::string& ext) {
  static const std::unordered_map<std::string, std::string_view> kMimes = {
    {".html",  "text/html; charset=utf-8"},
    {".css",   "text/css; charset=utf-8"},
    {".js",    "application/javascript; charset=utf-8"},
    {".json",  "application/json; charset=utf-8"},
    {".png",   "image/png"},
    {".jpg",   "image/jpeg"},
    {".svg",   "image/svg+xml"},
    {".ico",   "image/x-icon"},
    {".woff2", "font/woff2"},
  };
  if (auto it = kMimes.find(ext); it != kMimes.end())  return it->second;
  return "application/octet-stream";
}

// Resolves rel_path under root, guards against path traversal, reads the file.
// Returns false if the path is illegal or the file does not exist.
inline bool ServeFile(const std::filesystem::path& root,
                      std::string_view rel,
                      runtime::http::HttpResponse& resp) {
  namespace fs = std::filesystem;

  fs::path target = root / rel.substr(rel.starts_with('/') ? 1 : 0);
  if (fs::is_directory(target)) target /= "index.html";

  std::error_code ec;
  auto canonical = fs::canonical(target, ec);
  if (ec || !canonical.native().starts_with(root.native())) return false;

  std::ifstream f(canonical, std::ios::binary);
  if (!f) return false;

  std::string body(std::istreambuf_iterator<char>(f), {});
  resp.SetStatusCode(runtime::http::StatusCode::Ok);
  resp.SetContentType(std::string(MimeType(canonical.extension().string())));
  resp.SetBody(std::move(body));
  return true;
}

} // namespace runtime::gateway
