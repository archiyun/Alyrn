// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <atomic>
#if __has_include(<format>) && !defined(COROPACT_LOG_DISABLE_STD_FORMAT)
#include <format>
#endif
#include <memory>
#include <mutex>
#include <source_location>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>

#if defined(__cpp_lib_format) && __cpp_lib_format >= 201907L
#define COROPACT_LOG_HAS_STD_FORMAT 1
#else
#define COROPACT_LOG_HAS_STD_FORMAT 0
#endif

#include "coropact/base/singleton.h"
#include "coropact/utils/macros.h"

namespace coropact::log {

namespace detail {

#if !COROPACT_LOG_HAS_STD_FORMAT

template <std::size_t Index = 0, typename Tuple>
void AppendFallbackArgument(std::ostringstream& stream, Tuple& arguments,
                            std::size_t index) {
  if constexpr (Index < std::tuple_size_v<std::remove_reference_t<Tuple>>) {
    if (Index == index) {
      stream << std::get<Index>(arguments);
    } else {
      AppendFallbackArgument<Index + 1>(stream, arguments, index);
    }
  } else {
    stream << "{}";
  }
}

template <typename... Args>
std::string FallbackFormat(std::string_view format, Args&&... args) {
  std::ostringstream stream;
  auto arguments = std::forward_as_tuple(std::forward<Args>(args)...);
  std::size_t argument_index = 0;

  for (std::size_t i = 0; i < format.size(); ++i) {
    if (format[i] == '{' && i + 1 < format.size() && format[i + 1] == '{') {
      stream.put('{');
      ++i;
    } else if (format[i] == '}' && i + 1 < format.size() && format[i + 1] == '}') {
      stream.put('}');
      ++i;
    } else if (format[i] == '{' && i + 1 < format.size() && format[i + 1] == '}') {
      AppendFallbackArgument(stream, arguments, argument_index++);
      ++i;
    } else {
      stream.put(format[i]);
    }
  }
  return stream.str();
}

#endif

}  // namespace detail

class AsyncLogger;

// LogLevel orders severities from low to high for threshold filtering.
enum class LogLevel : uint8_t { DEBUG = 0, INFO, WARN, ERROR, FATAL };

// Logger formats log records and forwards them to AsyncLogger.
class Logger : public coropact::base::Singleton<Logger> {
public:
  void Init(const std::string& filename, LogLevel level = LogLevel::INFO,
            int flush_interval_ms = 1000, std::size_t roll_size = 10 * 1024 * 1024);

  void Shutdown();

  void set_log_level(LogLevel level);
  LogLevel log_level() const;
  bool ShouldLog(LogLevel level) const;

  // Log filters by level, formats the record, and appends it asynchronously.
  void Log(LogLevel level, const char* file, int line, const char* func, std::string_view message);

  // Preferred overload: captures the call site without requiring a macro to
  // spell out file, line, and function name separately.
  void Log(LogLevel level, std::string_view message,
           std::source_location location = std::source_location::current());

  // Type-safe C++23 formatting path. The source location is passed explicitly
  // so convenience macros preserve the caller's location.
  template <typename... Args>
  void Logf(LogLevel level, std::source_location location,
#if COROPACT_LOG_HAS_STD_FORMAT
            std::format_string<Args...> format, Args&&... args) {
#else
            std::string_view format, Args&&... args) {
#endif
    if (!ShouldLog(level)) {
      return;
    }
#if COROPACT_LOG_HAS_STD_FORMAT
    Log(level, std::format(format, std::forward<Args>(args)...), location);
#else
    Log(level, detail::FallbackFormat(format, std::forward<Args>(args)...), location);
#endif
  }

private:
  friend class coropact::base::Singleton<Logger>;

  Logger();
  ~Logger();

  std::atomic<LogLevel> level_{LogLevel::INFO};
  mutable std::shared_mutex lifecycle_mutex_;
  std::unique_ptr<AsyncLogger> async_logger_;
};

const char* ToString(LogLevel level);

class LogMessage {
public:
  LogMessage(LogLevel level, std::source_location location)
      : level_(level), location_(location) {}

  ~LogMessage() { Logger::Instance().Log(level_, stream_.str(), location_); }

  COROPACT_DELETE_COPY_MOVE(LogMessage);

  std::ostringstream& Stream() { return stream_; }

 private:
  LogLevel level_;
  std::source_location location_;
  std::ostringstream stream_;
};

}  // namespace coropact::log

#define LOG(level)                                                              \
  if (!::coropact::log::Logger::Instance().ShouldLog(::coropact::log::LogLevel::level)) \
    ;                                                                           \
  else                                                                          \
    ::coropact::log::LogMessage(::coropact::log::LogLevel::level,                     \
                            std::source_location::current()).Stream()

#define LOG_DEBUG() LOG(DEBUG)
#define LOG_INFO() LOG(INFO)
#define LOG_WARN() LOG(WARN)
#define LOG_ERROR() LOG(ERROR)
#define LOG_FATAL() LOG(FATAL)

#define COROPACT_LOGF(level, format, ...)                                           \
  ::coropact::log::Logger::Instance().Logf(                                        \
      ::coropact::log::LogLevel::level, std::source_location::current(), format    \
      __VA_OPT__(, ) __VA_ARGS__)

#define LOG_DEBUGF(format, ...) COROPACT_LOGF(DEBUG, format __VA_OPT__(, ) __VA_ARGS__)
#define LOG_INFOF(format, ...) COROPACT_LOGF(INFO, format __VA_OPT__(, ) __VA_ARGS__)
#define LOG_WARNF(format, ...) COROPACT_LOGF(WARN, format __VA_OPT__(, ) __VA_ARGS__)
#define LOG_ERRORF(format, ...) COROPACT_LOGF(ERROR, format __VA_OPT__(, ) __VA_ARGS__)
#define LOG_FATALF(format, ...) COROPACT_LOGF(FATAL, format __VA_OPT__(, ) __VA_ARGS__)
