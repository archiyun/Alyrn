// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <atomic>
#include <format>
#include <memory>
#include <mutex>
#include <source_location>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>

#include "vexo/base/singleton.h"
#include "vexo/utils/macros.h"

namespace vexo::log {

class AsyncLogger;

// LogLevel orders severities from low to high for threshold filtering.
enum class LogLevel : uint8_t { DEBUG = 0, INFO, WARN, ERROR, FATAL };

// Logger formats log records and forwards them to AsyncLogger.
class Logger : public vexo::base::Singleton<Logger> {
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
            std::format_string<Args...> format, Args&&... args) {
    if (!ShouldLog(level)) {
      return;
    }
    Log(level, std::format(format, std::forward<Args>(args)...), location);
  }

private:
  friend class vexo::base::Singleton<Logger>;

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

  VEXO_DELETE_COPY_MOVE(LogMessage);

  std::ostringstream& Stream() { return stream_; }

 private:
  LogLevel level_;
  std::source_location location_;
  std::ostringstream stream_;
};

}  // namespace vexo::log

#define LOG(level)                                                              \
  if (!::vexo::log::Logger::Instance().ShouldLog(::vexo::log::LogLevel::level)) \
    ;                                                                           \
  else                                                                          \
    ::vexo::log::LogMessage(::vexo::log::LogLevel::level,                     \
                            std::source_location::current()).Stream()

#define LOG_DEBUG() LOG(DEBUG)
#define LOG_INFO() LOG(INFO)
#define LOG_WARN() LOG(WARN)
#define LOG_ERROR() LOG(ERROR)
#define LOG_FATAL() LOG(FATAL)

#define VEXO_LOGF(level, format, ...)                                           \
  ::vexo::log::Logger::Instance().Logf(                                        \
      ::vexo::log::LogLevel::level, std::source_location::current(), format    \
      __VA_OPT__(, ) __VA_ARGS__)

#define LOG_DEBUGF(format, ...) VEXO_LOGF(DEBUG, format __VA_OPT__(, ) __VA_ARGS__)
#define LOG_INFOF(format, ...) VEXO_LOGF(INFO, format __VA_OPT__(, ) __VA_ARGS__)
#define LOG_WARNF(format, ...) VEXO_LOGF(WARN, format __VA_OPT__(, ) __VA_ARGS__)
#define LOG_ERRORF(format, ...) VEXO_LOGF(ERROR, format __VA_OPT__(, ) __VA_ARGS__)
#define LOG_FATALF(format, ...) VEXO_LOGF(FATAL, format __VA_OPT__(, ) __VA_ARGS__)
