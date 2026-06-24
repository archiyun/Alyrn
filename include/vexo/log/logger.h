// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <atomic>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>

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

private:
  friend class vexo::base::Singleton<Logger>;

  Logger();
  ~Logger();

  std::atomic<LogLevel> level_{LogLevel::INFO};
  std::unique_ptr<AsyncLogger> async_logger_;
};

const char* ToString(LogLevel level);

class LogMessage {
public:
  LogMessage(LogLevel level, const char* file, int line, const char* func)
      : level_(level), file_(file), line_(line), func_(func) {}

  ~LogMessage() { Logger::Instance().Log(level_, file_, line_, func_, stream_.str()); }

  VEXO_DELETE_COPY_MOVE(LogMessage);

  std::ostringstream& Stream() { return stream_; }

private:
  LogLevel level_;
  const char* file_;
  int line_;
  const char* func_;
  std::ostringstream stream_;
};

}  // namespace vexo::log

#define LOG(level)                                                              \
  if (!::vexo::log::Logger::Instance().ShouldLog(::vexo::log::LogLevel::level)) \
    ;                                                                           \
  else                                                                          \
    ::vexo::log::LogMessage(::vexo::log::LogLevel::level, __FILE__, __LINE__, __func__).Stream()

#define LOG_DEBUG() LOG(DEBUG)
#define LOG_INFO() LOG(INFO)
#define LOG_WARN() LOG(WARN)
#define LOG_ERROR() LOG(ERROR)
#define LOG_FATAL() LOG(FATAL)
