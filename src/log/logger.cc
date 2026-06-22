// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "vexo/log/logger.h"

#include <string>
#include <string_view>

#include "vexo/base/current_thread.h"
#include "vexo/log/async_logger.h"
#include "vexo/time/timestamp.h"

namespace vexo::log {

namespace {

std::string FormatLogMessage(LogLevel level, const char* file, int line,
                             const char* func, std::string_view message) {
  std::string result;
  result.reserve(128);
  result += '[';
  result += vexo::time::Timestamp::Now().ToFormattedString();
  result += "] [";
  result += ToString(level);
  result += "] [tid:";
  result += std::to_string(vexo::base::tid());
  result += "] [";
  result += file;
  result += ':';
  result += std::to_string(line);
  result += "] [";
  result += func;
  result += "] ";
  result += message;
  result += '\n';
  return result;
}

}  // namespace

Logger& Logger::Instance() {
    static Logger logger;
    return logger;
}

void Logger::Init(const std::string &filename,
                  LogLevel level,
                  int flush_interval_ms,
                  std::size_t roll_size) {
    if (async_logger_) {
        async_logger_->Stop();
    }

    level_.store(level, std::memory_order_relaxed);
    async_logger_ = std::make_unique<AsyncLogger>(filename, flush_interval_ms, roll_size);
    async_logger_->Start();
}

void Logger::Shutdown() {
    if (async_logger_) {
        async_logger_->Stop();
        async_logger_.reset();
    }
}

void Logger::set_log_level(LogLevel level) {
    level_.store(level, std::memory_order_relaxed);
}

LogLevel Logger::log_level() const {
    return level_.load(std::memory_order_relaxed);
}

bool Logger::ShouldLog(LogLevel level) const {
    return level >= level_.load(std::memory_order_relaxed);
}

void Logger::Log(LogLevel level,
                 const char *file,
                 int line,
                 const char *func,
                 std::string_view message) {
    if (level < level_.load(std::memory_order_relaxed)) {
        return;
    }

    if (!async_logger_) {
        return;
    }

    const std::string formatted =
        FormatLogMessage(level, file, line, func, message);

    async_logger_->Append(formatted.data(), formatted.size());
}

const char* ToString(LogLevel level) {
    switch (level) {
        case LogLevel::DEBUG:
            return "DEBUG";
        case LogLevel::INFO:
            return "INFO";
        case LogLevel::WARN:
            return "WARN";
        case LogLevel::ERROR:
            return "ERROR";
        case LogLevel::FATAL:
            return "FATAL";
        default:
            return "UNKNOWN";
    }
}

}  // namespace vexo::log
