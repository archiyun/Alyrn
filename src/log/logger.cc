// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "coropact/log/logger.h"

#include <charconv>
#include <cstdint>
#include <ctime>
#include <iterator>
#include <limits>
#include <string>
#include <string_view>

#include "coropact/base/current_thread.h"
#include "coropact/log/async_logger.h"
#include "coropact/time/timestamp.h"

namespace coropact::log {

namespace {

void AppendUnsigned(std::string& output, std::uint64_t value) {
  char buffer[std::numeric_limits<std::uint64_t>::digits10 + 3];
  const auto result = std::to_chars(std::begin(buffer), std::end(buffer), value);
  output.append(buffer, result.ptr);
}

void AppendTimestamp(std::string& output) {
  const auto timestamp = coropact::time::Timestamp::Now();
  const auto seconds = static_cast<std::time_t>(timestamp.SecondsSinceEpoch());
  std::tm tm_time{};
#if defined(_WIN32)
  localtime_s(&tm_time, &seconds);
#else
  localtime_r(&seconds, &tm_time);
#endif

  char calendar[32];
  const auto length = std::strftime(calendar, sizeof(calendar), "%F %T", &tm_time);
  if (length != 0) {
    output.append(calendar, length);
  } else {
    output.append("1970-01-01 00:00:00");
  }

  output.push_back('.');
  const auto micros = timestamp.MicrosecondsSinceEpoch() % 1'000'000;
  char fraction[6];
  const auto fraction_result =
      std::to_chars(std::begin(fraction), std::end(fraction), micros);
  const auto fraction_length = static_cast<std::size_t>(fraction_result.ptr - fraction);
  if (fraction_length < 6) {
    output.append(6 - fraction_length, '0');
  }
  output.append(fraction, fraction_result.ptr);
}

std::string FormatLogMessage(LogLevel level, const char* file, int line, const char* func,
                             std::string_view message) {
  std::string result;
  result.reserve(96 + message.size());
  result += '[';
  AppendTimestamp(result);
  result += "] [";
  result += ToString(level);
  result += "] [tid:";
  AppendUnsigned(result, static_cast<std::uint64_t>(coropact::base::tid()));
  result += "] [";
  result += file;
  result += ':';
  if (line < 0) {
    result.push_back('-');
    AppendUnsigned(result, static_cast<std::uint64_t>(-(static_cast<std::int64_t>(line))));
  } else {
    AppendUnsigned(result, static_cast<std::uint64_t>(line));
  }
  result += "] [";
  result += func;
  result += "] ";
  result += message;
  result += '\n';
  return result;
}

}  // namespace

Logger::Logger() = default;

Logger::~Logger() = default;

void Logger::Init(const std::string& filename, LogLevel level, int flush_interval_ms,
                  std::size_t roll_size) {
  std::unique_lock lock{lifecycle_mutex_};
  if (async_logger_) {
    async_logger_->Stop();
  }

  level_.store(level, std::memory_order_relaxed);
  async_logger_ = std::make_unique<AsyncLogger>(filename, flush_interval_ms, roll_size);
  async_logger_->Start();
}

void Logger::Shutdown() {
  std::unique_lock lock{lifecycle_mutex_};
  if (async_logger_) {
    async_logger_->Stop();
    async_logger_.reset();
  }
}

void Logger::set_log_level(LogLevel level) { level_.store(level, std::memory_order_relaxed); }

LogLevel Logger::log_level() const { return level_.load(std::memory_order_relaxed); }

bool Logger::ShouldLog(LogLevel level) const {
  return level >= level_.load(std::memory_order_relaxed);
}

void Logger::Log(LogLevel level, const char* file, int line, const char* func,
                 std::string_view message) {
  std::shared_lock lock{lifecycle_mutex_};
  if (level < level_.load(std::memory_order_relaxed)) {
    return;
  }

  if (!async_logger_) {
    return;
  }

  const std::string formatted = FormatLogMessage(level, file, line, func, message);

  async_logger_->Append(formatted.data(), formatted.size());
}

void Logger::Log(LogLevel level, std::string_view message,
                 std::source_location location) {
  Log(level, location.file_name(), static_cast<int>(location.line()),
      location.function_name(), message);
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

}  // namespace coropact::log
