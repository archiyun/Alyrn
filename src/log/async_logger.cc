// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "vexo/log/async_logger.h"

#include <errno.h>

#include <chrono>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <iomanip>
#include <sstream>
#include <string_view>
#include <system_error>
#include <utility>

namespace vexo::log {

namespace {

constexpr std::string_view kTruncatedSuffix = "... [truncated]\n";

}  // namespace

AsyncLogger::AsyncLogger(std::string filename, int flush_interval_ms,
                         std::size_t roll_size)
    : filename_(std::move(filename)), flush_interval_ms_(flush_interval_ms),
      roll_size_(roll_size) {
  for (auto& shard : shards_) {
    shard.current_buffer = std::make_unique<Buffer>();
    shard.next_buffer = std::make_unique<Buffer>();
    shard.buffers.reserve(8);
  }
}

AsyncLogger::~AsyncLogger() { Stop(); }

void AsyncLogger::Start() {
  if (started_) {
    return;
  }

  OpenFile();
  started_ = true;
  accepting_.store(true, std::memory_order_release);
  backend_thread_ = std::jthread([this](std::stop_token stop_token) {
    ThreadFunc(stop_token);
  });
}

void AsyncLogger::Stop() {
  if (!started_) {
    return;
  }

  // Block new appends before requesting the backend to stop. Every append that
  // already holds a shard lock is drained before the backend thread exits;
  // later appenders observe accepting_ == false.
  accepting_.store(false, std::memory_order_release);
  backend_thread_.request_stop();
  cv_.notify_one();

  if (backend_thread_.joinable()) {
    backend_thread_.join();
  }

  CloseFile();
  started_ = false;
}

void AsyncLogger::Append(const char *data, std::size_t len) {
  if (data == nullptr || len == 0) {
    return;
  }

  const char* write_data = data;
  std::size_t write_len = len;
  std::string truncated_storage;

  if (len > kBufferSize) {
    const std::size_t suffix_len = kTruncatedSuffix.size();
    const std::size_t prefix_len =
        suffix_len >= kBufferSize ? 0 : (kBufferSize - suffix_len);

    truncated_storage.reserve(prefix_len + suffix_len);
    truncated_storage.append(data, prefix_len);
    truncated_storage.append(kTruncatedSuffix.data(), suffix_len);

    write_data = truncated_storage.data();
    write_len = truncated_storage.size();
  }

  thread_local const auto shard_index =
      std::hash<std::thread::id>{}(std::this_thread::get_id()) % kShardCount;
  auto& shard = shards_[shard_index];
  std::lock_guard<std::mutex> lk{shard.mutex};
  if (!accepting_.load(std::memory_order_acquire)) {
    ++dropped_messages_;
    return;
  }

  if (shard.current_buffer->Append(write_data, write_len)) {
    return;
  }

  shard.buffers.push_back(std::move(shard.current_buffer));

  if (shard.next_buffer) {
    shard.current_buffer = std::move(shard.next_buffer);
  } else {
    shard.current_buffer = std::make_unique<Buffer>();
  }

  if (!shard.current_buffer->Append(write_data, write_len)) {
    ++dropped_messages_;
    return;
  }
  cv_.notify_one();
}

void AsyncLogger::OpenFile() {
  if (filename_.empty()) {
    file_ = stdout;
    return;
  }

  file_ = std::fopen(filename_.c_str(), "a");
  if (file_ == nullptr) {
    file_ = stdout;
  }
}

void AsyncLogger::CloseFile() {
  if (file_ == nullptr) {
    return;
  }
  std::fflush(file_);
  
  if (file_ != stdout) {
    std::fclose(file_);
  }
  file_ = nullptr;
}

void AsyncLogger::FlushFile() {
  if (file_ != nullptr) {
    std::fflush(file_);
  }
}

void AsyncLogger::WriteBuffer(const Buffer &buffer) {
  if (file_ == nullptr || buffer.Empty()) {
    return;
  }

  RotateIfNeeded();

  std::size_t size = buffer.size();
  std::size_t written = 0;
  while (written < size) {
    std::size_t n = std::fwrite(buffer.data() + written, 1, size - written, file_);
    if (n > 0) {
      written += n;
      written_bytes_ += n;
      continue;
    }

    if (std::ferror(file_) != 0) {
      last_error_code_ = errno;
      std::clearerr(file_);
      break;
    }

    break;
  }
}

void AsyncLogger::RotateIfNeeded() {
  if (file_ == nullptr || file_ == stdout || filename_.empty()) {
    return;
  }

  if (written_bytes_ < roll_size_) {
    return;
  }

  std::fflush(file_);
  std::fclose(file_);
  file_ = nullptr;

  std::error_code ec;
  const std::filesystem::path current_path(filename_);
  const std::filesystem::path rotate_path(BuildRotateFilename());
  std::filesystem::rename(current_path, rotate_path, ec);
  if (ec) {
    last_error_code_ = ec.value();
  } else {
    ++rotate_sequence_;
  }

  OpenFile();
  written_bytes_ = 0;
}

std::string AsyncLogger::BuildRotateFilename() const {
  const auto now = std::chrono::system_clock::now();
  const auto current_time = std::chrono::system_clock::to_time_t(now);
  std::tm tm_time{};
#if defined(_WIN32)
  localtime_s(&tm_time, &current_time);
#else
  localtime_r(&current_time, &tm_time);
#endif

  std::ostringstream oss;
  oss << filename_ << '.'
      << std::put_time(&tm_time, "%Y%m%d-%H%M%S")
      << '.' << rotate_sequence_;
  return oss.str();
}

void AsyncLogger::ThreadFunc(std::stop_token stop_token) {
  BufferQueue buffers_to_write;
  buffers_to_write.reserve(kShardCount * 4);
  BufferQueue spare_buffers;
  spare_buffers.reserve(kShardCount * 2);

  while (!stop_token.stop_requested()) {
    for (auto& shard : shards_) {
      std::lock_guard<std::mutex> lk{shard.mutex};
      if (shard.current_buffer && !shard.current_buffer->Empty()) {
        shard.buffers.push_back(std::move(shard.current_buffer));
      }
      while (!shard.buffers.empty()) {
        buffers_to_write.push_back(std::move(shard.buffers.back()));
        shard.buffers.pop_back();
      }
      if (!shard.current_buffer) {
        if (!spare_buffers.empty()) {
          shard.current_buffer = std::move(spare_buffers.back());
          spare_buffers.pop_back();
        } else {
          shard.current_buffer = std::make_unique<Buffer>();
        }
      }
      if (!shard.next_buffer) {
        if (!spare_buffers.empty()) {
          shard.next_buffer = std::move(spare_buffers.back());
          spare_buffers.pop_back();
        } else {
          shard.next_buffer = std::make_unique<Buffer>();
        }
      }
    }

    if (buffers_to_write.empty()) {
      std::unique_lock<std::mutex> lk{wait_mutex_};
      cv_.wait_for(lk, std::chrono::milliseconds(flush_interval_ms_));
      continue;
    }

    for (auto &buffer : buffers_to_write) {
      WriteBuffer(*buffer);
    }

    FlushFile();

    while (!buffers_to_write.empty()) {
      auto buffer = std::move(buffers_to_write.back());
      buffers_to_write.pop_back();
      if (buffer) {
        buffer->Reset();
        spare_buffers.push_back(std::move(buffer));
      }
    }
  }

  for (auto& shard : shards_) {
    std::lock_guard<std::mutex> lk{shard.mutex};
    if (shard.current_buffer && !shard.current_buffer->Empty()) {
      shard.buffers.push_back(std::move(shard.current_buffer));
    }
    while (!shard.buffers.empty()) {
      buffers_to_write.push_back(std::move(shard.buffers.back()));
      shard.buffers.pop_back();
    }
  }

  for (auto &buffer : buffers_to_write) {
    WriteBuffer(*buffer);
  }

  FlushFile();
}
} // namespace vexo::log
