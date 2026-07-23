// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <atomic>
#include <array>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <memory>
#include <mutex>
#include <stop_token>
#include <string>
#include <thread>
#include <vector>

#include "coropact/log/log_buffer.h"
#include "coropact/utils/macros.h"

namespace coropact::log {

/**
 * AsyncLogger batches log records on a background thread using a
 * producer-consumer model with double buffering.
 */
class AsyncLogger {
public:
  static constexpr std::size_t kBufferSize = 64 * 1024;
  static constexpr std::size_t kShardCount = 8;
  using Buffer = LogBuffer<kBufferSize>;
  using BufferPtr = std::unique_ptr<Buffer>;
  using BufferQueue = std::vector<BufferPtr>;

  explicit AsyncLogger(std::string filename,
                       int flush_interval_ms = 1000,
                       std::size_t roll_size = 10 * 1024 * 1024);
  ~AsyncLogger();

  COROPACT_DELETE_COPY_MOVE(AsyncLogger);

  void Start();
  void Stop();
  void Append(const char *data, std::size_t len);

private:
  struct Shard {
    std::mutex mutex;
    BufferPtr current_buffer;
    BufferPtr next_buffer;
    BufferQueue buffers;
  };

  void ThreadFunc(std::stop_token stop_token);

  void OpenFile();
  void CloseFile();
  void FlushFile();
  void WriteBuffer(const Buffer &buffer);
  void RotateIfNeeded();
  std::string BuildRotateFilename() const;

private:
  std::string filename_;
  int flush_interval_ms_;
  std::size_t roll_size_;

  std::mutex wait_mutex_;
  std::condition_variable_any cv_;
  std::array<Shard, kShardCount> shards_;

  std::jthread backend_thread_;
  bool started_ {false};
  std::atomic<bool> accepting_{false};

  FILE *file_{nullptr};
  std::size_t written_bytes_{0};
  std::uint64_t rotate_sequence_{0};
  std::atomic<std::size_t> dropped_messages_{0};
  int last_error_code_{0};
};
}  // namespace coropact::log
