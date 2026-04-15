#include "runtime/log/async_logger.h"

#include <chrono>
#include <ctime>
#include <cstring>
#include <errno.h>
#include <filesystem>
#include <iomanip>
#include <sstream>
#include <string_view>
#include <system_error>
#include <utility>

namespace runtime::log {

namespace {

constexpr std::string_view kTruncatedSuffix = "... [truncated]\n";

}  // namespace

AsyncLogger::AsyncLogger(std::string filename, int flush_interval_ms,
                         std::size_t roll_size)
    : filename_(std::move(filename)), flush_interval_ms_(flush_interval_ms),
      roll_size_(roll_size), current_buffer_(std::make_unique<Buffer>()),
      next_buffer_(std::make_unique<Buffer>()) {}

AsyncLogger::~AsyncLogger() { Stop(); }

void AsyncLogger::Start() {
  if (started_) {
    return;
  }

  OpenFile();
  started_ = true;
  backend_thread_ = std::jthread([this](std::stop_token stop_token) {
    ThreadFunc(stop_token);
  });
}

void AsyncLogger::Stop() {
  if (!started_) {
    return;
  }

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

  std::lock_guard<std::mutex> lk{mutex_};

  if (current_buffer_->Append(write_data, write_len)) {
    return;
  }

  buffers_.push_back(std::move(current_buffer_));

  if (next_buffer_) {
    current_buffer_ = std::move(next_buffer_);
  } else {
    current_buffer_ = std::make_unique<Buffer>();
  }

  if (!current_buffer_->Append(write_data, write_len)) {
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

  std::size_t size = buffer.Size();
  std::size_t written = 0;
  while (written < size) {
    std::size_t n = std::fwrite(buffer.Data() + written, 1, size - written, file_);
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
  buffers_to_write.reserve(16);

  auto spare_buffer1 = std::make_unique<Buffer>();
  auto spare_buffer2 = std::make_unique<Buffer>();

  while (!stop_token.stop_requested()) {
    {
      std::unique_lock<std::mutex> lk{mutex_};

      if (buffers_.empty() &&
          (current_buffer_ == nullptr || current_buffer_->Empty())) {
        cv_.wait_for(lk, std::chrono::milliseconds(flush_interval_ms_));
      }

      if (current_buffer_ && !current_buffer_->Empty()) {
        buffers_.push_back(std::move(current_buffer_));
      }

      buffers_to_write.swap(buffers_);

      if (!current_buffer_) {
        if (spare_buffer1) {
          current_buffer_ = std::move(spare_buffer1);
        } else if (spare_buffer2) {
          current_buffer_ = std::move(spare_buffer2);
        } else {
          current_buffer_ = std::make_unique<Buffer>();
        }
      }

      if (!next_buffer_) {
        if (spare_buffer1) {
          next_buffer_ = std::move(spare_buffer1);
        } else if (spare_buffer2) {
          next_buffer_ = std::move(spare_buffer2);
        } else {
          next_buffer_ = std::make_unique<Buffer>();
        }
      }
    }

    if (buffers_to_write.empty()) {
      continue;
    }

    for (auto &buffer : buffers_to_write) {
      WriteBuffer(*buffer);
    }

    FlushFile();

    while (!spare_buffer1 && !buffers_to_write.empty()) {
      auto buffer = std::move(buffers_to_write.back());
      buffers_to_write.pop_back();
      if (buffer) {
        buffer->Reset();
        spare_buffer1 = std::move(buffer);
      }
    }

    while (!spare_buffer2 && !buffers_to_write.empty()) {
      auto buffer = std::move(buffers_to_write.back());
      buffers_to_write.pop_back();
      if (buffer) {
        buffer->Reset();
        spare_buffer2 = std::move(buffer);
      }
    }

    buffers_to_write.clear();
  }

  {
    std::lock_guard<std::mutex> lk{mutex_};
    if (current_buffer_ && !current_buffer_->Empty()) {
      buffers_.push_back(std::move(current_buffer_));
    }
    buffers_to_write.swap(buffers_);
  }

  for (auto &buffer : buffers_to_write) {
    WriteBuffer(*buffer);
  }

  FlushFile();
}
} // namespace runtime::log
