// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <cstddef>
#include <cstring>

namespace vexo::log {

template <std::size_t SIZE> 
class LogBuffer {
public:
  LogBuffer() = default;

  bool Append(const char *data, std::size_t len) {
    if (len > Avail()) {
      return false;
    }

    std::memcpy(data_ + cur_, data, len);
    cur_ += len;
    return true;
  }
  const char *data() const { return data_; }

  std::size_t capacity() const { return SIZE; }

  std::size_t size() const { return cur_; }

  std::size_t Avail() const { return SIZE - cur_; }

  bool Empty() const { return cur_ == 0; }

  void Reset() { cur_ = 0; }

private:
  char data_[SIZE]{};
  std::size_t cur_{0};
};

}; // namespace vexo::log
