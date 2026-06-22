// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

#include <cstring>

#include "vexo/net/buffer.h"

namespace vexo::net {

Buffer::Buffer(std::size_t initial_size)
  : buffer_(kCheapPrepend + initial_size),
    reader_index_(kCheapPrepend),
    writer_index_(kCheapPrepend) {
  VEXO_ASSERT(reader_index_ == writer_index_, "buffer must start empty");
  AssertInvariant();
}

ssize_t Buffer::ReadFd(int fd, int* saved_errno) {
  char extrabuf[65536];
  iovec vec[2];

  const std::size_t writable = writable_bytes();
  vec[0].iov_base = BeginWrite();
  vec[0].iov_len = writable;
  vec[1].iov_base = extrabuf;
  vec[1].iov_len = sizeof(extrabuf);

  // Try to read directly into the writable region first, and spill any excess
  // bytes into a temporary stack buffer. This avoids an extra read when the
  // current writable region is smaller than the incoming payload.
  const int iovcnt = writable < sizeof(extrabuf) ? 2 : 1;
  const ssize_t n = ::readv(fd, vec, iovcnt);
  if (n < 0) {
    if (saved_errno != nullptr) {
      *saved_errno = errno;
    }
  } else if (static_cast<std::size_t>(n) <= writable) {
    writer_index_ += static_cast<std::size_t>(n);
  } else {
    writer_index_ = buffer_.size();
    Append(extrabuf, static_cast<std::size_t>(n) - writable);
  }

  return n;
}

ssize_t Buffer::WriteFd(int fd, int* saved_errno) {
  const ssize_t n = ::send(fd, Peek(), readable_bytes(), MSG_NOSIGNAL);
  if (n < 0) {
    if (saved_errno != nullptr) {
      *saved_errno = errno;
    }
    return n;
  }

  Retrieve(static_cast<std::size_t>(n));
  return n;
}

void Buffer::MakeSpace(std::size_t len) {
  if (writable_bytes() + prependable_bytes() < len + kCheapPrepend) {
    buffer_.resize(writer_index_ + len);
    AssertInvariant();
    return;
  }

  // Reclaim consumed space by moving the readable region toward the front
  // while preserving the reserved prepend area.
  const std::size_t readable = readable_bytes();
  std::memmove(Begin() + kCheapPrepend, Peek(), readable);
  reader_index_ = kCheapPrepend;
  writer_index_ = reader_index_ + readable;
  AssertInvariant();
}

}  // namespace vexo::net
