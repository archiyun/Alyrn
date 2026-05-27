// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "runtime/net/buffer.h"

#include <cstring>
#include <sys/uio.h>
#include <unistd.h>

#ifdef RUNTIME_ENABLE_SSL
#include <openssl/ssl.h>
#endif

namespace runtime::net {

Buffer::Buffer(std::size_t initial_size)
  : buffer_(kCheapPrepend + initial_size),
    reader_index_(kCheapPrepend),
    writer_index_(kCheapPrepend) {
  RUNTIME_ASSERT(reader_index_ == writer_index_, "buffer must start empty");
  AssertInvariant();
}

ssize_t Buffer::ReadFd(int fd, int* saved_errno) {
  char extrabuf[65536];
  iovec vec[2];

  const std::size_t writable = WritableBytes();
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

#ifdef RUNTIME_ENABLE_SSL
ssize_t Buffer::ReadSslFd(SSL* ssl, int* saved_errno) {
  // Ensure room for at least one max-size TLS record (16 KB) plus framing.
  EnsureWritableBytes(65536);
  const int n = SSL_read(ssl, BeginWrite(), static_cast<int>(WritableBytes()));
  if (n < 0) {
    if (saved_errno != nullptr)
      *saved_errno = SSL_get_error(ssl, n);
    return -1;
  }
  HasWritten(static_cast<std::size_t>(n));
  return n;
}

ssize_t Buffer::WriteSslFd(SSL* ssl, int* saved_errno) {
  const int n = SSL_write(ssl, Peek(), static_cast<int>(ReadableBytes()));
  if (n < 0) {
    if (saved_errno != nullptr)
      *saved_errno = SSL_get_error(ssl, n);
    return -1;
  }
  Retrieve(static_cast<std::size_t>(n));
  return n;
}
#endif

ssize_t Buffer::WriteFd(int fd, int* saved_errno) {
  const ssize_t n = ::write(fd, Peek(), ReadableBytes());
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
  if (WritableBytes() + PrependableBytes() < len + kCheapPrepend) {
    buffer_.resize(writer_index_ + len);
    AssertInvariant();
    return;
  }

  // Reclaim consumed space by moving the readable region toward the front
  // while preserving the reserved prepend area.
  const std::size_t readable = ReadableBytes();
  std::memmove(Begin() + kCheapPrepend, Peek(), readable);
  reader_index_ = kCheapPrepend;
  writer_index_ = reader_index_ + readable;
  AssertInvariant();
}

}  // namespace runtime::net
