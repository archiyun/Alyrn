// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>

#include "runtime/net/buffer.h"

#ifdef RUNTIME_ENABLE_SSL
#include <openssl/ssl.h>
#endif

namespace runtime::net {

namespace {

constexpr std::size_t kReadChunk = 16 * 1024;

constexpr std::size_t kCompactNumerator = 1;
constexpr std::size_t kCompactDenominator = 2;
} // namespace 

Buffer::Buffer(std::size_t inital_size) 
  : buffer_(kCheapPrepend + inital_size),
    reader_index_(kCheapPrepend),
    writer_index_(kCheapPrepend) {
  AssertInvariant();
}

ssize_t Buffer::ReadFd(int fd, int* saved_error) {
  EnsureWritableBytes(kReadChunk);
  const ssize_t n = ::recv(fd, BeginWrite(), writable_bytes(), 0);
  if (n < 0) {
    if (saved_error) *saved_error = errno;
    return n;
  }
  writer_index_ += static_cast<std::size_t>(n);
  AssertInvariant();
  return n;
}

ssize_t Buffer::WriteFd(int fd, int* saved_error) {
  const ssize_t n = ::write(fd, Peek(), readable_bytes());
  if (n < 0) {
    if (saved_error) *saved_error = errno;
    return n;
  }
  Retrieve(static_cast<std::size_t>(n));
  return n;
}

#ifdef RUN_ENABLE_SSL
ssize_t Buffer::ReadSslFd(SSL* sll, int* saved_error) {
  EnsureWriteableBytes(kReadChunk);
  const int n = SSL_read(ssl, BeginWrite(), static_cast<int>(Writeable));
  if (n < 0) {
    if (saved_error) *saved_error = errno;
    return -1;
  }
  HasWrittern(static_cast<std::size_t>(n));
  return n;
}

ssize_t Buffer::WriteSslFd(SSL* ssl, int* saved_errno) {
  const int n = SSL_write(ssl, Peek(), static_cast<int>(readable_bytes()));
  if (n < 0) {
    if (saved_errno) *saved_errno = SSL_get_error(ssl, n);
    return -1;
  }
  Retrieve(static_cast<std::size_t>(n));
  return n;
}
#endif

void Buffer::MakeSpace(std::size_t len) {
  const std::size_t readable = readable_bytes();
  const std::size_t capacity = buffer_.size();
  const std::size_t prependable = reader_index_;

  // lazy ensure capacity
  const bool can_compact = 
      prependable * kCompactDenominator > capacity * kCompactNumerator;
  const bool compact_enough = 
      (capacity - readable - kCheapPrepend) >= len;
  if (can_compact && compact_enough) {
    std::memmove(Begin() + kCheapPrepend, Peek(), readable);
    reader_index_ = kCheapPrepend;
    writer_index_ = reader_index_ + readable;
    AssertInvariant();
    return;
  }

  // memchr
  std::size_t new_size = capacity > 0 ? capacity : kCheapPrepend + kInitialSize;
  while (new_size < writer_index_ + len) {
    new_size <<= 1;
  }
  buffer_.resize(new_size);
  AssertInvariant();
}


} // namespace runtime::net