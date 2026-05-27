// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
// Reference: https://github.com/chenshuo/muduo/blob/master/muduo/net/Buffer.h
#pragma once

#include "runtime/base/noncopyable.h"
#include "runtime/net/net_assert.h"

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <string>
#include <vector>

#ifdef RUNTIME_ENABLE_SSL
#include <openssl/ssl.h>
#endif

namespace runtime::net {

// Buffer is a user-space byte buffer used by the networking layer.
//
// The internal storage is divided into three regions:
//
//   [prependable | readable | writable]
//
//   0            reader      writer      size
//   |------------|===========|-----------|
//                ^           ^
//                |           |
//          reader_index_   writer_index_
//
// - prependable: bytes before the readable region, including reserved prepend
//   space and bytes that have already been consumed
// - readable: bytes currently available to the caller
// - writable: free space available for appending more data
//
// This layout allows the buffer to append efficiently while still reusing
// previously consumed space when possible.

// const value CRLF and CRLFCRLF
static constexpr std::string_view kCRLF = "\r\n";
static constexpr std::string_view kCRLFCRLF = "\r\n\r\n";

class Buffer : public runtime::base::NonCopyable {
public:
  // Reserved bytes at the front of the buffer for prepend operations, such as
  // adding a length field before an already encoded payload.
  static constexpr std::size_t kCheapPrepend = 8;

  // Default writable capacity, excluding the prepend region.
  static constexpr std::size_t kInitialSize = 1024;

  explicit Buffer(std::size_t initial_size = kInitialSize);

  // Returns the number of bytes currently available for reading.
  std::size_t ReadableBytes() const {
    return writer_index_ - reader_index_;
  }

  // Returns the number of bytes currently available for appending.
  std::size_t WritableBytes() const {
    return buffer_.size() - writer_index_;
  }

  // Returns the number of bytes before the readable region.
  std::size_t PrependableBytes() const {
    return reader_index_;
  }

  // Returns a pointer to the beginning of the readable region.
  const char* Peek() const {
    return Begin() + reader_index_;
  }

  // Consumes len bytes from the readable region.
  void Retrieve(std::size_t len) {
  RUNTIME_ASSERT(len <= ReadableBytes(), "Retrieve: len exceeds readable bytes");
  if (len < ReadableBytes()) {
    reader_index_ += len;
    AssertInvariant();
    return;
  }
  RetrieveAll();
  }

  // Consumes bytes up to end, where end must point into the readable region.
  void RetrieveUntil(const char* end) {
  RUNTIME_ASSERT(end >= Peek(), "RetrieveUntil: end pointer is before Peek()");
  RUNTIME_ASSERT(end <= Peek() + ReadableBytes(),
                 "RetrieveUntil: end pointer is past the readable region");
  Retrieve(static_cast<std::size_t>(end - Peek()));
  }

  // Resets the buffer to an empty state.
  void RetrieveAll() {
    reader_index_ = kCheapPrepend;
    writer_index_ = kCheapPrepend;
  }

  // Returns and consumes len bytes as a string.
  std::string RetrieveAsString(std::size_t len) {
    len = std::min(len, ReadableBytes());
    std::string res(Peek(), len);
    Retrieve(len);
    return res;
  }

  // Returns and consumes the entire readable region as a string.
  std::string RetrieveAllAsString() {
    return RetrieveAsString(ReadableBytes());
  }

  // Appends raw bytes to the writable region.
  void Append(const char* data, std::size_t len) {
    EnsureWritableBytes(len);
    std::memcpy(BeginWrite(), data, len);
    HasWritten(len);
  }

  // Appends a string to the writable region.
  void Append(const std::string& str) {
    Append(str.data(), str.size());
  }

  // Returns a pointer to the beginning of the writable region.
  char* BeginWrite() {
    return Begin() + writer_index_;
  }

  // Returns a pointer to the beginning of the writable region.
  const char* BeginWrite() const {
    return Begin() + writer_index_;
  }

  // Advances the writer index after len bytes have been written into the
  // writable region by external code.
  void HasWritten(std::size_t len) {
    RUNTIME_ASSERT(len <= WritableBytes(), "HasWritten: len exceeds writable bytes");
    writer_index_ += len;
    AssertInvariant();
  }


  // Ensures that at least len writable bytes are available.
  void EnsureWritableBytes(std::size_t len) {
    if (WritableBytes() >= len) return;
    MakeSpace(len);
  }

  // Reads from fd into the buffer.
  //
  // Returns the number of bytes read, or -1 on error. If saved_errno is not
  // null, it is updated with errno when a read error occurs.
  ssize_t ReadFd(int fd, int* saved_errno);

  // Writes the readable region to fd.
  //
  // Returns the number of bytes written, or -1 on error. Successfully written
  // bytes are consumed from the buffer.
  ssize_t WriteFd(int fd, int* saved_errno);

#ifdef RUNTIME_ENABLE_SSL
  // SSL variants: read/write through an established TLS session.
  // saved_errno receives SSL_get_error() on failure, not errno.
  ssize_t ReadSslFd(SSL* ssl, int* saved_errno);
  ssize_t WriteSslFd(SSL* ssl, int* saved_errno);
#endif

  // Find "\r\n" and "\r\n\r\n" in the readable region for HTTP parsing.
  // Uses glibc memmem (Two-Way algorithm) to guarantee linear time and avoid
  // the constant-factor blow-up of memchr-then-restart on adversarial inputs
  // with a high density of stray '\r' bytes.
  // Requires _GNU_SOURCE, which is defined PUBLIC on the runtime_net target.
  const char* FindCRLF() const {
    const std::size_t n = ReadableBytes();
    if (n < 2) return nullptr;
    return static_cast<const char*>(::memmem(Peek(), n, "\r\n", 2));
  }

  const char* FindCRLFCRLF() const {
    const std::size_t n = ReadableBytes();
    if (n < 4) return nullptr;
    return static_cast<const char*>(::memmem(Peek(), n, "\r\n\r\n", 4));
  }
private:
  char* Begin()  { return buffer_.data(); }
  const char* Begin() const { return buffer_.data(); }

  // Makes room for len additional writable bytes, either by resizing the
  // underlying storage or by moving readable bytes toward the front.
  void MakeSpace(std::size_t len);

  // Checks the three-region invariant. Active only in Debug builds.
  void AssertInvariant() const {
    RUNTIME_ASSERT(reader_index_ >= kCheapPrepend,
                   "reader_index_ undershot kCheapPrepend");
    RUNTIME_ASSERT(reader_index_ <= writer_index_,
                   "reader_index_ overtook writer_index_");
    RUNTIME_ASSERT(writer_index_ <= buffer_.size(),
                   "writer_index_ past end of buffer");
  }

private:
  std::vector<char> buffer_;
  std::size_t reader_index_;
  std::size_t writer_index_;
};

}  // namespace runtime::net
