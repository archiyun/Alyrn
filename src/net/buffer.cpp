#include "runtime/net/buffer.h"

#include <algorithm>
#include <cstring>
#include <sys/uio.h>
#include <unistd.h>

namespace runtime::net {

Buffer::Buffer(std::size_t initial_size)
    : buffer_(kCheapPrepend + initial_size),
      reader_index_(kCheapPrepend),
      writer_index_(kCheapPrepend) {}

std::size_t Buffer::ReadableBytes() const {
  return writer_index_ - reader_index_;
}

std::size_t Buffer::WritableBytes() const {
  return buffer_.size() - writer_index_;
}

std::size_t Buffer::PrependableBytes() const {
  return reader_index_;
}

const char* Buffer::Peek() const {
  return Begin() + reader_index_;
}

void Buffer::Retrieve(std::size_t len) {
  if (len < ReadableBytes()) {
    reader_index_ += len;
    return;
  }
  RetrieveAll();
}

void Buffer::RetrieveUntil(const char* end) {
  Retrieve(static_cast<std::size_t>(end - Peek()));
}

void Buffer::RetrieveAll() {
  reader_index_ = kCheapPrepend;
  writer_index_ = kCheapPrepend;
}

std::string Buffer::RetrieveAsString(std::size_t len) {
  len = std::min(len, ReadableBytes());
  std::string result(Peek(), len);
  Retrieve(len);
  return result;
}

std::string Buffer::RetrieveAllAsString() {
  return RetrieveAsString(ReadableBytes());
}

void Buffer::Append(const char* data, std::size_t len) {
  EnsureWritableBytes(len);
  std::memcpy(BeginWrite(), data, len);
  HasWritten(len);
}

void Buffer::Append(const std::string& str) {
  Append(str.data(), str.size());
}

char* Buffer::BeginWrite() {
  return Begin() + writer_index_;
}

const char* Buffer::BeginWrite() const {
  return Begin() + writer_index_;
}

void Buffer::HasWritten(std::size_t len) {
  writer_index_ += len;
}

void Buffer::EnsureWritableBytes(std::size_t len) {
  if (WritableBytes() >= len) {
    return;
  }
  MakeSpace(len);
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

char* Buffer::Begin() {
  return buffer_.data();
}

const char* Buffer::Begin() const {
  return buffer_.data();
}

void Buffer::MakeSpace(std::size_t len) {
  if (WritableBytes() + PrependableBytes() < len + kCheapPrepend) {
    buffer_.resize(writer_index_ + len);
    return;
  }

  // Reclaim consumed space by moving the readable region toward the front
  // while preserving the reserved prepend area.
  const std::size_t readable = ReadableBytes();
  std::memmove(Begin() + kCheapPrepend, Peek(), readable);
  reader_index_ = kCheapPrepend;
  writer_index_ = reader_index_ + readable;
}

}  // namespace runtime::net
