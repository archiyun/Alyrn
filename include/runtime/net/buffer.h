#pragma once

#include "runtime/base/noncopyable.h"

#include <cstddef>
#include <string>
#include <vector>

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
class Buffer : public runtime::base::NonCopyable {
public:
  // Reserved bytes at the front of the buffer for prepend operations, such as
  // adding a length field before an already encoded payload.
  static constexpr std::size_t kCheapPrepend = 8;

  // Default writable capacity, excluding the prepend region.
  static constexpr std::size_t kInitialSize = 1024;

  explicit Buffer(std::size_t initial_size = kInitialSize);

  // Returns the number of bytes currently available for reading.
  std::size_t ReadableBytes() const;

  // Returns the number of bytes currently available for appending.
  std::size_t WritableBytes() const;

  // Returns the number of bytes before the readable region.
  std::size_t PrependableBytes() const;

  // Returns a pointer to the beginning of the readable region.
  const char* Peek() const;

  // Consumes len bytes from the readable region.
  void Retrieve(std::size_t len);

  // Consumes bytes up to end, where end must point into the readable region.
  void RetrieveUntil(const char* end);

  // Resets the buffer to an empty state.
  void RetrieveAll();

  // Returns and consumes len bytes as a string.
  std::string RetrieveAsString(std::size_t len);

  // Returns and consumes the entire readable region as a string.
  std::string RetrieveAllAsString();

  // Appends raw bytes to the writable region.
  void Append(const char* data, std::size_t len);

  // Appends a string to the writable region.
  void Append(const std::string& str);

  // Returns a pointer to the beginning of the writable region.
  char* BeginWrite();

  // Returns a pointer to the beginning of the writable region.
  const char* BeginWrite() const;

  // Advances the writer index after len bytes have been written into the
  // writable region by external code.
  void HasWritten(std::size_t len);

  // Ensures that at least len writable bytes are available.
  void EnsureWritableBytes(std::size_t len);

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

private:
  char* Begin();
  const char* Begin() const;

  // Makes room for len additional writable bytes, either by resizing the
  // underlying storage or by moving readable bytes toward the front.
  void MakeSpace(std::size_t len);

private:
  std::vector<char> buffer_;
  std::size_t reader_index_;
  std::size_t writer_index_;
};

}  // namespace runtime::net
