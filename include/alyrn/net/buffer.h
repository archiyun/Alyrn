// SPDX-License-Identifier: MIT
#pragma once

#include <sys/uio.h>

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include "alyrn/detail/check.h"
#include "alyrn/detail/intrusive_list.h"
#include "alyrn/detail/macros.h"

namespace alyrn::net {

// A move-only byte chain with explicit write reservation. It owns storage but
// has no socket, scheduler, or backend dependency; adapters use its iovec
// views only at the POSIX scatter/gather boundary.
class Buffer {
public:
  static constexpr std::size_t kDefaultBlockSize = 16 * 1024;

  explicit Buffer(std::size_t block_size = kDefaultBlockSize)
      : block_size_(std::max<std::size_t>(block_size, 1)) {}

  ALYRN_DELETE_COPY(Buffer);

  Buffer(Buffer&& other) noexcept { MoveFromObject(std::move(other)); }

  Buffer& operator=(Buffer&& other) noexcept {
    if (this != &other) {
      AssertNoWriteReservation();
      Clear();
      MoveFromObject(std::move(other));
    }
    return *this;
  }

  ~Buffer() { Clear(); }

  [[nodiscard]]
  std::size_t ReadableBytes() const noexcept {
    return readable_bytes_;
  }
  [[nodiscard]]
  bool Empty() const noexcept {
    return readable_bytes_ == 0;
  }

  [[nodiscard]]
  std::span<const std::byte> ContiguousView() const noexcept {
    const Block* block = FirstReadableBlock();
    if (block == nullptr) return {};
    return {block->ReadData(), block->ReadableBytes()};
  }

  [[nodiscard]]
  std::string_view ContiguousText() const noexcept {
    auto view = ContiguousView();
    return {reinterpret_cast<const char*>(view.data()), view.size()};
  }

  [[nodiscard]]
  std::vector<iovec> ReadableIov(std::size_t max_iov = 16) const {
    std::vector<iovec> out;
    out.reserve(max_iov);

    auto& blocks = const_cast<BlockList&>(blocks_);
    for (Block& block : blocks) {
      if (out.size() >= max_iov) break;
      if (block.ReadableBytes() == 0) continue;

      out.push_back(iovec{
          .iov_base = const_cast<std::byte*>(block.ReadData()),
          .iov_len = block.ReadableBytes(),
      });
    }

    return out;
  }

  [[nodiscard]]
  std::vector<iovec> PrepareWrite(std::size_t hint, std::size_t max_iov = 16) {
    ALYRN_CHECK(!write_reserved_, "nested Buffer::PrepareWrite is not allowed");
    if (max_iov == 0) return {};

    if (hint == 0) hint = block_size_;
    reserved_bytes_ = 0;
    write_reserved_ = true;

    try {
      EnsureTailWritable(hint, max_iov);

      std::vector<iovec> out;
      out.reserve(max_iov);
      for (Block& block : blocks_) {
        if (!block.reserved_for_write) continue;
        if (out.size() == max_iov) break;

        const std::size_t bytes = block.WritableBytes();
        if (bytes == 0) continue;

        out.push_back(iovec{
            .iov_base = block.WriteData(),
            .iov_len = bytes,
        });
        reserved_bytes_ += bytes;
      }

      if (out.empty()) {
        ClearWriteReservation();
      }
      return out;
    } catch (...) {
      ClearWriteReservation();
      throw;
    }
  }

  // Fills caller-owned iovec storage for a new write reservation. This avoids
  // allocating an iovec vector when the caller needs the views only for one
  // synchronous syscall, as the Epoll does for readv(). The caller must
  // keep the returned view only until its storage is reused; the Buffer owns
  // the reserved byte ranges until CommitWrite() or AbortWrite().
  [[nodiscard]]
  std::span<iovec> PrepareWrite(std::size_t hint, std::span<iovec> out) {
    ALYRN_CHECK(!write_reserved_, "nested Buffer::PrepareWrite is not allowed");
    if (out.empty()) return {};

    if (hint == 0) hint = block_size_;
    reserved_bytes_ = 0;
    write_reserved_ = true;

    try {
      EnsureTailWritable(hint, out.size());
      auto iovs = FillReservedWriteIov(out);
      for (const iovec& iov : iovs) {
        reserved_bytes_ += iov.iov_len;
      }
      if (iovs.empty()) {
        ClearWriteReservation();
      }
      return iovs;
    } catch (...) {
      ClearWriteReservation();
      throw;
    }
  }

  // Recreates iovec views for the active reservation in caller-owned
  // storage. This is useful after a readiness notification: the reservation
  // remains owned by the Buffer, but a Epoll need not retain an iovec
  // allocation while the coroutine is suspended.
  [[nodiscard]]
  std::span<iovec> ReservedWriteIov(std::span<iovec> out) noexcept {
    ALYRN_CHECK(write_reserved_, "Buffer::ReservedWriteIov without PrepareWrite");
    return FillReservedWriteIov(out);
  }

  // Creates a reservation only when one contiguous writable range can satisfy
  // hint. A caller that must retain iovec storage across an asynchronous
  // backend request can use this fast path and fall back to PrepareWrite()
  // when an existing tail needs a scatter/gather reservation instead.
  [[nodiscard]]
  std::optional<iovec> TryPrepareWriteOne(std::size_t hint) {
    ALYRN_CHECK(!write_reserved_, "nested Buffer::PrepareWrite is not allowed");

    if (hint == 0) hint = block_size_;
    Block* tail = blocks_.Back();
    if (tail != nullptr && tail->WritableBytes() < hint) {
      return std::nullopt;
    }

    reserved_bytes_ = 0;
    write_reserved_ = true;
    try {
      if (tail == nullptr) {
        tail = NewBlock(std::max(block_size_, hint));
        const bool linked = blocks_.PushBack(tail);
        ALYRN_CHECK(linked, "Buffer failed to link a newly allocated block");
      }

      tail->reserved_for_write = true;
      const std::size_t bytes = tail->WritableBytes();
      ALYRN_CHECK(bytes >= hint, "single Buffer write reservation is too small");
      reserved_bytes_ = bytes;
      return iovec{
          .iov_base = tail->WriteData(),
          .iov_len = bytes,
      };
    } catch (...) {
      ClearWriteReservation();
      throw;
    }
  }

  void CommitWrite(std::size_t n) {
    ALYRN_CHECK(write_reserved_, "Buffer::CommitWrite without PrepareWrite");
    ALYRN_CHECK(n <= reserved_bytes_, "Buffer::CommitWrite exceeds reserved bytes");

    std::size_t remaining = n;
    for (Block& block : blocks_) {
      if (!block.reserved_for_write) continue;
      if (remaining == 0) break;

      const std::size_t m = std::min(remaining, block.WritableBytes());
      block.write_pos += m;
      readable_bytes_ += m;
      remaining -= m;
    }

    ALYRN_CHECK(remaining == 0, "Buffer::CommitWrite reservation became inconsistent");
    ClearWriteReservation();
  }

  void AbortWrite() noexcept { ClearWriteReservation(); }

  void Append(std::span<const std::byte> bytes) {
    AssertNoWriteReservation();

    while (!bytes.empty()) {
      EnsureOneTailBlock(bytes.size());

      Block* tail = blocks_.Back();
      const std::size_t n = std::min(bytes.size(), tail->WritableBytes());
      std::memcpy(tail->WriteData(), bytes.data(), n);

      tail->write_pos += n;
      readable_bytes_ += n;
      bytes = bytes.subspan(n);
    }
  }

  void Append(std::string_view text) {
    Append(std::as_bytes(std::span<const char>(text.data(), text.size())));
  }

  void Drain(std::size_t n) noexcept {
    AssertNoWriteReservation();
    DrainCommitted(std::min(n, readable_bytes_));
  }

  void DrainAll() noexcept {
    AssertNoWriteReservation();
    Clear();
  }

private:
  struct BlockTag {};

  struct Block : alyrn::detail::ListNode<Block, BlockTag> {
    explicit Block(std::size_t cap) : data(new std::byte[cap]), capacity(cap) {}

    ALYRN_DELETE_COPY(Block);

    std::unique_ptr<std::byte[]> data;
    std::size_t capacity{0};
    std::size_t read_pos{0};
    std::size_t write_pos{0};
    bool reserved_for_write{false};

    std::size_t ReadableBytes() const noexcept {
      return write_pos - read_pos;
    }
    std::size_t WritableBytes() const noexcept {
      return capacity - write_pos;
    }

    const std::byte* ReadData() const noexcept {
      return data.get() + read_pos;
    }
    std::byte* WriteData() noexcept {
      return data.get() + write_pos;
    }
  };

  using BlockList = alyrn::detail::IntrusiveList<Block, BlockTag>;

  static Block* NewBlock(std::size_t capacity) { return new Block(capacity); }

  const Block* FirstReadableBlock() const noexcept {
    auto& blocks = const_cast<BlockList&>(blocks_);
    for (const Block& block : blocks) {
      if (block.ReadableBytes() > 0) return &block;
    }
    return nullptr;
  }

  void EnsureOneTailBlock(std::size_t hint) {
    Block* tail = blocks_.Back();
    if (tail != nullptr && tail->WritableBytes() > 0) return;
    Block* block = NewBlock(std::max(block_size_, hint));
    const bool linked = blocks_.PushBack(block);
    ALYRN_CHECK(linked, "Buffer failed to link a newly allocated block");
  }

  void EnsureTailWritable(std::size_t hint, std::size_t max_iov) {
    std::size_t writable = 0;
    std::size_t reserved = 0;

    Block* tail = blocks_.Back();
    if (tail != nullptr && tail->WritableBytes() > 0) {
      tail->reserved_for_write = true;
      writable += tail->WritableBytes();
      reserved += 1;
    }

    while (writable < hint && reserved < max_iov) {
      Block* block = NewBlock(std::max(block_size_, hint - writable));
      const bool linked = blocks_.PushBack(block);
      ALYRN_CHECK(linked, "Buffer failed to link a newly allocated block");
      block->reserved_for_write = true;
      writable += block->WritableBytes();
      reserved += 1;
    }
  }

  std::span<iovec> FillReservedWriteIov(std::span<iovec> out) noexcept {
    std::size_t count = 0;
    for (Block& block : blocks_) {
      if (!block.reserved_for_write) continue;
      if (count == out.size()) break;

      const std::size_t n = block.WritableBytes();
      if (n == 0) continue;

      out[count++] = iovec{
          .iov_base = block.WriteData(),
          .iov_len = n,
      };
    }
    return out.first(count);
  }

  void ClearWriteReservation() noexcept {
    if (!write_reserved_) return;

    for (Block& block : blocks_) {
      block.reserved_for_write = false;
    }

    write_reserved_ = false;
    reserved_bytes_ = 0;
  }

  void AssertNoWriteReservation() const noexcept {
    ALYRN_CHECK(!write_reserved_, "Buffer mutation during pending write reservation");
  }

  void DrainCommitted(std::size_t n) noexcept {
    while (n > 0) {
      Block* front = blocks_.Front();
      if (front == nullptr) break;

      const std::size_t m = std::min(n, front->ReadableBytes());
      front->read_pos += m;
      readable_bytes_ -= m;
      n -= m;

      if (front->ReadableBytes() == 0) {
        blocks_.PopFront();
        delete front;
      }
    }
  }

  void Clear() noexcept {
    ClearWriteReservation();

    while (Block* block = blocks_.PopFront()) {
      delete block;
    }

    readable_bytes_ = 0;
  }

  void MoveFromObject(Buffer&& other) noexcept {
    ALYRN_CHECK(!other.write_reserved_, "moving a Buffer with pending write reservation");

    block_size_ = other.block_size_;
    readable_bytes_ = other.readable_bytes_;
    write_reserved_ = false;
    reserved_bytes_ = 0;

    blocks_.Splice(other.blocks_);

    other.readable_bytes_ = 0;
    other.write_reserved_ = false;
    other.reserved_bytes_ = 0;
  }

  BlockList blocks_;
  std::size_t readable_bytes_{0};
  std::size_t block_size_{kDefaultBlockSize};

  bool write_reserved_{false};
  std::size_t reserved_bytes_{0};
};

}  // namespace alyrn::net
