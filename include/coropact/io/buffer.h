// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <sys/uio.h>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstring>
#include <memory>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include "coropact/ds/intrusive_list.h"
#include "coropact/utils/macros.h"

namespace coropact::io {

class Buffer {
public:
  static constexpr std::size_t kDefaultBlockSize = 16 * 1024;

  explicit Buffer(std::size_t block_size = kDefaultBlockSize)
      : block_size_(std::max<std::size_t>(block_size, 1)) {}

  COROPACT_DELETE_COPY(Buffer);

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

  [[nodiscard]] std::size_t ReadableBytes() const noexcept { return readable_bytes_; }
  [[nodiscard]] bool Empty() const noexcept { return readable_bytes_ == 0; }

  [[nodiscard]] std::span<const std::byte> ContiguousView() const noexcept {
    const Block* block = FirstReadableBlock();
    if (block == nullptr) return {};
    return {block->ReadData(), block->ReadableBytes()};
  }

  [[nodiscard]] std::string_view ContiguousText() const noexcept {
    auto view = ContiguousView();
    return {reinterpret_cast<const char*>(view.data()), view.size()};
  }

  [[nodiscard]] std::vector<iovec> ReadableIov(std::size_t max_iov = 16) const {
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

  [[nodiscard]] std::vector<iovec> PrepareWrite(std::size_t hint, std::size_t max_iov = 16) {
    assert(!write_reserved_ && "nested Buffer::PrepareWrite is not allowed");
    if (max_iov == 0) return {};

    if (hint == 0) hint = block_size_;
    EnsureTailWritable(hint, max_iov);

    std::vector<iovec> out;
    out.reserve(max_iov);

    reserved_bytes_ = 0;
    reserved_block_count_ = 0;
    write_reserved_ = true;

    for (Block& block : blocks_) {
      if (!block.reserved_for_write) continue;
      if (out.size() >= max_iov) break;

      const std::size_t n = block.WritableBytes();
      if (n == 0) continue;

      out.push_back(iovec{
          .iov_base = block.WriteData(),
          .iov_len = n,
      });
      reserved_bytes_ += n;
      ++reserved_block_count_;
    }

    return out;
  }

  void CommitWrite(std::size_t n) {
    assert(write_reserved_ && "Buffer::CommitWrite without PrepareWrite");
    assert(n <= reserved_bytes_ && "Buffer::CommitWrite exceeds reserved bytes");

    std::size_t remaining = n;
    for (Block& block : blocks_) {
      if (!block.reserved_for_write) continue;
      if (remaining == 0) break;

      const std::size_t m = std::min(remaining, block.WritableBytes());
      block.write_pos += m;
      readable_bytes_ += m;
      remaining -= m;
    }

    assert(remaining == 0);
    ClearWriteReservation();
  }

  void AbortWrite() noexcept { ClearWriteReservation(); }

  void Append(std::span<const std::byte> bytes) {
    AssertNoWriteReservation();

    while (!bytes.empty()) {
      EnsureOneTailBlock(bytes.size());

      Block* tail = blocks_.back();
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

  struct Block : coropact::ds::ListNode<Block, BlockTag> {
    explicit Block(std::size_t cap) : data(new std::byte[cap]), capacity(cap) {}

    COROPACT_DELETE_COPY(Block);

    std::unique_ptr<std::byte[]> data;
    std::size_t capacity{0};
    std::size_t read_pos{0};
    std::size_t write_pos{0};
    bool reserved_for_write{false};

    [[nodiscard]] std::size_t ReadableBytes() const noexcept { return write_pos - read_pos; }
    [[nodiscard]] std::size_t WritableBytes() const noexcept { return capacity - write_pos; }

    [[nodiscard]] const std::byte* ReadData() const noexcept { return data.get() + read_pos; }
    [[nodiscard]] std::byte* WriteData() noexcept { return data.get() + write_pos; }
  };

  using BlockList = coropact::ds::IntrusiveList<Block, BlockTag>;

  static Block* NewBlock(std::size_t capacity) { return new Block(capacity); }

  [[nodiscard]] const Block* FirstReadableBlock() const noexcept {
    auto& blocks = const_cast<BlockList&>(blocks_);
    for (const Block& block : blocks) {
      if (block.ReadableBytes() > 0) return &block;
    }
    return nullptr;
  }

  void EnsureOneTailBlock(std::size_t hint) {
    Block* tail = blocks_.back();
    if (tail != nullptr && tail->WritableBytes() > 0) return;
    blocks_.PushBack(NewBlock(std::max(block_size_, hint)));
  }

  void EnsureTailWritable(std::size_t hint, std::size_t max_iov) {
    std::size_t writable = 0;
    std::size_t reserved = 0;

    Block* tail = blocks_.back();
    if (tail != nullptr && tail->WritableBytes() > 0) {
      tail->reserved_for_write = true;
      writable += tail->WritableBytes();
      reserved += 1;
    }

    while (writable < hint && reserved < max_iov) {
      Block* block = NewBlock(std::max(block_size_, hint - writable));
      block->reserved_for_write = true;
      writable += block->WritableBytes();
      reserved += 1;
      blocks_.PushBack(block);
    }
  }

  void ClearWriteReservation() noexcept {
    if (!write_reserved_) return;

    for (Block& block : blocks_) {
      block.reserved_for_write = false;
    }

    write_reserved_ = false;
    reserved_bytes_ = 0;
    reserved_block_count_ = 0;
  }

  void AssertNoWriteReservation() const noexcept {
    assert(!write_reserved_ && "Buffer mutation during pending write reservation");
  }

  void DrainCommitted(std::size_t n) noexcept {
    while (n > 0) {
      Block* front = blocks_.front();
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
    assert(!other.write_reserved_ && "moving a Buffer with pending write reservation");

    block_size_ = other.block_size_;
    readable_bytes_ = other.readable_bytes_;
    write_reserved_ = false;
    reserved_bytes_ = 0;
    reserved_block_count_ = 0;

    blocks_.Splice(other.blocks_);

    other.readable_bytes_ = 0;
    other.write_reserved_ = false;
    other.reserved_bytes_ = 0;
    other.reserved_block_count_ = 0;
  }

  BlockList blocks_;
  std::size_t readable_bytes_{0};
  std::size_t block_size_{kDefaultBlockSize};

  bool write_reserved_{false};
  std::size_t reserved_bytes_{0};
  std::size_t reserved_block_count_{0};
};

}  // namespace coropact::io
