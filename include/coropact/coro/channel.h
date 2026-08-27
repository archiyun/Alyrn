// SPDX-License-Identifier: MIT
#pragma once

#include <cerrno>
#include <coroutine>
#include <cstddef>
#include <expected>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

#include "coropact/base/check.h"
#include "coropact/coro/scheduler.h"
#include "coropact/coro/work.h"
#include "coropact/ds/intrusive_queue.h"
#include "coropact/result.h"
#include "coropact/utils/macros.h"

namespace coropact::coro {

/*
 * A scheduler-affine FIFO channel. Its buffer and waiter queues are mutated
 * only by the owning scheduler thread. Cross-thread transport remains a
 * backend mailbox concern: Scheduler::Schedule() is not a portable posting
 * interface.
 */
template <class T>
class Channel final {
  static_assert(std::is_nothrow_move_constructible_v<T>);
  static_assert(std::is_nothrow_destructible_v<T>);

  struct SendTag;
  struct ReceiveTag;

public:
  COROPACT_DELETE_COPY_MOVE(Channel);

  explicit Channel(Scheduler& scheduler, std::size_t capacity)
      : scheduler_(&scheduler), buffer_(capacity), capacity_(capacity) {}

  ~Channel() {
    // Scheduling a waiter here would leave its awaiter holding this destroyed
    // channel. The owner must Close() and drain its scheduler before releasing
    // the channel, so outstanding waiters are a lifetime-contract violation.
    COROPACT_CHECK(senders_.Empty() && receivers_.Empty(),
                   "Channel destroyed with pending send or receive waiter");
  }

  class SendAwaiter;
  class ReceiveAwaiter;

  [[nodiscard]]
  Result<void> TrySend(T&& value) noexcept {
    CheckOwner();
    if (closed_) return std::unexpected(Errno(EPIPE));

    if (ReceiveAwaiter* receiver = receivers_.PopFront()) {
      receiver->CompleteAndSchedule(Result<std::optional<T>>{
          std::in_place, std::optional<T>{std::move(value)}});
      return {};
    }
    if (size_ == capacity_) return std::unexpected(Errno(EAGAIN));

    PushBuffer(std::move(value));
    return {};
  }

  [[nodiscard]]
  Result<std::optional<T>> TryReceive() noexcept {
    CheckOwner();

    if (size_ != 0) {
      T value = PopBuffer();
      if (SendAwaiter* sender = senders_.PopFront()) {
        PushBuffer(sender->TakeValue());
        sender->CompleteAndSchedule(Result<void>{});
      }
      return std::optional<T>{std::move(value)};
    }
    if (SendAwaiter* sender = senders_.PopFront()) {
      T value = sender->TakeValue();
      sender->CompleteAndSchedule(Result<void>{});
      return std::optional<T>{std::move(value)};
    }
    if (closed_) return std::optional<T>{};
    return std::unexpected(Errno(EAGAIN));
  }

  [[nodiscard]]
  SendAwaiter Send(T value) noexcept {
    return SendAwaiter{*this, std::move(value)};
  }

  [[nodiscard]]
  ReceiveAwaiter Receive() noexcept { return ReceiveAwaiter{*this}; }

  void Close() noexcept {
    CheckOwner();
    if (closed_) return;
    closed_ = true;

    while (SendAwaiter* sender = senders_.PopFront()) {
      sender->CompleteAndSchedule(std::unexpected(Errno(EPIPE)));
    }
    if (size_ != 0) return;
    while (ReceiveAwaiter* receiver = receivers_.PopFront()) {
      receiver->CompleteAndSchedule(Result<std::optional<T>>{
          std::in_place, std::optional<T>{}});
    }
  }

  [[nodiscard]]
  std::size_t Capacity() const noexcept { return capacity_; }

  [[nodiscard]]
  std::size_t Size() const noexcept { return size_; }

  [[nodiscard]]
  bool Closed() const noexcept { return closed_; }

  class SendAwaiter : public ds::QueueNode<SendAwaiter, SendTag> {
  public:
    COROPACT_DELETE_COPY_MOVE(SendAwaiter);

    SendAwaiter(Channel& channel, T value) noexcept
        : channel_(&channel), value_(std::move(value)) {}

    ~SendAwaiter() {
      if (waiting_) channel_->CancelSend(*this);
    }

    bool await_ready() const noexcept { return false; }

    bool await_suspend(std::coroutine_handle<> continuation) noexcept {
      channel_->CheckOwner();
      if (channel_->closed_) {
        result_.emplace(std::unexpected(Errno(EPIPE)));
        return false;
      }
      if (ReceiveAwaiter* receiver = channel_->receivers_.PopFront()) {
        receiver->CompleteAndSchedule(Result<std::optional<T>>{
            std::in_place, std::optional<T>{TakeValue()}});
        result_.emplace(Result<void>{});
        return false;
      }
      if (channel_->size_ != channel_->capacity_) {
        channel_->PushBuffer(TakeValue());
        result_.emplace(Result<void>{});
        return false;
      }

      continuation_.SetHandle(continuation);
      waiting_ = true;
      COROPACT_CHECK(channel_->senders_.PushBack(this), "Channel sender queued twice");
      return true;
    }

    Result<void> await_resume() noexcept {
      COROPACT_CHECK(result_.has_value(), "Channel sender resumed without a result");
      return std::move(*result_);
    }

  private:
    friend class Channel;

    T TakeValue() noexcept {
      COROPACT_CHECK(value_.has_value(), "Channel sender value taken twice");
      T value = std::move(*value_);
      value_.reset();
      return value;
    }

    void CompleteAndSchedule(Result<void> result) noexcept {
      COROPACT_CHECK(waiting_, "Channel completed a sender that was not waiting");
      waiting_ = false;
      result_.emplace(std::move(result));
      channel_->scheduler_->Schedule(&continuation_);
    }

    Channel* channel_;
    std::optional<T> value_;
    std::optional<Result<void>> result_;
    ResumeWork continuation_;
    bool waiting_{false};
  };

  class ReceiveAwaiter : public ds::QueueNode<ReceiveAwaiter, ReceiveTag> {
  public:
    COROPACT_DELETE_COPY_MOVE(ReceiveAwaiter);

    explicit ReceiveAwaiter(Channel& channel) noexcept : channel_(&channel) {}

    ~ReceiveAwaiter() {
      if (waiting_) channel_->CancelReceive(*this);
    }

    bool await_ready() const noexcept { return false; }

    bool await_suspend(std::coroutine_handle<> continuation) noexcept {
      channel_->CheckOwner();
      if (channel_->size_ != 0) {
        T value = channel_->PopBuffer();
        if (SendAwaiter* sender = channel_->senders_.PopFront()) {
          channel_->PushBuffer(sender->TakeValue());
          sender->CompleteAndSchedule(Result<void>{});
        }
        result_.emplace(std::in_place, std::optional<T>{std::move(value)});
        return false;
      }
      if (SendAwaiter* sender = channel_->senders_.PopFront()) {
        T value = sender->TakeValue();
        sender->CompleteAndSchedule(Result<void>{});
        result_.emplace(std::in_place, std::optional<T>{std::move(value)});
        return false;
      }
      if (channel_->closed_) {
        result_.emplace(std::in_place, std::optional<T>{});
        return false;
      }

      continuation_.SetHandle(continuation);
      waiting_ = true;
      COROPACT_CHECK(channel_->receivers_.PushBack(this), "Channel receiver queued twice");
      return true;
    }

    Result<std::optional<T>> await_resume() noexcept {
      COROPACT_CHECK(result_.has_value(), "Channel receiver resumed without a result");
      return std::move(*result_);
    }

  private:
    friend class Channel;

    void CompleteAndSchedule(Result<std::optional<T>> result) noexcept {
      COROPACT_CHECK(waiting_, "Channel completed a receiver that was not waiting");
      waiting_ = false;
      result_.emplace(std::move(result));
      channel_->scheduler_->Schedule(&continuation_);
    }

    Channel* channel_;
    std::optional<Result<std::optional<T>>> result_;
    ResumeWork continuation_;
    bool waiting_{false};
  };

private:
  void CheckOwner() const noexcept {
    COROPACT_CHECK(Scheduler::TryCurrent() == scheduler_,
                   "Channel operation called outside its owning scheduler");
  }

  void PushBuffer(T value) noexcept {
    COROPACT_CHECK(size_ != capacity_, "Channel buffer overflow");
    buffer_[tail_].emplace(std::move(value));
    tail_ = (tail_ + 1) % capacity_;
    ++size_;
  }

  T PopBuffer() noexcept {
    COROPACT_CHECK(size_ != 0, "Channel buffer underflow");
    std::optional<T>& slot = buffer_[head_];
    COROPACT_CHECK(slot.has_value(), "Channel buffer slot is empty");
    T value = std::move(*slot);
    slot.reset();
    head_ = (head_ + 1) % capacity_;
    --size_;
    return value;
  }

  void CancelSend(SendAwaiter& sender) noexcept {
    CheckOwner();
    bool removed = false;
    senders_.ForEachSafe([&](SendAwaiter& current) noexcept {
      if (&current != &sender) return false;
      removed = true;
      return true;
    });
    COROPACT_CHECK(removed, "Channel sender was not queued");
    sender.waiting_ = false;
  }

  void CancelReceive(ReceiveAwaiter& receiver) noexcept {
    CheckOwner();
    bool removed = false;
    receivers_.ForEachSafe([&](ReceiveAwaiter& current) noexcept {
      if (&current != &receiver) return false;
      removed = true;
      return true;
    });
    COROPACT_CHECK(removed, "Channel receiver was not queued");
    receiver.waiting_ = false;
  }

  Scheduler* scheduler_;
  ds::IntrusiveQueue<SendAwaiter, SendTag> senders_;
  ds::IntrusiveQueue<ReceiveAwaiter, ReceiveTag> receivers_;
  std::vector<std::optional<T>> buffer_;
  std::size_t capacity_;
  std::size_t head_{0};
  std::size_t tail_{0};
  std::size_t size_{0};
  bool closed_{false};
};

}  // namespace coropact::coro
