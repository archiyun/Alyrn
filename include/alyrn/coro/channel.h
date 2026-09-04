// SPDX-License-Identifier: MIT
#pragma once

#include <bit>
#include <coroutine>
#include <cstddef>
#include <expected>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

#include "alyrn/coro/scheduler.h"
#include "alyrn/coro/select_case.h"
#include "alyrn/coro/work.h"
#include "alyrn/detail/check.h"
#include "alyrn/detail/intrusive_queue.h"
#include "alyrn/detail/macros.h"
#include "alyrn/result.h"

namespace alyrn::coro {

namespace detail {

template <class T>
struct ChannelSendTag;

template <class T>
class ChannelSendWaiter
    : public ::alyrn::detail::QueueNode<ChannelSendWaiter<T>, ChannelSendTag<T>> {
public:
  virtual ~ChannelSendWaiter() = default;
  virtual T TakeValue() noexcept = 0;
  virtual void Complete() noexcept = 0;
  virtual void NotifyClosed() noexcept = 0;
  virtual void Cancelled() noexcept = 0;
};

template <class T>
struct ChannelReceiveTag;

template <class T>
class ChannelReceiveWaiter
    : public ::alyrn::detail::QueueNode<ChannelReceiveWaiter<T>, ChannelReceiveTag<T>> {
public:
  virtual ~ChannelReceiveWaiter() = default;
  virtual void CompleteValue(T value) noexcept = 0;
  virtual void CompleteClosed() noexcept = 0;
  virtual void Cancelled() noexcept = 0;
};

template <class T>
class SelectSendRegistration;

template <class T>
class SelectReceiveRegistration;

}  // namespace detail

/*
 * A scheduler-affine FIFO channel. Its buffer and waiter queues are mutated
 * only by the owning scheduler thread. Cross-thread transport remains a
 * backend mailbox concern: Scheduler::Schedule() is not a portable posting
 * interface.
 *
 * Blocking operations use Go-like channel notation:
 *
 *   co_await (channel << value);
 *   std::optional<T> received;
 *   co_await (channel >> received);
 *
 * A successful receive stores a value in `received`; a successful receive from
 * a closed and drained channel clears it. `Result<void>` still reports
 * operation errors separately from the closed-channel state.
 *
 * `capacity` must be zero or a power of two. Zero selects an unbuffered
 * channel.
 */
template <class T>
class Channel final {
  static_assert(std::is_nothrow_move_constructible_v<T>);
  static_assert(std::is_nothrow_destructible_v<T>);

public:
  ALYRN_DELETE_COPY_MOVE(Channel);

  friend class detail::SelectSendRegistration<T>;
  friend class detail::SelectReceiveRegistration<T>;

  explicit Channel(Scheduler& scheduler, std::size_t capacity)
      : scheduler_(&scheduler), buffer_(ValidateCapacity(capacity)), capacity_(capacity) {}

  ~Channel() {
    // Channel has an explicit lifetime contract: the owner must Close() it
    // before destruction. Forgetting to close the channel is a programming
    // error and therefore triggers a panic rather than silently abandoning
    // channel state.
    ALYRN_CHECK(closed_, "Channel destroyed without Close()");
    // Scheduling a waiter here would leave its awaiter holding this destroyed
    // channel. The owner must Close() and drain its scheduler before releasing
    // the channel, so outstanding waiters are a lifetime-contract violation.
    ALYRN_CHECK(senders_.Empty() && receivers_.Empty(),
                "Channel destroyed with pending send or receive waiter");
  }

  class SendAwaiter;
  class ReceiveAwaiter;

  // Blocking send. The parentheses are required when this expression is used
  // as the operand of co_await because shift operators have lower precedence.
  SelectSendCase<T> operator<<(T value) noexcept {
    return SelectSendCase<T>{this, std::move(value)};
  }

  // Blocking receive. The output optional is reset on a successful receive;
  // it remains unchanged when the operation returns an error.
  SelectReceiveCase<T> operator>>(std::optional<T>& output) noexcept {
    return SelectReceiveCase<T>{this, &output};
  }

  // Close the channel exactly once. Pending senders fail immediately;
  // buffered values remain receivable, followed by the end-of-stream after drain.
  void Close() noexcept {
    CheckOwner();
    ALYRN_CHECK(!closed_, "close of closed Channel");

    closed_ = true;

    while (auto* sender = senders_.PopFront()) {
      sender->NotifyClosed();
    }
    if (size_ != 0) {
      return;
    }
    while (auto* receiver = receivers_.PopFront()) {
      receiver->CompleteClosed();
    }
  }

  [[nodiscard]]
  std::size_t Capacity() const noexcept {
    CheckOwner();
    return capacity_;
  }

  [[nodiscard]]
  std::size_t Size() const noexcept {
    CheckOwner();
    return size_;
  }

  [[nodiscard]]
  bool Closed() const noexcept {
    CheckOwner();
    return closed_;
  }

  class [[nodiscard]] SendAwaiter : public detail::ChannelSendWaiter<T> {
  public:
    ALYRN_DELETE_COPY_MOVE(SendAwaiter);

    SendAwaiter(Channel& channel, T value) noexcept
        : channel_(&channel), value_(std::move(value)) {}

    ~SendAwaiter() {
      if (waiting_) {
        channel_->CancelSend(*this);
      }
    }

    bool await_ready() const noexcept { return false; }

    bool await_suspend(std::coroutine_handle<> continuation) noexcept {
      channel_->CheckOwner();
      ALYRN_CHECK(!channel_->closed_, "send on closed Channel");
      if (auto* receiver = channel_->receivers_.PopFront()) {
        receiver->CompleteValue(TakeValue());
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
      ALYRN_CHECK(channel_->senders_.PushBack(this), "Channel sender queued twice");
      return true;
    }

    Result<void> await_resume() noexcept {
      ALYRN_CHECK(result_.has_value(), "Channel sender resumed without a result");
      return *result_;
    }

  private:
    friend class Channel;

    T TakeValue() noexcept override {
      ALYRN_CHECK(value_.has_value(), "Channel sender value taken twice");
      T value = std::move(*value_);
      value_.reset();
      return value;
    }

    void NotifyClosed() noexcept override { ALYRN_CHECK(false, "send on closed Channel"); }

    void Complete() noexcept override {
      ALYRN_CHECK(waiting_, "Channel completed a sender that was not waiting");
      waiting_ = false;
      result_.emplace(Result<void>{});
      channel_->scheduler_->Schedule(&continuation_);
    }

    void Cancelled() noexcept override { waiting_ = false; }

    Channel* channel_;
    std::optional<T> value_;
    std::optional<Result<void>> result_;
    ResumeWork continuation_;
    bool waiting_{false};
  };

  class [[nodiscard]] ReceiveAwaiter : public detail::ChannelReceiveWaiter<T> {
  public:
    ALYRN_DELETE_COPY_MOVE(ReceiveAwaiter);

    ReceiveAwaiter(Channel& channel, std::optional<T>& output) noexcept
        : channel_(&channel), output_(&output) {}

    ~ReceiveAwaiter() {
      if (waiting_) {
        channel_->CancelReceive(*this);
      }
    }

    bool await_ready() const noexcept { return false; }

    bool await_suspend(std::coroutine_handle<> continuation) noexcept {
      channel_->CheckOwner();
      if (channel_->size_ != 0) {
        T value = channel_->PopBuffer();
        if (auto* sender = channel_->senders_.PopFront()) {
          channel_->PushBuffer(sender->TakeValue());
          sender->Complete();
        }
        result_.emplace(std::in_place, std::optional<T>{std::move(value)});
        return false;
      }
      if (auto* sender = channel_->senders_.PopFront()) {
        T value = sender->TakeValue();
        sender->Complete();
        result_.emplace(std::in_place, std::optional<T>{std::move(value)});
        return false;
      }
      if (channel_->closed_) {
        result_.emplace(std::in_place, std::optional<T>{});
        return false;
      }

      continuation_.SetHandle(continuation);
      waiting_ = true;
      ALYRN_CHECK(channel_->receivers_.PushBack(this), "Channel receiver queued twice");
      return true;
    }

    Result<void> await_resume() noexcept {
      ALYRN_CHECK(result_.has_value(), "Channel receiver resumed without a result");
      auto result = std::move(*result_);
      if (!result.HasValue()) {
        return std::unexpected(result.Error());
      }

      output_->reset();
      if (result->has_value()) {
        output_->emplace(std::move(**result));
      }
      return {};
    }

  private:
    friend class Channel;

    void CompleteValue(T value) noexcept override {
      ALYRN_CHECK(waiting_, "Channel completed a receiver that was not waiting");
      waiting_ = false;
      result_.emplace(std::in_place, std::optional<T>{std::move(value)});
      channel_->scheduler_->Schedule(&continuation_);
    }

    void CompleteClosed() noexcept override {
      ALYRN_CHECK(waiting_, "Channel completed a receiver that was not waiting");
      waiting_ = false;
      result_.emplace(std::in_place, std::optional<T>{});
      channel_->scheduler_->Schedule(&continuation_);
    }

    void Cancelled() noexcept override { waiting_ = false; }

    Channel* channel_;
    std::optional<T>* output_;
    std::optional<Result<std::optional<T>>> result_;
    ResumeWork continuation_;
    bool waiting_{false};
  };

private:
  using SendQueue =
      ::alyrn::detail::IntrusiveQueue<detail::ChannelSendWaiter<T>, detail::ChannelSendTag<T>>;
  using ReceiveQueue = ::alyrn::detail::IntrusiveQueue<detail::ChannelReceiveWaiter<T>,
                                                       detail::ChannelReceiveTag<T>>;

  static std::size_t ValidateCapacity(std::size_t capacity) noexcept {
    ALYRN_CHECK(capacity == 0 || std::has_single_bit(capacity),
                "Channel capacity must be zero or a power of two");
    return capacity;
  }

  void CheckOwner() const noexcept {
    ALYRN_CHECK(Scheduler::TryCurrent() == scheduler_,
                "Channel operation called outside its owning scheduler");
  }

  void PushBuffer(T value) noexcept {
    ALYRN_CHECK(size_ != capacity_, "Channel buffer overflow");
    buffer_[tail_].emplace(std::move(value));
    tail_ = (tail_ + 1) & (capacity_ - 1);
    ++size_;
  }

  T PopBuffer() noexcept {
    ALYRN_CHECK(size_ != 0, "Channel buffer underflow");
    std::optional<T>& slot = buffer_[head_];
    ALYRN_CHECK(slot.has_value(), "Channel buffer slot is empty");
    T value = std::move(*slot);
    slot.reset();
    head_ = (head_ + 1) & (capacity_ - 1);
    --size_;
    return value;
  }

  bool SelectSendReady() const noexcept {
    CheckOwner();
    return closed_ || !receivers_.Empty() || size_ != capacity_;
  }

  bool SelectReceiveReady() const noexcept {
    CheckOwner();
    return closed_ || !senders_.Empty() || size_ != 0;
  }

  void RegisterSelectSend(detail::ChannelSendWaiter<T>& sender) noexcept {
    CheckOwner();
    ALYRN_CHECK(!closed_, "cannot register a sender on a closed Channel");
    ALYRN_CHECK(senders_.PushBack(&sender), "Channel select sender queued twice");
  }

  void RegisterSelectReceive(detail::ChannelReceiveWaiter<T>& receiver) noexcept {
    CheckOwner();
    ALYRN_CHECK(!closed_, "cannot register a receiver on a closed Channel");
    ALYRN_CHECK(receivers_.PushBack(&receiver), "Channel select receiver queued twice");
  }

  void CommitSelectSend(detail::ChannelSendWaiter<T>& sender) noexcept {
    CheckOwner();
    if (closed_) {
      sender.NotifyClosed();
      return;
    }
    if (auto* receiver = receivers_.PopFront()) {
      receiver->CompleteValue(sender.TakeValue());
      sender.Complete();
      return;
    }

    ALYRN_CHECK(size_ != capacity_, "selected sender was not ready");
    PushBuffer(sender.TakeValue());
    sender.Complete();
  }

  void CommitSelectReceive(detail::ChannelReceiveWaiter<T>& receiver) noexcept {
    CheckOwner();
    if (size_ != 0) {
      T value = PopBuffer();
      if (auto* sender = senders_.PopFront()) {
        PushBuffer(sender->TakeValue());
        sender->Complete();
      }
      receiver.CompleteValue(std::move(value));
      return;
    }
    if (auto* sender = senders_.PopFront()) {
      receiver.CompleteValue(sender->TakeValue());
      sender->Complete();
      return;
    }
    if (closed_) {
      receiver.CompleteClosed();
      return;
    }

    ALYRN_CHECK(false, "selected receiver was not ready");
  }

  void CancelSend(detail::ChannelSendWaiter<T>& sender) noexcept {
    CheckOwner();
    bool removed = false;
    senders_.ForEachSafe([&](auto& current) noexcept {
      if (&current != &sender) {
        return false;
      }
      removed = true;
      return true;
    });
    ALYRN_CHECK(removed, "Channel sender was not queued");
    sender.Cancelled();
  }

  void CancelReceive(detail::ChannelReceiveWaiter<T>& receiver) noexcept {
    CheckOwner();
    bool removed = false;
    receivers_.ForEachSafe([&](auto& current) noexcept {
      if (&current != &receiver) {
        return false;
      }
      removed = true;
      return true;
    });
    ALYRN_CHECK(removed, "Channel receiver was not queued");
    receiver.Cancelled();
  }

  Scheduler* scheduler_;
  SendQueue senders_;
  ReceiveQueue receivers_;
  std::vector<std::optional<T>> buffer_;
  std::size_t capacity_{0};
  std::size_t head_{0};
  std::size_t tail_{0};
  std::size_t size_{0};
  bool closed_{false};
};

template <class T>
typename Channel<T>::SendAwaiter operator co_await(SelectSendCase<T>&& send) noexcept {
  ALYRN_CHECK(send.channel != nullptr, "Select send case has no Channel");
  return typename Channel<T>::SendAwaiter{*send.channel, std::move(send.value)};
}

template <class T>
typename Channel<T>::ReceiveAwaiter operator co_await(SelectReceiveCase<T>&& receive) noexcept {
  ALYRN_CHECK(receive.channel != nullptr, "Select receive case has no Channel");
  ALYRN_CHECK(receive.output != nullptr, "Select receive case has no output");
  return typename Channel<T>::ReceiveAwaiter{*receive.channel, *receive.output};
}

}  // namespace alyrn::coro
