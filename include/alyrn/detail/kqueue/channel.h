// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>

#include "alyrn/kqueue/options.h"
#include "alyrn/detail/utils/macros.h"

namespace alyrn::kqueue {

class Loop;

namespace detail {

// Channel is the event dispatch unit for a single file descriptor.
//
// A Channel does not own the file descriptor. Instead, it records:
// - which events the owner is interested in
// - which events were returned by the Poller
// - which callbacks should run for read, write, close, and error events
//
// Channel is also responsible for keeping its local event state consistent
// with the registration state stored in the Poller.
class Channel {
public:
  ALYRN_DELETE_COPY(Channel);

  using EventCallback = void (*)(void*) noexcept;
  using ReadEventCallback = void (*)(void*) noexcept;

  explicit Channel(Loop* loop, int fd) noexcept;
  ~Channel() noexcept = default;

  // Moving transfers the non-owning fd association and callbacks. Both the
  // source and destination must be detached from the Poller; a registered
  // Poller entry stores the Channel object's address and cannot be moved
  // transparently.
  Channel(Channel&& other) noexcept;
  Channel& operator=(Channel&& other) noexcept;

  // Dispatches the active events stored in revents_ to the corresponding
  // callbacks.
  void HandleEvent();

  void SetReadCallback(ReadEventCallback callback, void* context) noexcept {
    read_callback_ = callback;
    read_context_ = context;
  }
  void SetWriteCallback(EventCallback callback, void* context) noexcept {
    write_callback_ = callback;
    write_context_ = context;
  }
  void SetCloseCallback(EventCallback callback, void* context) noexcept {
    close_callback_ = callback;
    close_context_ = context;
  }
  void SetErrorCallback(EventCallback callback, void* context) noexcept {
    error_callback_ = callback;
    error_context_ = context;
  }

  [[nodiscard]]
  int Fd() const {
    return fd_;
  }
  [[nodiscard]]
  int Events() const {
    return events_;
  }
  [[nodiscard]]
  int Revents() const {
    return revents_;
  }
  void SetRevents(int revt) { revents_ = revt; }

  // Updates the local interest set and immediately synchronizes it with the
  // underlying Poller.
  void EnableReading() {
    events_ |= kReadEvent;
    Update();
  }
  void DisableReading() {
    events_ &= ~kReadEvent;
    Update();
  }
  void EnableWriting() {
    events_ |= kWriteEvent;
    Update();
  }
  void DisableWriting() {
    events_ &= ~kWriteEvent;
    Update();
  }
  void DisableAll() {
    events_ = kNoneEvent;
    Update();
  }

  [[nodiscard]]
  bool IsNoneEvent() const {
    return events_ == kNoneEvent;
  }
  [[nodiscard]]
  bool IsWriting() const {
    return static_cast<bool>(events_ & kWriteEvent);
  }
  [[nodiscard]]
  bool IsReading() const {
    return static_cast<bool>(events_ & kReadEvent);
  }

  /*
   * Selects how readiness is delivered. A mode switch is expressed to the
   * kernel as a re-add of whichever filters are currently armed, so it is
   * pushed down immediately when there is interest to re-arm. With no
   * interest there is nothing to re-add, and skipping the sync also avoids
   * creating a poller entry for a Channel that has never been armed.
   */
  void SetTriggerMode(TriggerMode mode) {
    if (trigger_mode_ == mode) {
      return;
    }
    trigger_mode_ = mode;
    if (!IsNoneEvent()) {
      Update();
    }
  }

  [[nodiscard]]
  TriggerMode Mode() const {
    return trigger_mode_;
  }

  [[nodiscard]]
  bool IsEdgeTriggered() const {
    return trigger_mode_ == TriggerMode::kEdgeTriggered;
  }

  /*
   * One-shot interest is consumed by delivery. A Channel in this mode reports
   * no interest in a filter once that filter has fired, and the callback must
   * call EnableReading() or EnableWriting() again to arm the next operation.
   */
  [[nodiscard]]
  bool IsOneShot() const {
    return trigger_mode_ == TriggerMode::kOneShot;
  }

  // Returns whether the owning poller currently tracks this Channel. The
  // authoritative registration state lives in the poller's per-filter table,
  // not here; Loop deliberately keeps that table private from kqueue
  // callers.
  [[nodiscard]]
  bool IsRegistered() const;

  // Returns the Loop that owns this Channel.
  Loop* OwnerLoop() { return loop_; }

  // Removes the Channel from its owning Loop.
  void Remove();

  // A Channel starts with no interested events.
  // Read events include normal readable data and priority data.
  // Write events indicate that the fd can accept more output.
  static constexpr int kNoneEvent = 0;
  static constexpr int kReadEvent = 0x01;
  static constexpr int kWriteEvent = 0x02;
  static constexpr int kErrorEvent = 0x04;
  static constexpr int kHupEvent = 0x08;

private:
  // The active epoch is Poller-private bookkeeping and must stay hidden from
  // general consumers. The concrete Poller is the only legitimate user.
  friend class Poller;

  // Read and write readiness arrive as two separate kevents, so one Poll() can
  // report the same Channel twice. The poller stamps the current poll epoch on
  // first sight to decide whether to overwrite revents_ or accumulate into it.
  [[nodiscard]]
  std::uint64_t ActiveEpoch() const noexcept {
    return active_epoch_;
  }
  void SetActiveEpoch(std::uint64_t epoch) noexcept { active_epoch_ = epoch; }

  /*
   * Withdraws interest that the kernel has already dropped on delivery of a
   * one-shot event. This deliberately does not call Update(): there is no
   * registration left to remove, and issuing EV_DELETE for it would be
   * rejected. Clearing the bit here is what lets a re-arming Enable call read
   * as a real change and lets Remove() see an empty interest set.
   */
  void ConsumeOneShot(int event) noexcept { events_ &= ~event; }

  // Pushes the current interest set to the Poller.
  void Update();

  Loop* loop_{nullptr};
  int fd_{-1};
  int events_{kNoneEvent};
  int revents_{kNoneEvent};
  std::uint64_t active_epoch_{0};
  TriggerMode trigger_mode_{TriggerMode::kLevelTriggered};

  ReadEventCallback read_callback_{nullptr};
  EventCallback write_callback_{nullptr};
  EventCallback close_callback_{nullptr};
  EventCallback error_callback_{nullptr};
  void* read_context_{nullptr};
  void* write_context_{nullptr};
  void* close_context_{nullptr};
  void* error_context_{nullptr};
};

}  // namespace detail
}  // namespace alyrn::kqueue
