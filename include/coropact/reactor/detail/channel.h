// SPDX-License-Identifier: MIT
#pragma once

#include "coropact/reactor/options.h"
#include "coropact/utils/macros.h"

namespace coropact::reactor {

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
  COROPACT_DELETE_COPY(Channel);

  using EventCallback = void (*)(void*) noexcept;
  using ReadEventCallback = void (*)(void*) noexcept;

  explicit Channel(Loop* loop, int fd) noexcept;
  ~Channel() = default;

  // Moving transfers the non-owning fd association and callbacks. Both the
  // source and destination must be detached from the Poller; a registered
  // Poller entry stores the Channel object's address and cannot be moved
  // transparently.
  Channel(Channel&& other) noexcept;
  Channel& operator=(Channel&& other) noexcept;

  // Dispatches the active events stored in revents_ to the corresponding
  // callbacks.
  void HandleEvent() noexcept;

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
  int Fd() const noexcept{
    return fd_;
  }
  [[nodiscard]]
  int Events() const noexcept{
    return events_;
  }
  [[nodiscard]]
  int Revents() const noexcept{
    return revents_;
  }
  void SetRevents(int revt) noexcept { revents_ = revt; }

  // Updates the local interest set and immediately synchronizes it with the
  // underlying Poller.
  void EnableReading() noexcept {
    events_ |= kReadEvent;
    Update();
  }
  void DisableReading() noexcept {
    events_ &= ~kReadEvent;
    Update();
  }
  void EnableWriting() noexcept {
    events_ |= kWriteEvent;
    Update();
  }
  void DisableWriting() noexcept {
    events_ &= ~kWriteEvent;
    Update();
  }
  void DisableAll() noexcept {
    events_ = kNoneEvent;
    Update();
  }

  [[nodiscard]]
  bool IsNoneEvent() const noexcept {
    return events_ == kNoneEvent;
  }
  [[nodiscard]]
  bool IsWriting() const noexcept {
    return static_cast<bool>(events_ & kWriteEvent);
  }
  [[nodiscard]]
  bool IsReading() const noexcept {
    return static_cast<bool>(events_ & kReadEvent);
  }

  // Switches the Channel between level-triggered and edge-triggered mode.
  void SetEdgeTriggered(bool et_mode) noexcept {
    trigger_mode_ = et_mode ? TriggerMode::kEdgeTriggered : TriggerMode::kLevelTriggered;
  }

  [[nodiscard]]
  bool IsEdgeTriggered() const noexcept {
    return trigger_mode_ == TriggerMode::kEdgeTriggered;
  }

  // Returns whether the owning poller currently tracks this Channel. This is
  // an implementation-only lifecycle query; Loop deliberately keeps its
  // poller membership private from Reactor callers.
  [[nodiscard]]
  bool IsRegistered() const noexcept;

  // Returns the Loop that owns this Channel.
  Loop* OwnerLoop() const noexcept { return loop_; }

  // Removes the Channel from its owning Loop.
  void Remove() noexcept;

  // A Channel starts with no interested events.
  // Read events include normal readable data and priority data.
  // Write events indicate that the fd can accept more output.
  static constexpr int kNoneEvent = 0;
  static constexpr int kReadEvent = 0x01;
  static constexpr int kWriteEvent = 0x02;
  static constexpr int kErrorEvent = 0x04;
  static constexpr int kHupEvent = 0x08;

private:
  // Index/set_index track Poller-private registration state and must remain
  // hidden from general consumers. Concrete Poller implementations are the
  // only legitimate users.
  friend class EPollPoller;

  [[nodiscard]]
  int index() const {
    return index_;
  }
  void set_index(int idx) { index_ = idx; }

  // Pushes the current interest set to the Poller.
  void Update() noexcept;

  Loop* loop_{nullptr};
  int fd_{-1};
  int events_{kNoneEvent};
  int revents_{kNoneEvent};
  int index_{-1};
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
}  // namespace coropact::reactor
