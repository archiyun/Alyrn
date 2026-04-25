#pragma once

#include "runtime/base/noncopyable.h"
#include "runtime/time/timestamp.h"

#include <functional>
#include <memory>
#include <utility>

namespace runtime::net {

class EventLoop;

// TriggerMode controls whether epoll registration uses level-triggered or
// edge-triggered delivery.
enum class TriggerMode {
  kLevelTriggered,
  kEdgeTriggered,
};

// Channel is the event dispatch unit for a single file descriptor.
//
// A Channel does not own the file descriptor. Instead, it records:
// - which events the owner is interested in
// - which events were returned by the Poller
// - which callbacks should run for read, write, close, and error events
//
// Channel is also responsible for keeping its local event state consistent
// with the registration state stored in the Poller.
class Channel : public runtime::base::NonCopyable {
public:
  using EventCallback = std::function<void()>;
  using ReadEventCallback = std::function<void(runtime::time::Timestamp)>;

  explicit Channel(EventLoop* loop, int fd);
  ~Channel();

  // Dispatches the active events stored in revents_ to the corresponding
  // callbacks.
  void HandleEvent(runtime::time::Timestamp receive_time);

  void SetReadCallback(ReadEventCallback&& cb) {
    read_callback_ = std::forward<ReadEventCallback>(cb);
  }
  void SetWriteCallback(EventCallback&& cb) {
    write_callback_ = std::forward<EventCallback>(cb);
  }
  void SetCloseCallback(EventCallback&& cb) {
    close_callback_ = std::forward<EventCallback>(cb);
  }
  void SetErrorCallback(EventCallback&& cb) {
    error_callback_ = std::forward<EventCallback>(cb);
  }

  // Ties the Channel to an owner object so callbacks are not dispatched after
  // the owner has already been destroyed.
  void Tie(const std::shared_ptr<void>&);

  int Fd() const { return fd_; }
  int Events() const { return events_; }
  int Revents() const { return revents_; }
  void SetRevents(int revt) { revents_ = revt; }

  // Updates the local interest set and immediately synchronizes it with the
  // underlying Poller.
  void EnableReading() { events_ |= kReadEvent; Update(); }
  void DisableReading() { events_ &= ~kReadEvent; Update(); }
  void EnableWriting() { events_ |= kWriteEvent; Update(); }
  void DisableWriting() { events_ &= ~kWriteEvent; Update(); }
  void DisableAll() { events_ = kNoneEvent; Update(); }

  bool IsNoneEvent() const { return events_ == kNoneEvent; }
  bool IsWriting() const { return events_ & kWriteEvent; }
  bool IsReading() const { return events_ & kReadEvent; }

  int Index() const { return index_; }

  // Stores the Poller-specific index used to track registration state.
  void SetIndex(int idx) { index_ = idx; }

  // Switches the Channel between level-triggered and edge-triggered mode.
  void SetEdgeTriggered(bool et) {
    trigger_mode_ = et ? TriggerMode::kEdgeTriggered
                       : TriggerMode::kLevelTriggered;
  }

  bool IsEdgeTriggered() const {
    return trigger_mode_ == TriggerMode::kEdgeTriggered;
  }

  // Returns the EventLoop that owns this Channel.
  EventLoop* OwnerLoop() { return loop_; }

  // Removes the Channel from its owning EventLoop.
  void Remove();

  // A Channel starts with no interested events.
  // Read events include normal readable data and priority data.
  // Write events indicate that the fd can accept more output.
  static const int kNoneEvent{0};
  static const int kReadEvent{0x01};
  static const int kWriteEvent{0x02};
  static const int kErrorEvent{0x04};
  static const int kHupEvent{0x08}; // 对端关闭
private:
  // Pushes the current interest set to the Poller.
  void Update();

  // Dispatches events only after verifying that the tied owner is still alive.
  void HandleEventWithGuard(runtime::time::Timestamp receive_time);

private:

  EventLoop* loop_;
  const int fd_;
  int events_;
  int revents_;
  int index_;
  TriggerMode trigger_mode_ { TriggerMode::kLevelTriggered };

  std::weak_ptr<void> tie_;
  bool tied_;

  ReadEventCallback read_callback_;
  EventCallback write_callback_;
  EventCallback close_callback_;
  EventCallback error_callback_;

};

}  // namespace runtime::net
