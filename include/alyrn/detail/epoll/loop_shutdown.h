// SPDX-License-Identifier: MIT
#pragma once

#include "alyrn/detail/ds/intrusive_list.h"
#include "alyrn/detail/macros.h"

namespace alyrn::epoll::detail {

// Owner-loop resource that must be synchronously told when its dispatcher
// begins shutdown. RequestStop() may be called from any thread, but the
// registry invokes this callback only from the loop thread.
class LoopShutdownParticipant : public ::alyrn::detail::ds::ListNode<LoopShutdownParticipant> {
public:
  using StopFn = void (*)(void*) noexcept;

  LoopShutdownParticipant(void* context, StopFn stop) noexcept : context_(context), stop_(stop) {}

  ~LoopShutdownParticipant() noexcept = default;

  ALYRN_DELETE_COPY_MOVE(LoopShutdownParticipant);

  void RequestStop() noexcept { stop_(context_); }

private:
  void* context_;
  StopFn stop_;
};

// Intrusive, owner-thread-only registry for resources associated with one
// Epoll Loop. It deliberately only requests logical resource shutdown:
// the loop still owns draining scheduled continuations and backend events.
class LoopShutdownRegistry final {
public:
  LoopShutdownRegistry() noexcept = default;

  ALYRN_DELETE_COPY_MOVE(LoopShutdownRegistry);

  bool Register(LoopShutdownParticipant* participant) noexcept {
    return participants_.PushBack(participant);
  }

  bool Unregister(LoopShutdownParticipant* participant) noexcept {
    return participants_.Erase(participant);
  }

  void RequestStop() noexcept {
    participants_.ForEachSafe([](LoopShutdownParticipant& participant) noexcept {
      participant.RequestStop();
      return false;
    });
  }

  bool Empty() const noexcept { return participants_.Empty(); }

private:
  ::alyrn::detail::ds::IntrusiveList<LoopShutdownParticipant> participants_;
};

}  // namespace alyrn::epoll::detail
