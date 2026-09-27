// SPDX-License-Identifier: MIT
#pragma once

#include <functional>
#include <optional>

#include "alyrn/backend/loop.h"
#include "alyrn/coro/channel.h"
#include "alyrn/coro/select_case.h"
#include "alyrn/detail/check.h"
#include "alyrn/detail/loop_shutdown.h"
#include "alyrn/detail/macros.h"
#include "alyrn/result.h"
#include "alyrn/time/clock.h"
#include "alyrn/time/timer_id.h"

namespace alyrn::detail {

// Shared implementation of the backends' Timer and Ticker: a loop timer that
// delivers its firing time through a one-slot Channel, so it can be received
// directly or selected against other channels. Ops adapts one backend loop:
//
//   static Result<time::TimerId> Arm(Loop&, time::Deadline, std::function<void()>);
//   static void Disarm(Loop&, time::TimerId) noexcept;
//   static void Register(Loop&, LoopShutdownParticipant&) noexcept;
//   static void Unregister(Loop&, LoopShutdownParticipant&) noexcept;
//
// Channel operations require the loop's scheduling context, so every one of
// them here runs through RunOnOwner: a timer may be created or destroyed on
// the owner thread outside Run().
template <class Loop, class Ops>
class BasicTimerChannel {
public:
  ALYRN_DELETE_COPY_MOVE(BasicTimerChannel);

  // Receives the next firing time: co_await (timer >> fired), or a case of
  // Select. Once the loop stops, the channel is closed and a receive
  // completes with an empty optional instead of waiting forever.
  coro::SelectReceiveCase<time::Deadline> operator>>(
      std::optional<time::Deadline>& fired) noexcept {
    return channel_ >> fired;
  }

protected:
  // A zero period fires once; a positive period repeats.
  BasicTimerChannel(Loop& loop, time::Duration delay, time::Duration period)
      : loop_(&loop), period_(period), channel_(loop, 1) {
    ALYRN_CHECK(loop.IsInLoopThread(), "Timer created from wrong Loop thread");
    if (Stopping()) {
      CloseChannel();
      return;
    }
    Ops::Register(*loop_, participant_);
    Arm(time::SteadyNow() + delay);
  }

  ~BasicTimerChannel() {
    ALYRN_CHECK(loop_->IsInLoopThread(), "Timer destroyed from wrong Loop thread");
    Disarm();
    if (participant_.InList()) {
      Ops::Unregister(*loop_, participant_);
    }
    CloseChannel();
  }

  // Cancels future firings and discards a firing that was not received.
  // Returns whether a firing was still scheduled.
  bool StopFiring() noexcept {
    ALYRN_CHECK(loop_->IsInLoopThread(), "Timer used from wrong Loop thread");
    const bool scheduled = timer_.Valid();
    Disarm();
    DiscardUnreceived();
    return scheduled;
  }

  // Stops, then fires after delay (and every period, when it is positive).
  // A timer whose loop has stopped stays closed.
  void Rearm(time::Duration delay, time::Duration period) {
    (void)StopFiring();
    period_ = period;
    if (!participant_.InList()) {
      return;
    }
    Arm(time::SteadyNow() + delay);
  }

private:
  static void OnLoopStop(void* context) noexcept {
    auto* self = static_cast<BasicTimerChannel*>(context);
    self->Disarm();
    Ops::Unregister(*self->loop_, self->participant_);
    self->CloseChannel();
  }

  bool Stopping() const noexcept {
    const backend::LoopState state = loop_->State();
    return state == backend::LoopState::kStopping || state == backend::LoopState::kStopped;
  }

  void Arm(time::Deadline when) {
    next_ = when;
    auto armed = Ops::Arm(*loop_, when, [this] { Fire(); });
    if (!armed.HasValue()) {
      // A timer that cannot fire behaves like one whose loop stopped.
      if (participant_.InList()) {
        Ops::Unregister(*loop_, participant_);
      }
      CloseChannel();
      return;
    }
    timer_ = *armed;
  }

  // Timer callbacks run in the loop's scheduling context.
  void Fire() {
    timer_ = {};
    const time::Deadline now = time::SteadyNow();
    // One slot: a firing the receiver has not taken yet absorbs this one.
    (void)channel_.TrySend(now);
    if (period_ > time::Duration::zero()) {
      next_ += period_;
      if (next_ <= now) {
        // Skip the periods a slow receiver or a busy loop already missed.
        next_ = now + period_;
      }
      Arm(next_);
    }
  }

  void Disarm() noexcept {
    if (timer_.Valid()) {
      Ops::Disarm(*loop_, timer_);
      timer_ = {};
    }
  }

  void DiscardUnreceived() noexcept {
    loop_->RunOnOwner([this] {
      std::optional<time::Deadline> stale;
      if (!channel_.Closed()) {
        (void)channel_.TryReceive(stale);
      }
    });
  }

  void CloseChannel() noexcept {
    loop_->RunOnOwner([this] {
      if (!channel_.Closed()) {
        channel_.Close();
      }
    });
  }

  Loop* loop_;
  time::Duration period_;
  coro::Channel<time::Deadline> channel_;
  time::TimerId timer_{};
  time::Deadline next_{};
  LoopShutdownParticipant participant_{this, &OnLoopStop};
};

}  // namespace alyrn::detail
