// SPDX-License-Identifier: MIT
#pragma once

#include <array>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <tuple>
#include <type_traits>
#include <utility>

#include "alyrn/coro/channel.h"
#include "alyrn/coro/detail/select_awaiter_storage.h"
#include "alyrn/detail/check.h"
#include "alyrn/detail/macros.h"

namespace alyrn::coro::detail {

class SelectState final {
public:
  static constexpr std::size_t kNoWinner = std::numeric_limits<std::size_t>::max();

  void Begin(Scheduler& scheduler, std::coroutine_handle<> continuation) noexcept {
    scheduler_ = &scheduler;
    continuation_.SetHandle(continuation);
    winner_ = kNoWinner;
    phase_ = Phase::kArming;
  }

  bool Completed() const noexcept { return winner_ != kNoWinner; }

  std::size_t Winner() const noexcept {
    ALYRN_CHECK(Completed(), "Select has no selected case");
    return winner_;
  }

  bool TryWin(std::size_t index) noexcept {
    if (Completed()) {
      return false;
    }
    winner_ = index;
    return true;
  }

  void MarkWaiting() noexcept {
    ALYRN_CHECK(phase_ == Phase::kArming, "Select was not in its arming phase");
    ALYRN_CHECK(!Completed(), "completed Select cannot enter its waiting phase");
    phase_ = Phase::kWaiting;
  }

  void ResumeIfWaiting() noexcept {
    if (phase_ != Phase::kWaiting) {
      return;
    }
    ALYRN_CHECK(scheduler_ != nullptr, "Select has no Scheduler");
    phase_ = Phase::kScheduled;
    scheduler_->Schedule(&continuation_);
  }

private:
  enum class Phase { kArming, kWaiting, kScheduled };

  Scheduler* scheduler_{nullptr};
  ResumeWork continuation_;
  std::size_t winner_{kNoWinner};
  Phase phase_{Phase::kArming};
};

using SelectCompleteFn = void (*)(void*, std::size_t) noexcept;

inline std::size_t SelectRandomIndex(std::size_t upper_bound) noexcept {
  ALYRN_CHECK(upper_bound != 0, "Select random choice requires a non-empty set");
  static thread_local std::uint64_t state = 0x9e3779b97f4a7c15ULL;
  state ^= state << 13;
  state ^= state >> 7;
  state ^= state << 17;
  return static_cast<std::size_t>(state % upper_bound);
}

template <class T>
class SelectSendRegistration final : public ChannelSendWaiter<T> {
public:
  ALYRN_DELETE_COPY_MOVE(SelectSendRegistration);

  explicit SelectSendRegistration(SelectSendCase<T>& select_case) noexcept
      : select_case_(&select_case), channel_(select_case.channel) {
    ALYRN_CHECK(channel_ != nullptr, "Select send case has no Channel");
  }

  ~SelectSendRegistration() override {
    if (queued_) {
      channel_->CancelSend(*this);
    }
  }

  void Bind(void* context, SelectState& state, std::size_t index,
            SelectCompleteFn complete) noexcept {
    context_ = context;
    state_ = &state;
    index_ = index;
    complete_ = complete;
  }

  bool IsDefault() const noexcept { return false; }
  bool Ready() const noexcept { return channel_->SelectSendReady(); }

  void Arm() noexcept {
    ALYRN_CHECK(active_, "inactive Select send case cannot be armed");
    ALYRN_CHECK(!queued_, "Select send case armed twice");
    queued_ = true;
    channel_->RegisterSelectSend(*this);
  }

  void Commit() noexcept { channel_->CommitSelectSend(*this); }

  void Cancel() noexcept {
    if (!active_) {
      return;
    }
    if (queued_) {
      channel_->CancelSend(*this);
      return;
    }
    active_ = false;
  }

  T TakeValue() noexcept override {
    ALYRN_CHECK(!value_taken_, "Select sender value taken twice");
    value_taken_ = true;
    return std::move(select_case_->value);
  }

  void Complete() noexcept override {
    if (!active_) {
      return;
    }
    active_ = false;
    queued_ = false;
    if (!state_->TryWin(index_)) {
      return;
    }
    complete_(context_, index_);
    state_->ResumeIfWaiting();
  }

  void NotifyClosed() noexcept override {
    if (!active_) {
      return;
    }
    active_ = false;
    queued_ = false;
    if (!state_->TryWin(index_)) {
      return;
    }
    complete_(context_, index_);
    ALYRN_CHECK(false, "send on closed Channel");
  }

  void Cancelled() noexcept override {
    active_ = false;
    queued_ = false;
  }

private:
  SelectSendCase<T>* select_case_;
  Channel<T>* channel_;
  SelectState* state_{nullptr};
  void* context_{nullptr};
  SelectCompleteFn complete_{nullptr};
  std::size_t index_{0};
  bool active_{true};
  bool queued_{false};
  bool value_taken_{false};
};

template <class T>
class SelectReceiveRegistration final : public ChannelReceiveWaiter<T> {
public:
  ALYRN_DELETE_COPY_MOVE(SelectReceiveRegistration);

  explicit SelectReceiveRegistration(SelectReceiveCase<T>& select_case) noexcept
      : channel_(select_case.channel), output_(select_case.output) {
    ALYRN_CHECK(channel_ != nullptr, "Select receive case has no Channel");
    ALYRN_CHECK(output_ != nullptr, "Select receive case has no output");
  }

  ~SelectReceiveRegistration() override {
    if (queued_) {
      channel_->CancelReceive(*this);
    }
  }

  void Bind(void* context, SelectState& state, std::size_t index,
            SelectCompleteFn complete) noexcept {
    context_ = context;
    state_ = &state;
    index_ = index;
    complete_ = complete;
  }

  bool IsDefault() const noexcept { return false; }
  bool Ready() const noexcept { return channel_->SelectReceiveReady(); }

  void Arm() noexcept {
    ALYRN_CHECK(active_, "inactive Select receive case cannot be armed");
    ALYRN_CHECK(!queued_, "Select receive case armed twice");
    queued_ = true;
    channel_->RegisterSelectReceive(*this);
  }

  void Commit() noexcept { channel_->CommitSelectReceive(*this); }

  void Cancel() noexcept {
    if (!active_) {
      return;
    }
    if (queued_) {
      channel_->CancelReceive(*this);
      return;
    }
    active_ = false;
  }

  void CompleteValue(T value) noexcept override {
    if (!active_) {
      return;
    }
    active_ = false;
    queued_ = false;
    if (!state_->TryWin(index_)) {
      return;
    }
    output_->reset();
    output_->emplace(std::move(value));
    complete_(context_, index_);
    state_->ResumeIfWaiting();
  }

  void CompleteClosed() noexcept override {
    if (!active_) {
      return;
    }
    active_ = false;
    queued_ = false;
    if (!state_->TryWin(index_)) {
      return;
    }
    output_->reset();
    complete_(context_, index_);
    state_->ResumeIfWaiting();
  }

  void Cancelled() noexcept override {
    active_ = false;
    queued_ = false;
  }

private:
  Channel<T>* channel_;
  std::optional<T>* output_;
  SelectState* state_{nullptr};
  void* context_{nullptr};
  SelectCompleteFn complete_{nullptr};
  std::size_t index_{0};
  bool active_{true};
  bool queued_{false};
};

class SelectDefaultRegistration final {
public:
  ALYRN_DELETE_COPY_MOVE(SelectDefaultRegistration);

  explicit SelectDefaultRegistration(SelectDefaultCase&) noexcept {}

  void Bind(void* context, SelectState& state, std::size_t index,
            SelectCompleteFn complete) noexcept {
    context_ = context;
    state_ = &state;
    index_ = index;
    complete_ = complete;
  }

  bool IsDefault() const noexcept { return true; }
  bool Ready() const noexcept { return false; }
  void Arm() noexcept {}
  void Cancel() noexcept {}

  void Commit() noexcept {
    if (!state_->TryWin(index_)) {
      return;
    }
    complete_(context_, index_);
    state_->ResumeIfWaiting();
  }

private:
  SelectState* state_{nullptr};
  void* context_{nullptr};
  SelectCompleteFn complete_{nullptr};
  std::size_t index_{0};
};

template <class Case>
class SelectRegistration;

template <class T>
class SelectRegistration<SelectSendCase<T>> final {
public:
  explicit SelectRegistration(SelectSendCase<T>& select_case) noexcept
      : registration_(select_case) {}

  void Bind(void* context, SelectState& state, std::size_t index,
            SelectCompleteFn complete) noexcept {
    registration_.Bind(context, state, index, complete);
  }
  bool IsDefault() const noexcept { return registration_.IsDefault(); }
  bool Ready() const noexcept { return registration_.Ready(); }
  void Arm() noexcept { registration_.Arm(); }
  void Commit() noexcept { registration_.Commit(); }
  void Cancel() noexcept { registration_.Cancel(); }

private:
  SelectSendRegistration<T> registration_;
};

template <class T>
class SelectRegistration<SelectReceiveCase<T>> final {
public:
  explicit SelectRegistration(SelectReceiveCase<T>& select_case) noexcept
      : registration_(select_case) {}

  void Bind(void* context, SelectState& state, std::size_t index,
            SelectCompleteFn complete) noexcept {
    registration_.Bind(context, state, index, complete);
  }
  bool IsDefault() const noexcept { return registration_.IsDefault(); }
  bool Ready() const noexcept { return registration_.Ready(); }
  void Arm() noexcept { registration_.Arm(); }
  void Commit() noexcept { registration_.Commit(); }
  void Cancel() noexcept { registration_.Cancel(); }

private:
  SelectReceiveRegistration<T> registration_;
};

template <>
class SelectRegistration<SelectDefaultCase> final {
public:
  explicit SelectRegistration(SelectDefaultCase& select_case) noexcept
      : registration_(select_case) {}

  void Bind(void* context, SelectState& state, std::size_t index,
            SelectCompleteFn complete) noexcept {
    registration_.Bind(context, state, index, complete);
  }
  bool IsDefault() const noexcept { return registration_.IsDefault(); }
  bool Ready() const noexcept { return registration_.Ready(); }
  void Arm() noexcept { registration_.Arm(); }
  void Commit() noexcept { registration_.Commit(); }
  void Cancel() noexcept { registration_.Cancel(); }

private:
  SelectDefaultRegistration registration_;
};

}  // namespace alyrn::coro::detail

namespace alyrn::coro {

template <class... Cases>
class [[nodiscard]] SelectAwaiter final {
  using Storage = detail::SelectAwaiterStorage<std::decay_t<Cases>...>;
  using Registrations = std::tuple<detail::SelectRegistration<std::decay_t<Cases>>...>;
  using ReadyCases = std::array<bool, sizeof...(Cases)>;

public:
  ALYRN_DELETE_COPY_MOVE(SelectAwaiter);

  static constexpr std::size_t kCaseCount = sizeof...(Cases);

  template <class... Args>
    requires(sizeof...(Cases) == sizeof...(Args))
  explicit SelectAwaiter(Args&&... args)
      : SelectAwaiter(std::index_sequence_for<Cases...>{}, std::forward<Args>(args)...) {}

  bool await_ready() const noexcept { return false; }

  bool await_suspend(std::coroutine_handle<> continuation) noexcept {
    auto& scheduler = Scheduler::RequireCurrent();
    state_.Begin(scheduler, continuation);
    BindRegistrations(std::index_sequence_for<Cases...>{});

    ReadyCases ready{};
    std::size_t fallback = detail::SelectState::kNoWinner;
    FindReadyCases(std::index_sequence_for<Cases...>{}, ready, fallback);

    const std::size_t selected = ChooseReadyCase(ready);
    if (selected != detail::SelectState::kNoWinner) {
      CommitCase(selected, std::index_sequence_for<Cases...>{});
      return false;
    }
    if (fallback != detail::SelectState::kNoWinner) {
      CommitCase(fallback, std::index_sequence_for<Cases...>{});
      return false;
    }

    ArmRegistrations(std::index_sequence_for<Cases...>{});
    if (state_.Completed()) {
      return false;
    }
    state_.MarkWaiting();
    return true;
  }

  std::size_t await_resume() noexcept { return state_.Winner(); }

  template <std::size_t Index>
  decltype(auto) Case() noexcept {
    return storage_.template Case<Index>();
  }

  detail::SelectState& State() noexcept { return state_; }

private:
  template <std::size_t... Index, class... Args>
  explicit SelectAwaiter(std::index_sequence<Index...>, Args&&... args)
      : storage_(std::forward<Args>(args)...), registrations_(storage_.template Case<Index>()...) {}

  template <std::size_t... Index>
  void BindRegistrations(std::index_sequence<Index...>) noexcept {
    (std::get<Index>(registrations_).Bind(this, state_, Index, &SelectAwaiter::CaseCompleted), ...);
  }

  template <std::size_t Index>
  void FindReadyCase(ReadyCases& ready, std::size_t& fallback) noexcept {
    auto& registration = std::get<Index>(registrations_);
    if (registration.IsDefault()) {
      if (fallback == detail::SelectState::kNoWinner) {
        fallback = Index;
      }
      return;
    }
    ready[Index] = registration.Ready();
  }

  template <std::size_t... Index>
  void FindReadyCases(std::index_sequence<Index...>, ReadyCases& ready,
                      std::size_t& fallback) noexcept {
    (FindReadyCase<Index>(ready, fallback), ...);
  }

  static std::size_t ChooseReadyCase(const ReadyCases& ready) noexcept {
    std::size_t ready_count = 0;
    for (bool is_ready : ready) {
      ready_count += is_ready ? 1 : 0;
    }
    if (ready_count == 0) {
      return detail::SelectState::kNoWinner;
    }

    std::size_t choice = detail::SelectRandomIndex(ready_count);
    for (std::size_t index = 0; index != ready.size(); ++index) {
      if (!ready[index]) {
        continue;
      }
      if (choice == 0) {
        return index;
      }
      --choice;
    }
    ALYRN_CHECK(false, "Select failed to choose a ready case");
  }

  template <std::size_t... Index>
  void ArmRegistrations(std::index_sequence<Index...>) noexcept {
    (std::get<Index>(registrations_).Arm(), ...);
  }

  template <std::size_t Index>
  void CommitIfSelected(std::size_t selected, bool& committed) noexcept {
    if (Index == selected) {
      std::get<Index>(registrations_).Commit();
      committed = true;
    }
  }

  template <std::size_t... Index>
  void CommitCase(std::size_t selected, std::index_sequence<Index...>) noexcept {
    bool committed = false;
    (CommitIfSelected<Index>(selected, committed), ...);
    ALYRN_CHECK(committed, "Select selected an invalid case");
  }

  template <std::size_t Index>
  void CancelIfLoser(std::size_t winner) noexcept {
    if (Index != winner) {
      std::get<Index>(registrations_).Cancel();
    }
  }

  template <std::size_t... Index>
  void CancelLosers(std::size_t winner, std::index_sequence<Index...>) noexcept {
    (CancelIfLoser<Index>(winner), ...);
  }

  static void CaseCompleted(void* context, std::size_t winner) noexcept {
    auto* select = static_cast<SelectAwaiter*>(context);
    select->CancelLosers(winner, std::index_sequence_for<Cases...>{});
  }

  Storage storage_;
  Registrations registrations_;
  detail::SelectState state_;
};

template <class... Cases>
auto Select(Cases&&... cases) {
  static_assert(sizeof...(Cases) != 0, "Select requires at least one case");
  return SelectAwaiter<std::decay_t<Cases>...>{std::forward<Cases>(cases)...};
}

}  // namespace alyrn::coro
