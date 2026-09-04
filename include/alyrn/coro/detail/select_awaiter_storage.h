// SPDX-License-Identifier: MIT
#pragma once

#include <concepts>
#include <cstddef>
#include <tuple>
#include <type_traits>
#include <utility>

#include "alyrn/detail/macros.h"

namespace alyrn::coro::detail {

// Owns the movable case descriptors used by a SelectAwaiter. Channel
// registration, winner selection, and cancellation of losing cases belong to
// the SelectAwaiter implementation built on top of this storage.
template <class... Cases>
class SelectAwaiterStorage final {
  static_assert(sizeof...(Cases) != 0, "Select requires at least one case");

public:
  ALYRN_DELETE_COPY_MOVE(SelectAwaiterStorage);

  static constexpr std::size_t kCaseCount = sizeof...(Cases);

  template <class... Args>
    requires(sizeof...(Args) == sizeof...(Cases) &&
             (std::constructible_from<Cases, Args &&> && ...))
  explicit SelectAwaiterStorage(Args&&... args) noexcept(
      (std::is_nothrow_constructible_v<Cases, Args&&> && ...))
      : cases_(std::forward<Args>(args)...) {}

  template <std::size_t Index>
  decltype(auto) Case() noexcept {
    static_assert(Index < kCaseCount);
    return std::get<Index>(cases_);
  }

  template <std::size_t Index>
  decltype(auto) Case() const noexcept {
    static_assert(Index < kCaseCount);
    return std::get<Index>(cases_);
  }

private:
  std::tuple<Cases...> cases_;
};

template <class... Cases>
SelectAwaiterStorage(Cases&&...) -> SelectAwaiterStorage<std::decay_t<Cases>...>;

}  // namespace alyrn::coro::detail
