// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
//
// Backend-neutral awaitable concepts.
//
// This header describes:
//   1. The language shape of an awaiter.
//   2. Conversion from an awaitable expression to an awaiter.
//   3. Promise-dependent await_transform support.
//
// It does not depend on schedulers, EventLoop, or any I/O backend.
#pragma once

#include <concepts>
#include <coroutine>
#include <type_traits>
#include <utility>

namespace coropact::coro {

namespace detail {

// -----------------------------------------------------------------------------
// coroutine_handle detection
// -----------------------------------------------------------------------------

template <class T>
struct IsCoroutineHandle : std::false_type {};

template <class Promise>
struct IsCoroutineHandle<std::coroutine_handle<Promise>> : std::true_type {};

template <class T>
inline constexpr bool kIsCoroutineHandle = IsCoroutineHandle<T>::value;

// await_suspend() may return:
//   void
//   bool
//   std::coroutine_handle<Promise>
template <class T>
concept AwaitSuspendResult =
    std::same_as<T, void> || std::same_as<T, bool> || kIsCoroutineHandle<T>;

// After operator co_await, the language materializes the result and refers
// to the awaiter as an lvalue.
template <class T>
using AwaiterLvalue = std::add_lvalue_reference_t<std::remove_reference_t<T>>;

// await_ready() uses contextual conversion to bool. This also accepts types
// with an explicit operator bool().
template <class T>
concept ContextuallyBoolean = requires(T&& value) {
  { bool(std::forward<T>(value)) } -> std::same_as<bool>;
};

// -----------------------------------------------------------------------------
// operator co_await detection
// -----------------------------------------------------------------------------

template <class T>
concept HasMemberCoAwait = requires(T&& value) { std::forward<T>(value).operator co_await(); };

template <class T>
concept HasAdlCoAwait = requires(T&& value) { operator co_await(std::forward<T>(value)); };

// Member operator co_await has language-level precedence over ADL.
template <class T>
  requires HasMemberCoAwait<T>
decltype(auto) GetAwaiter(T&& value) noexcept(
    noexcept(std::forward<T>(value).operator co_await())) {
  return std::forward<T>(value).operator co_await();
}

// Non-member operator co_await found through ADL.
template <class T>
  requires(!HasMemberCoAwait<T> && HasAdlCoAwait<T>)
decltype(auto) GetAwaiter(T&& value) noexcept(noexcept(operator co_await(std::forward<T>(value)))) {
  return operator co_await(std::forward<T>(value));
}

// No operator co_await: the object itself is the awaiter.
template <class T>
  requires(!HasMemberCoAwait<T> && !HasAdlCoAwait<T>)
decltype(auto) GetAwaiter(T&& value) noexcept {
  return std::forward<T>(value);
}

}  // namespace detail

// -----------------------------------------------------------------------------
// Awaiter
// -----------------------------------------------------------------------------

// Checks whether T is an awaiter usable by a coroutine whose promise type is
// Promise. The language passes std::coroutine_handle<Promise> to await_suspend().
template <class T, class Promise>
concept AwaiterFor =
    requires(detail::AwaiterLvalue<T> awaiter, std::coroutine_handle<Promise> handle) {
      requires detail::ContextuallyBoolean<decltype(awaiter.await_ready())>;

      { awaiter.await_suspend(handle) } -> detail::AwaitSuspendResult;

      awaiter.await_resume();
    };

// Convenience concept for promise-independent awaiters that accept
// std::coroutine_handle<> (which is std::coroutine_handle<void>).
template <class T>
concept Awaiter = AwaiterFor<T, void>;

// -----------------------------------------------------------------------------
// Awaitable
// -----------------------------------------------------------------------------

// Checks an awaitable expression after any promise await_transform has already
// been applied.
//
// T deliberately preserves cv/ref information:
//   Awaitable<Foo>   checks an rvalue Foo
//   Awaitable<Foo&>  checks an lvalue Foo
template <class T, class Promise>
concept AwaitableFor = requires(T&& value) {
  requires AwaiterFor<decltype(detail::GetAwaiter(std::forward<T>(value))), Promise>;
};

// Convenience concept for promise-independent awaitables.
template <class T>
concept Awaitable = AwaitableFor<T, void>;

// -----------------------------------------------------------------------------
// Promise-dependent await_transform
// -----------------------------------------------------------------------------

// Checks the path:
//
//   promise.await_transform(value)
//       -> operator co_await
//       -> AwaiterFor<..., Promise>
//
// Keep this separate from Awaitable because await_transform belongs to the
// enclosing coroutine's promise type, not to the awaitable type itself.
template <class Promise, class T>
concept PromiseTransformedAwaitable = requires(Promise& promise, T&& value) {
  requires AwaitableFor<decltype(promise.await_transform(std::forward<T>(value))), Promise>;
};

// -----------------------------------------------------------------------------
// Useful type aliases
// -----------------------------------------------------------------------------

template <class T>
  requires Awaitable<T>
using AwaiterType = decltype(detail::GetAwaiter(std::declval<T>()));

template <class T>
  requires Awaitable<T>
using AwaitResult = decltype(std::declval<detail::AwaiterLvalue<AwaiterType<T>>>().await_resume());

}  // namespace coropact::coro
