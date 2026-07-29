// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <concepts>
#include <optional>

#include "coropact/backend/async_stream.h"
#include "coropact/coro/task.h"
#include "coropact/net/accept_source.h"

namespace coropact::backend {

using AcceptSourceOptions = ::coropact::net::AcceptSourceOptions;

template <class T>
concept AsyncAcceptSource = requires(T& source) {
  typename T::Stream;
  requires AsyncStream<typename T::Stream>;
  { source.Next() } -> std::same_as<
      coro::Task<base::Result<std::optional<typename T::Stream>>>>;
  { source.Stop() } -> std::same_as<coro::Task<base::Result<void>>>;
};

}  // namespace coropact::backend
