// SPDX-License-Identifier: MIT
// Compile-time contract checks shared by backend-specific tests.
#pragma once

#include <concepts>
#include <type_traits>

#include "coropact/io/async_connector.h"
#include "coropact/io/async_listener.h"
#include "coropact/io/async_stream.h"

namespace coropact::test::contracts {

template <class Stream>
concept CoreStream = io::AsyncStream<Stream> && std::move_constructible<Stream>;

template <class Listener>
concept CoreListener = io::AsyncListener<Listener> && std::move_constructible<Listener>;

template <class Connector>
concept CoreConnector = io::AsyncConnector<Connector> && std::move_constructible<Connector>;

template <class Stream>
consteval void AssertCoreStream() {
  static_assert(CoreStream<Stream>, "backend stream does not satisfy AsyncStream Core contract");
}

template <class Listener>
consteval void AssertCoreListener() {
  static_assert(CoreListener<Listener>,
                "backend listener does not satisfy AsyncListener Core contract");
}

template <class Connector>
consteval void AssertCoreConnector() {
  static_assert(CoreConnector<Connector>,
                "backend connector does not satisfy AsyncConnector Core contract");
}

}  // namespace coropact::test::contracts
