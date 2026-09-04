// SPDX-License-Identifier: MIT
#pragma once

#include <optional>
#include <utility>

namespace alyrn::coro {

template <class T>
class Channel;

template <class T>
struct SelectReceiveCase {
  Channel<T>* channel;
  std::optional<T>* output;
};

template <class T>
struct SelectSendCase {
  Channel<T>* channel;
  T value;
};

struct SelectDefaultCase {};

template <class T>
SelectReceiveCase<T> SelectReceive(Channel<T>& channel, std::optional<T>& output) noexcept {
  return SelectReceiveCase<T>{&channel, &output};
}

template <class T>
SelectSendCase<T> SelectSend(Channel<T>& channel, T value) noexcept {
  return SelectSendCase<T>{&channel, std::move(value)};
}

inline SelectDefaultCase SelectDefault() noexcept { return {}; }

}  // namespace alyrn::coro
