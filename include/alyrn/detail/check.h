// SPDX-License-Identifier: MIT
#pragma once

#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <source_location>
#include <string_view>

namespace alyrn::detail {
namespace {

inline const char* PanicTextData(std::string_view text) noexcept {
  return text.empty() ? "" : text.data();
}

inline int PanicPrintLength(std::size_t size) noexcept {
  constexpr auto kMax = static_cast<std::size_t>(std::numeric_limits<int>::max());
  return static_cast<int>(std::min(size, kMax));
}

inline void WritePanicText(std::string_view text) noexcept {
  while (!text.empty()) {
    const ssize_t written = ::write(STDERR_FILENO, text.data(), text.size());
    if (written > 0) {
      text.remove_prefix(static_cast<std::size_t>(written));
      continue;
    }
    if (written < 0 && errno == EINTR) {
      continue;
    }
    return;
  }
}

}  // namespace

[[noreturn]]
inline void Panic(std::string_view condition, std::string_view message,
                  std::source_location location = std::source_location::current()) noexcept {
  std::array<char, 2048> buffer{};

  const int length = std::snprintf(buffer.data(), buffer.size(),
                                   "\n[ALYRN PANIC] %.*s\n"
                                   "  condition: %.*s\n"
                                   "  location : %s:%u\n"
                                   "  function : %s\n",
                                   PanicPrintLength(message.size()), PanicTextData(message),
                                   PanicPrintLength(condition.size()), PanicTextData(condition),
                                   location.file_name(), location.line(), location.function_name());

  if (length > 0) {
    const auto output_size = std::min(static_cast<std::size_t>(length), buffer.size() - 1);
    WritePanicText(std::string_view(buffer.data(), output_size));
  }

  std::abort();
}

}  // namespace alyrn::detail

// Checks a non-recoverable program invariant in every build configuration.
// Recoverable failures from input, resource exhaustion, or system calls should
// be returned through alyrn::Result instead.
#define ALYRN_CHECK(condition, message)                                                 \
  do {                                                                                  \
    if (!(condition)) [[unlikely]] {                                                    \
      ::alyrn::detail::Panic(#condition, (message), ::std::source_location::current()); \
    }                                                                                   \
  } while (false)

// Checks an expensive diagnostic invariant in debug builds. Neither condition
// nor message is evaluated when NDEBUG is defined.
#ifndef NDEBUG
#define ALYRN_DCHECK(condition, message) ALYRN_CHECK(condition, message)
#else
#define ALYRN_DCHECK(condition, message) \
  do {                                   \
  } while (false)
#endif
