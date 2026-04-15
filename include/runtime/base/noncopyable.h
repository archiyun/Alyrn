#pragma once

namespace runtime::base {

// NonCopyable disables copy construction and copy assignment for derived
// types.
class NonCopyable {
protected:
  NonCopyable() = default;
  ~NonCopyable() = default;

  NonCopyable(const NonCopyable&) = delete;
  NonCopyable& operator=(const NonCopyable&) = delete;
};

}  // namespace runtime::base
