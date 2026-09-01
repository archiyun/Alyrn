// SPDX-License-Identifier: MIT
#pragma once

#include <memory>
#include <stop_token>
#include <utility>

#include "alyrn/net/endpoint.h"
#include "alyrn/result.h"

namespace alyrn::runtime {

// Backend tags select a Runtime::Builder specialization at compile time. They
// carry no runtime state and never enter an I/O hot path.
struct Epoll final {};
struct Uring final {};
struct Kqueue final {};

// Selects the platform-native default backend at compile time. io_uring stays
// explicit because its availability and semantics are capability-dependent.
#if defined(__linux__)
using Auto = Epoll;
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || \
    defined(__OpenBSD__)
using Auto = Kqueue;
#else
#error "Alyrn has no default runtime backend for this platform"
#endif

}  // namespace alyrn::runtime

namespace alyrn::detail::runtime {

// Cold lifecycle seam only. Concrete controls remain owned by their backend;
// streams, awaiters, operations, and worker-local state are never erased.
class RuntimeControl {
public:
  virtual ~RuntimeControl() = default;

  virtual Result<void> Start() = 0;
  virtual Result<void> Run(std::stop_token stop_token) = 0;
  virtual void RequestStop() noexcept = 0;
  virtual void Stop() noexcept = 0;
  virtual bool Started() const noexcept = 0;
};

}  // namespace alyrn::detail::runtime

namespace alyrn {

/*
 * Backend-neutral application lifecycle control. Backend selection remains a
 * compile-time Builder choice; this type erases only cold start/stop control,
 * never streams, operations, awaiters, or worker-local resources.
 */
class Runtime final {
public:
  template <class Backend>
  class Builder;

  Runtime(const Runtime&) = delete;
  Runtime& operator=(const Runtime&) = delete;

  // Moving preserves the control object's address. As with Start/Stop, it
  // must not race with lifecycle calls on the source or destination object.
  Runtime(Runtime&&) noexcept = default;
  Runtime& operator=(Runtime&&) noexcept = default;
  ~Runtime() noexcept = default;

  [[nodiscard]]
  Result<void> Start() {
    return control_->Start();
  }

  [[nodiscard]]
  Result<void> Run(std::stop_token stop_token) {
    return control_->Run(stop_token);
  }

  void RequestStop() noexcept { control_->RequestStop(); }
  void Stop() noexcept { control_->Stop(); }

  [[nodiscard]]
  bool Started() const noexcept {
    return control_->Started();
  }

  // Default startup path. Omitting Backend selects the platform-native
  // runtime::Auto backend; an explicit backend tag selects that adapter.
  template <class Backend = runtime::Auto, class Handler>
  [[nodiscard]]
  static Runtime Create(net::Endpoint listen_addr, Handler&& handler) {
    return Builder<Backend>{listen_addr}.OnConnection(std::forward<Handler>(handler)).Build();
  }

private:
  template <class>
  friend class Builder;

  explicit Runtime(std::unique_ptr<detail::runtime::RuntimeControl> control) noexcept
      : control_(std::move(control)) {}

  std::unique_ptr<detail::runtime::RuntimeControl> control_;
};

}  // namespace alyrn
