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
struct Reactor final {};
struct LUring final {};
struct Kqueue final {};

namespace detail {

// Cold lifecycle seam only. Concrete controls remain owned by their backend;
// streams, awaiters, operations, and worker-local state are never erased.
class RuntimeControl {
public:
  virtual ~RuntimeControl() = default;

  virtual Result<void> Start() = 0;
  virtual Result<void> Run(std::stop_token stop_token) = 0;
  virtual void RequestStop() noexcept = 0;
  virtual void Stop() noexcept = 0;
  [[nodiscard]]
  virtual bool Started() const noexcept = 0;
};

}  // namespace detail

}  // namespace alyrn::runtime

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

  // Default startup path. It deliberately leaves backend selection explicit,
  // while avoiding a Builder for applications that accept default settings.
  template <class Backend, class Handler>
  [[nodiscard]]
  static Runtime Create(net::Endpoint listen_addr, Handler&& handler) {
    return Builder<Backend>{listen_addr}.OnConnection(std::forward<Handler>(handler)).Build();
  }

private:
  template <class>
  friend class Builder;

  explicit Runtime(std::unique_ptr<runtime::detail::RuntimeControl> control) noexcept
      : control_(std::move(control)) {}

  std::unique_ptr<runtime::detail::RuntimeControl> control_;
};

}  // namespace alyrn
