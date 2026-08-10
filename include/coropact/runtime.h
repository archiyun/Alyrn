// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <memory>
#include <stop_token>
#include <utility>

#include "coropact/base/error.h"
#include "coropact/net/endpoint.h"

namespace coropact::runtime {

// Backend tags select a Runtime::Builder specialization at compile time. They
// carry no runtime state and never enter an I/O hot path.
struct Reactor final {};
struct LUring final {};

namespace detail {

// Cold lifecycle seam only. Concrete controls remain owned by their backend;
// streams, awaiters, operations, and worker-local state are never erased.
class RuntimeControl {
public:
  virtual ~RuntimeControl() = default;

  virtual base::Result<void> Start() = 0;
  virtual base::Result<void> Run(std::stop_token stop_token) = 0;
  virtual void RequestStop() noexcept = 0;
  virtual void Stop() noexcept = 0;
  [[nodiscard]] virtual bool Started() const noexcept = 0;
};

}  // namespace detail

}  // namespace coropact::runtime

namespace coropact {

// Backend-neutral application composition root. Select a backend through its
// Builder tag; Runtime itself exposes only process lifecycle control.
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

  [[nodiscard]] base::Result<void> Start() { return control_->Start(); }

  [[nodiscard]] base::Result<void> Run(std::stop_token stop_token) {
    return control_->Run(stop_token);
  }

  void RequestStop() noexcept { control_->RequestStop(); }
  void Stop() noexcept { control_->Stop(); }

  [[nodiscard]] bool Started() const noexcept { return control_->Started(); }

  // Default startup path. It deliberately leaves backend selection explicit,
  // while avoiding a Builder for applications that accept default settings.
  template <class Backend, class Handler>
  [[nodiscard]] static Runtime Create(net::Endpoint listen_addr, Handler&& handler) {
    return Builder<Backend>{listen_addr}.OnConnection(std::forward<Handler>(handler)).Build();
  }

private:
  template <class>
  friend class Builder;

  explicit Runtime(std::unique_ptr<runtime::detail::RuntimeControl> control) noexcept
      : control_(std::move(control)) {}

  std::unique_ptr<runtime::detail::RuntimeControl> control_;
};

}  // namespace coropact
