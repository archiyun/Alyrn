// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
// The shared compile-time seam for backend-neutral network contracts.

#include <iostream>

#include "async_io_contracts.h"
#include "coropact/reactor/reactor_connect.h"
#include "coropact/reactor/reactor_listener.h"
#include "coropact/reactor/reactor_stream.h"

#if defined(COROPACT_ENABLE_URING)
#include "coropact/luring/connector.h"
#include "coropact/luring/listener.h"
#include "coropact/luring/stream.h"
#endif

namespace {

using coropact::test::contracts::AssertCoreConnector;
using coropact::test::contracts::AssertCoreListener;
using coropact::test::contracts::AssertCoreStream;

consteval bool CheckReactorContracts() {
  AssertCoreStream<coropact::reactor::ReactorStream>();
  AssertCoreListener<coropact::reactor::ReactorListener>();
  AssertCoreConnector<coropact::reactor::ReactorConnector>();
  return true;
}

static_assert(CheckReactorContracts());

#if defined(COROPACT_ENABLE_URING)
consteval bool CheckLuringContracts() {
  AssertCoreStream<coropact::luring::LUringStream>();
  AssertCoreListener<coropact::luring::LUringListener>();
  AssertCoreConnector<coropact::luring::LUringConnector>();
  return true;
}

static_assert(CheckLuringContracts());
#endif

}  // namespace

int main() {
  std::cout << "backend-neutral I/O contracts: PASS\n";
  return 0;
}
