// SPDX-License-Identifier: MIT
// The shared compile-time seam for backend-neutral network contracts.

#include <iostream>

#include "async_io_contracts.h"
#include "coropact/coro.h"
#include "coropact/io.h"
#include "coropact/net.h"
#include "coropact/reactor.h"
#include "coropact/io/buffer.h"
#include "coropact/io/loop.h"
#include "coropact/reactor/connector.h"
#include "coropact/reactor/listener.h"
#include "coropact/reactor/loop.h"
#include "coropact/reactor/stream.h"

#if defined(COROPACT_ENABLE_URING)
#include "coropact/luring/connector.h"
#include "coropact/luring/listener.h"
#include "coropact/luring/loop.h"
#include "coropact/luring/stream.h"
#endif

namespace {

using coropact::test::contracts::AssertCoreConnector;
using coropact::test::contracts::AssertCoreListener;
using coropact::test::contracts::AssertCoreStream;

template <class T>
concept HasBorrowedBufferRead =
    requires(T& stream, coropact::io::Buffer& buffer) { stream.ReadSome(buffer, 4096); };

consteval bool CheckReactorContracts() {
  AssertCoreStream<coropact::reactor::Stream>();
  AssertCoreListener<coropact::reactor::Listener>();
  AssertCoreConnector<coropact::reactor::Connector>();
  return true;
}

static_assert(CheckReactorContracts());
static_assert(coropact::io::ManagedLoop<coropact::reactor::Loop>);
static_assert(!HasBorrowedBufferRead<coropact::reactor::Stream>);

#if defined(COROPACT_ENABLE_URING)
consteval bool CheckLuringContracts() {
  AssertCoreStream<coropact::luring::Stream>();
  AssertCoreListener<coropact::luring::Listener>();
  AssertCoreConnector<coropact::luring::Connector>();
  return true;
}

static_assert(CheckLuringContracts());
static_assert(coropact::io::ManagedLoop<coropact::luring::Loop>);
static_assert(!HasBorrowedBufferRead<coropact::luring::Stream>);
#endif

}  // namespace

int main() {
  std::cout << "backend-neutral I/O contracts: PASS\n";
  return 0;
}
