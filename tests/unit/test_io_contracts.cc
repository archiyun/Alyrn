// SPDX-License-Identifier: MIT
// The shared compile-time seam for backend-neutral network contracts.

#include <iostream>

#include "async_io_contracts.h"
#include "alyrn/coro.h"
#include "alyrn/io.h"
#include "alyrn/net.h"
#include "alyrn/reactor.h"
#include "alyrn/io/buffer.h"
#include "alyrn/io/loop.h"
#include "alyrn/reactor/connector.h"
#include "alyrn/reactor/listener.h"
#include "alyrn/reactor/loop.h"
#include "alyrn/reactor/stream.h"

#if defined(ALYRN_ENABLE_URING)
#include "alyrn/luring/connector.h"
#include "alyrn/luring/listener.h"
#include "alyrn/luring/loop.h"
#include "alyrn/luring/stream.h"
#endif

namespace {

using alyrn::test::contracts::AssertCoreConnector;
using alyrn::test::contracts::AssertCoreListener;
using alyrn::test::contracts::AssertCoreStream;

template <class T>
concept HasBorrowedBufferRead =
    requires(T& stream, alyrn::io::Buffer& buffer) { stream.ReadSome(buffer, 4096); };

consteval bool CheckReactorContracts() {
  AssertCoreStream<alyrn::reactor::Stream>();
  AssertCoreListener<alyrn::reactor::Listener>();
  AssertCoreConnector<alyrn::reactor::Connector>();
  return true;
}

static_assert(CheckReactorContracts());
static_assert(alyrn::io::ManagedLoop<alyrn::reactor::Loop>);
static_assert(!HasBorrowedBufferRead<alyrn::reactor::Stream>);

#if defined(ALYRN_ENABLE_URING)
consteval bool CheckLuringContracts() {
  AssertCoreStream<alyrn::luring::Stream>();
  AssertCoreListener<alyrn::luring::Listener>();
  AssertCoreConnector<alyrn::luring::Connector>();
  return true;
}

static_assert(CheckLuringContracts());
static_assert(alyrn::io::ManagedLoop<alyrn::luring::Loop>);
static_assert(!HasBorrowedBufferRead<alyrn::luring::Stream>);
#endif

}  // namespace

int main() {
  std::cout << "backend-neutral I/O contracts: PASS\n";
  return 0;
}
