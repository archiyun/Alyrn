// SPDX-License-Identifier: MIT
// The shared compile-time seam for backend-neutral network contracts.

#include <iostream>

#include "alyrn/coro.h"
#include "alyrn/epoll.h"
#include "alyrn/epoll/connector.h"
#include "alyrn/epoll/listener.h"
#include "alyrn/epoll/loop.h"
#include "alyrn/epoll/stream.h"
#include "alyrn/io.h"
#include "alyrn/io/buffer.h"
#include "alyrn/io/loop.h"
#include "alyrn/net.h"
#include "async_io_contracts.h"

#if defined(ALYRN_ENABLE_URING)
#include "alyrn/uring/connector.h"
#include "alyrn/uring/listener.h"
#include "alyrn/uring/loop.h"
#include "alyrn/uring/stream.h"
#endif

namespace {

using alyrn::test::contracts::AssertCoreConnector;
using alyrn::test::contracts::AssertCoreListener;
using alyrn::test::contracts::AssertCoreStream;

template <class T>
concept HasBorrowedBufferRead =
    requires(T& stream, alyrn::io::Buffer& buffer) { stream.Read(buffer, 4096); };

consteval bool CheckEpollContracts() {
  AssertCoreStream<alyrn::epoll::Stream>();
  AssertCoreListener<alyrn::epoll::Listener>();
  AssertCoreConnector<alyrn::epoll::Connector>();
  return true;
}

static_assert(CheckEpollContracts());
static_assert(alyrn::io::ManagedLoop<alyrn::epoll::Loop>);
static_assert(!HasBorrowedBufferRead<alyrn::epoll::Stream>);

#if defined(ALYRN_ENABLE_URING)
consteval bool CheckLuringContracts() {
  AssertCoreStream<alyrn::uring::Stream>();
  AssertCoreListener<alyrn::uring::Listener>();
  AssertCoreConnector<alyrn::uring::Connector>();
  return true;
}

static_assert(CheckLuringContracts());
static_assert(alyrn::io::ManagedLoop<alyrn::uring::Loop>);
static_assert(!HasBorrowedBufferRead<alyrn::uring::Stream>);
#endif

}  // namespace

int main() {
  std::cout << "backend-neutral I/O contracts: PASS\n";
  return 0;
}
