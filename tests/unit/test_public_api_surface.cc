// SPDX-License-Identifier: MIT

#include <cstddef>
#include <span>

#include "alyrn/alyrn.h"
#include "alyrn/io.h"
#include "alyrn/net.h"
#include "alyrn/time.h"

namespace {

struct PublicStream {
  alyrn::Task<alyrn::Result<std::size_t>> ReadSome(std::span<std::byte>) {
    co_return std::size_t{0};
  }

  alyrn::Task<alyrn::Result<void>> WriteAll(std::span<const std::byte>) {
    co_return alyrn::Result<void>{};
  }

  alyrn::Task<alyrn::Result<void>> Shutdown() { co_return alyrn::Result<void>{}; }
  alyrn::Task<alyrn::Result<void>> Close() { co_return alyrn::Result<void>{}; }
  alyrn::Task<alyrn::Result<void>> CloseRead() { co_return alyrn::Result<void>{}; }
  alyrn::Task<alyrn::Result<void>> CloseWrite() { co_return alyrn::Result<void>{}; }

  alyrn::Result<alyrn::net::Endpoint> LocalAddr() const {
    return alyrn::net::Endpoint::Loopback(0);
  }

  const alyrn::net::Endpoint& RemoteAddr() const { return peer; }

  alyrn::net::Endpoint peer = alyrn::net::Endpoint::Loopback(1);
};

static_assert(alyrn::io::AsyncStream<PublicStream>);
static_assert(alyrn::io::AsyncReadStream<PublicStream>);
static_assert(alyrn::io::AsyncWriteStream<PublicStream>);

alyrn::Task<int> RootTask() { co_return 42; }
alyrn::DetachedTask RootDetachedTask() { co_return; }

}  // namespace

int main() {
  [[maybe_unused]] auto task = RootTask();
  [[maybe_unused]] auto detached = RootDetachedTask();
  return 0;
}
