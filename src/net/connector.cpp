#include "runtime/net/connector.h"
#include "runtime/net/channel.h"
#include "runtime/net/net_utils.h"
#include "runtime/log/logger.h"

#include <cassert>
#include <cerrno>
#include <algorithm>
#include <cstring>
#include <sys/socket.h>
#include <unistd.h>

namespace runtime::net {

static constexpr double kMaxRetryDelaySec = 30.0;

Connector::Connector(EventLoop* loop, const InetAddress& server_addr)
  : loop_(loop), server_addr_(server_addr) {}

Connector::~Connector() {
  assert(!channel_);
}
}  // namespace runtime::net
