#include "runtime/net/timer.h"

namespace runtime::net {

std::atomic<int64_t> Timer::next_sequence_{0};

}  // namespace runtime::net
