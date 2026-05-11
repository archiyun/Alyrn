#include "runtime/net/timer.h"
#include "runtime/net/timer_rbtree.h"

namespace runtime::net {

bool TimerLess(const Timer *a, const Timer *b) {
  if (a->Expiration() < b->Expiration()) return true;
  if (b->Expiration() < a->Expiration()) return false;
  return a->Sequence() < b->Sequence();
}

} // namespace runtime::net
