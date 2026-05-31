// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "runtime/uring/uring_loop.h"

#include <liburing.h>

#include <cerrno>

#include "runtime/log/logger.h"
#include "runtime/uring/completion.h"

namespace runtime::uring {

void UringLoop::Loop() {
  quit_.store(false, std::memory_order_relaxed);
  while (!quit_.load(std::memory_order_relaxed)) {
    io_uring_submit(ring_.Raw());

    io_uring_cqe* cqe = nullptr;
    const int rc = io_uring_wait_cqe(ring_.Raw(), &cqe);
    if (rc < 0) {
      if (-rc == EINTR) continue;
      LOG_ERROR() << "io_uring_wait_cqe failed: " << rc;
      break;
    }

    // 不阻塞地把当前已就绪的 CQE 全部排干
    do {
      auto* comp = static_cast<Completion*>(io_uring_cqe_get_data(cqe));
      const int res = cqe->res;
      const unsigned flags = cqe->flags;
      io_uring_cqe_seen(ring_.Raw(), cqe);  // 推进 CQ head
      if (comp && comp->OnComplete) comp->OnComplete(res, flags);

      if (comp && !(flags & IORING_CQE_F_MORE)) delete comp;
    } while (!quit_.load(std::memory_order_relaxed) &&
             io_uring_peek_cqe(ring_.Raw(), &cqe) == 0);
  }
}

void UringLoop::Quit() { quit_.store(true, std::memory_order_relaxed); }

}  // namespace runtime::uring
