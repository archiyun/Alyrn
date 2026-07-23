// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "coropact/luring/capabilities.h"

#include <liburing.h>
#include <liburing/io_uring.h>

#include <cerrno>
#include <expected>

#include "coropact/base/error.h"
#include "coropact/io/io_backend.h"
#include "coropact/luring/options.h"

namespace coropact::luring {

namespace {

void EnableCore(coropact::io::CapabilitySet& set) noexcept {
  set.Enable(coropact::io::IoCapability::kReadSome);
  set.Enable(coropact::io::IoCapability::kWriteSome);
  set.Enable(coropact::io::IoCapability::kAccept);
  set.Enable(coropact::io::IoCapability::kConnect);
  set.Enable(coropact::io::IoCapability::kShutdown);
  set.Enable(coropact::io::IoCapability::kClose);
  set.Enable(coropact::io::IoCapability::kCancelByClose);
  set.Enable(coropact::io::IoCapability::kTimeout);
}

void EnableBasicLuringTags(coropact::io::CapabilitySet& set) noexcept {
  set.Enable(coropact::io::IoCapability::kSubmitRead);
  set.Enable(coropact::io::IoCapability::kSubmitWrite);
}

bool ProbeSupports(io_uring_probe* probe, unsigned op) noexcept {
  return probe != nullptr && io_uring_opcode_supported(probe, op) != 0;
}

}  // namespace

base::Result<coropact::io::CapabilitySet> ProbeCapabilities(const LUringOptions& options) noexcept {
  io_uring ring{};
  io_uring_params params{};

  params.flags |= IORING_SETUP_CLAMP;
  if (options.setup_submit_all) {
    params.flags |= IORING_SETUP_SUBMIT_ALL;
  }

  if (options.setup_single_issuer) {
    params.flags |= IORING_SETUP_SINGLE_ISSUER;
  }

  if (options.setup_defer_taskrun) {
    params.flags |= IORING_SETUP_COOP_TASKRUN;
    params.flags |= IORING_SETUP_TASKRUN_FLAG;
    params.flags |= IORING_SETUP_DEFER_TASKRUN;
  }

  if (options.cq_entries != 0) {
    params.flags |= IORING_SETUP_CQSIZE;
    params.cq_entries = options.cq_entries;
  }

  if (options.setup_sqpoll) {
    params.flags |= IORING_SETUP_SQPOLL;
    params.sq_thread_idle = options.sqpoll_idle_ms;
  }

  if (options.setup_iopoll) {
    params.flags |= IORING_SETUP_IOPOLL;
  }

  int result = io_uring_queue_init_params(options.entries, &ring, &params);
  if (result < 0) {
    return std::unexpected(base::make_neg_errno(result));
  }

  io_uring_probe* probe = io_uring_get_probe_ring(&ring);
  if (probe == nullptr) {
    io_uring_queue_exit(&ring);
    return std::unexpected(base::make_errno(ENOTSUP));
  }

  coropact::io::CapabilitySet caps;
  EnableCore(caps);
  EnableBasicLuringTags(caps);

  if (options.setup_sqpoll) {
    caps.Enable(coropact::io::IoCapability::kSqPoll);
  }
  if (options.setup_iopoll) {
    caps.Enable(coropact::io::IoCapability::kIoPoll);
  }

  if (ProbeSupports(probe, IORING_OP_PROVIDE_BUFFERS) ||
      ProbeSupports(probe, IORING_OP_REMOVE_BUFFERS)) {
    caps.Enable(coropact::io::IoCapability::kProvidedBuffer);
  }

  if (ProbeSupports(probe, IORING_OP_ACCEPT)) {
    caps.Enable(coropact::io::IoCapability::kMultishotAccept);
  }

  if (ProbeSupports(probe, IORING_OP_MSG_RING)) {
    caps.Enable(coropact::io::IoCapability::kMsgRing);
  }
#ifdef IORING_OP_RECV
  if (ProbeSupports(probe, IORING_OP_RECV)) {
    caps.Enable(coropact::io::IoCapability::kMultishotRecv);
  }
#endif

#ifdef IORING_OP_SEND_ZC
  if (ProbeSupports(probe, IORING_OP_SEND_ZC)) {
    caps.Enable(coropact::io::IoCapability::kSendZeroCopy);
  }
#endif

  caps.Enable(coropact::io::IoCapability::kLinkedOps);
  io_uring_free_probe(probe);
  io_uring_queue_exit(&ring);
  return caps;
}

base::Result<coropact::io::BackendBinding> BindLUring(const LUringOptions& options,
                                                  coropact::io::CapabilitySet active_profile) noexcept {
  auto caps = ProbeCapabilities(options);
  if (!caps.has_value()) {
    return std::unexpected(caps.error());
  }
  return coropact::io::BindBackend(coropact::io::Backend::kLuring, *caps, active_profile);
}

}  // namespace coropact::luring
