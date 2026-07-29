// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "coropact/luring/capabilities.h"

#include <liburing.h>
#include <liburing/io_uring.h>

#include <cerrno>
#include <expected>

#include "coropact/base/error.h"
#include "coropact/base/try.h"
#include "coropact/io/detail/backend_capability_builder.h"
#include "coropact/io/io_backend.h"
#include "coropact/luring/options.h"

namespace coropact::luring {

namespace {

void Enable(
    coropact::io::BackendCapabilities& set,
    coropact::io::IoCapability capability) noexcept {
  coropact::io::detail::BackendCapabilityBuilder::Enable(set, capability);
}

void EnableCore(coropact::io::BackendCapabilities& set) noexcept {
  Enable(set, coropact::io::IoCapability::kReadSome);
  Enable(set, coropact::io::IoCapability::kWriteSome);
  Enable(set, coropact::io::IoCapability::kAccept);
  Enable(set, coropact::io::IoCapability::kConnect);
  Enable(set, coropact::io::IoCapability::kShutdown);
  Enable(set, coropact::io::IoCapability::kClose);
  Enable(set, coropact::io::IoCapability::kCancelByClose);
  Enable(set, coropact::io::IoCapability::kTimeout);
}

void EnableBasicLuringTags(coropact::io::BackendCapabilities& set) noexcept {
  Enable(set, coropact::io::IoCapability::kSubmitRead);
  Enable(set, coropact::io::IoCapability::kSubmitWrite);
}

bool ProbeSupports(io_uring_probe* probe, unsigned op) noexcept {
  return probe != nullptr && io_uring_opcode_supported(probe, op) != 0;
}

bool ProbeProvidedBufferRing(io_uring* ring, unsigned flags = 0) noexcept {
  int error = 0;
  io_uring_buf_ring* buffer_ring =
      io_uring_setup_buf_ring(ring, 1, 0, flags, &error);
  if (buffer_ring == nullptr) {
    return false;
  }
  return io_uring_free_buf_ring(ring, buffer_ring, 1, 0) == 0;
}

}  // namespace

base::Result<coropact::io::BackendCapabilities> ProbeCapabilities(
    const LUringOptions& options) noexcept {
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
    return std::unexpected(base::MakeNegErrno(result));
  }

  io_uring_probe* probe = io_uring_get_probe_ring(&ring);
  if (probe == nullptr) {
    io_uring_queue_exit(&ring);
    return std::unexpected(base::MakeErrno(ENOTSUP));
  }

  auto caps = coropact::io::detail::BackendCapabilityBuilder::Create(
      coropact::io::Backend::kLuring);
  EnableCore(caps);
  EnableBasicLuringTags(caps);

  if (options.setup_sqpoll) {
    Enable(caps, coropact::io::IoCapability::kSqPoll);
  }
  if (options.setup_iopoll) {
    Enable(caps, coropact::io::IoCapability::kIoPoll);
  }

  if (ProbeSupports(probe, IORING_OP_PROVIDE_BUFFERS) ||
      ProbeSupports(probe, IORING_OP_REMOVE_BUFFERS)) {
    Enable(caps, coropact::io::IoCapability::kProvidedBuffer);
  }

  if (ProbeProvidedBufferRing(&ring)) {
    Enable(caps, coropact::io::IoCapability::kProvidedBufferRing);
    if (ProbeProvidedBufferRing(&ring, IOU_PBUF_RING_INC)) {
      Enable(caps, coropact::io::IoCapability::kProvidedBufferRingIncremental);
    }
  }

  if (ProbeSupports(probe, IORING_OP_ACCEPT)) {
    Enable(caps, coropact::io::IoCapability::kMultishotAccept);
  }

  if (ProbeSupports(probe, IORING_OP_MSG_RING)) {
    Enable(caps, coropact::io::IoCapability::kMsgRing);
  }
#ifdef IORING_OP_RECV
  if (ProbeSupports(probe, IORING_OP_RECV)) {
    Enable(caps, coropact::io::IoCapability::kMultishotRecv);
  }
#endif

#ifdef IORING_OP_SEND_ZC
  if (ProbeSupports(probe, IORING_OP_SEND_ZC)) {
    Enable(caps, coropact::io::IoCapability::kSendZeroCopy);
  }
#endif

  Enable(caps, coropact::io::IoCapability::kLinkedOps);
  io_uring_free_probe(probe);
  io_uring_queue_exit(&ring);
  return caps;
}

base::Result<coropact::io::BackendBinding> BindLUring(
    const LUringOptions& options, coropact::io::CapabilitySet active_profile) noexcept {
  auto caps = COROPACT_TRY(ProbeCapabilities(options));
  return coropact::io::BindBackend(coropact::io::Backend::kLuring, caps, active_profile);
}

}  // namespace coropact::luring
