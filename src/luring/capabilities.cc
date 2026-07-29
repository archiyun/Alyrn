// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "coropact/luring/capabilities.h"

#include <liburing.h>
#include <liburing/io_uring.h>

#include <cerrno>
#include <expected>
#include <utility>

#include "coropact/base/error.h"
#include "coropact/base/try.h"
#include "coropact/luring/detail/capability_builder.h"
#include "coropact/luring/options.h"

namespace coropact::luring {

namespace {

void Enable(
    Capabilities& set,
    NativeFeature feature) noexcept {
  detail::CapabilityBuilder::Enable(set, feature);
}

void EnableCore(Capabilities& set) noexcept {
  Enable(set, NativeFeature::kSubmitRead);
  Enable(set, NativeFeature::kSubmitWrite);
}

bool ProbeSupports(io_uring_probe* probe, unsigned op) noexcept {
  return probe != nullptr && io_uring_opcode_supported(probe, op) != 0;
}

bool ProbeProvidedBufferRing(
    io_uring* ring,
    unsigned flags,
    unsigned buffer_group) noexcept {
  int error = 0;
  io_uring_buf_ring* buffer_ring =
      io_uring_setup_buf_ring(ring, 1, buffer_group, flags, &error);
  if (buffer_ring == nullptr) {
    return false;
  }
  return io_uring_free_buf_ring(ring, buffer_ring, 1, buffer_group) == 0;
}

}  // namespace

base::Result<Capabilities> ProbeCapabilities(
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

  auto caps = detail::CapabilityBuilder::Create();
  EnableCore(caps);

  if (options.setup_sqpoll) {
    Enable(caps, NativeFeature::kSqPoll);
  }
  if (options.setup_iopoll) {
    Enable(caps, NativeFeature::kIoPoll);
  }

  if (ProbeSupports(probe, IORING_OP_PROVIDE_BUFFERS) ||
      ProbeSupports(probe, IORING_OP_REMOVE_BUFFERS)) {
    Enable(caps, NativeFeature::kProvidedBuffer);
  }

  if (ProbeProvidedBufferRing(&ring, 0, 1)) {
    Enable(caps, NativeFeature::kProvidedBufferRing);
    if (ProbeProvidedBufferRing(&ring, IOU_PBUF_RING_INC, 2)) {
      Enable(caps, NativeFeature::kProvidedBufferRingIncremental);
    }
  }

  if (ProbeSupports(probe, IORING_OP_ACCEPT)) {
    Enable(caps, NativeFeature::kMultishotAccept);
  }

  if (ProbeSupports(probe, IORING_OP_MSG_RING)) {
    Enable(caps, NativeFeature::kMsgRing);
  }
  if (ProbeSupports(probe, IORING_OP_RECV)) {
    Enable(caps, NativeFeature::kMultishotRecv);
  }

  if (ProbeSupports(probe, IORING_OP_SEND_ZC)) {
    Enable(caps, NativeFeature::kSendZeroCopy);
  }

  Enable(caps, NativeFeature::kLinkedOps);
  io_uring_free_probe(probe);
  io_uring_queue_exit(&ring);
  return caps;
}

base::Result<RuntimeBinding> BindLUring(
    const LUringOptions& options, RuntimeProfile active_profile) noexcept {
  auto caps = COROPACT_TRY(ProbeCapabilities(options));
  return BindCapabilities(std::move(caps), active_profile);
}

}  // namespace coropact::luring
