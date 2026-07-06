#include "vexo/luring/capabilities.h"

#include <liburing.h>
#include <liburing/io_uring.h>

#include <cerrno>
#include <expected>

#include "vexo/base/error.h"
#include "vexo/io/io_backend.h"
#include "vexo/luring/options.h"

namespace vexo::luring {

namespace {

void EnableCore(vexo::io::CapabilitySet& set) noexcept {
  set.Enable(vexo::io::IoCapability::kReadSome);
  set.Enable(vexo::io::IoCapability::kWriteSome);
  set.Enable(vexo::io::IoCapability::kAccept);
  set.Enable(vexo::io::IoCapability::kConnect);
  set.Enable(vexo::io::IoCapability::kShutdown);
  set.Enable(vexo::io::IoCapability::kClose);
  set.Enable(vexo::io::IoCapability::kCancelByClose);
  set.Enable(vexo::io::IoCapability::kTimeout);
}

void EnableBasicLuringTags(vexo::io::CapabilitySet& set) noexcept {
  set.Enable(vexo::io::IoCapability::kSubmitRead);
  set.Enable(vexo::io::IoCapability::kSubmitWrite);
}

bool ProbeSupports(io_uring_probe* probe, unsigned op) noexcept {
  return probe != nullptr && io_uring_opcode_supported(probe, op) != 0;
}

}  // namespace

base::Result<vexo::io::CapabilitySet> ProbeCapabilities(const LUringOptions& options) noexcept {
  io_uring ring{};
  io_uring_params params{};

  params.flags |= IORING_SETUP_CLAMP;
  if (options.setup_submit_all) {
    params.flags |= IORING_SETUP_SUBMIT_ALL;
  }

  if (options.setup_single_issuer) {
    params.flags |= IORING_SETUP_SINGLE_ISSUER;
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

  int r = io_uring_queue_init_params(options.entries, &ring, &params);
  if (r < 0) {
    return std::unexpected(base::make_neg_errno(r));
  }

  io_uring_probe* probe = io_uring_get_probe_ring(&ring);
  if (probe == nullptr) {
    io_uring_queue_exit(&ring);
    return std::unexpected(base::make_errno(ENOTSUP));
  }

  vexo::io::CapabilitySet caps;
  EnableCore(caps);
  EnableBasicLuringTags(caps);

  if (options.setup_sqpoll) {
    caps.Enable(vexo::io::IoCapability::kSqPoll);
  }
  if (options.setup_iopoll) {
    caps.Enable(vexo::io::IoCapability::kIoPoll);
  }

  if (ProbeSupports(probe, IORING_OP_PROVIDE_BUFFERS) ||
      ProbeSupports(probe, IORING_OP_REMOVE_BUFFERS)) {
    caps.Enable(vexo::io::IoCapability::kProvidedBuffer);
  }

  if (ProbeSupports(probe, IORING_OP_ACCEPT)) {
    caps.Enable(vexo::io::IoCapability::kMultishotAccept);
  }
#ifdef IORING_OP_RECV
  if (ProbeSupports(probe, IORING_OP_RECV)) {
    caps.Enable(vexo::io::IoCapability::kMultishotRecv);
  }
#endif

#ifdef IORING_OP_SEND_ZC
  if (ProbeSupports(probe, IORING_OP_SEND_ZC)) {
    caps.Enable(vexo::io::IoCapability::kSendZeroCopy);
  }
#endif

  caps.Enable(vexo::io::IoCapability::kLinkedOps);
  io_uring_free_probe(probe);
  io_uring_queue_exit(&ring);
  return caps;
}

base::Result<vexo::io::BackendBinding> BindLUring(const LUringOptions& options,
                                                  vexo::io::CapabilitySet active_profile) noexcept {
  auto caps = ProbeCapabilities(options);
  if (!caps.has_value()) {
    return std::unexpected(caps.error());
  }
  return vexo::io::BindBackend(vexo::io::Backend::kLuring, *caps, active_profile);
}
}  // namespace vexo::luring
