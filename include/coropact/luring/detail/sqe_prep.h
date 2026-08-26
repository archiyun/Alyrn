// SPDX-License-Identifier: MIT
#pragma once

#include <liburing.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <utility>

#include <sys/socket.h>
#include <sys/uio.h>

namespace coropact::luring::detail {

// Captures one liburing prep function and its SQE arguments by value. SubmitOp
// invokes the returned closure synchronously, before it publishes the SQE.
// Pointer arguments therefore remain owned by the submitting operation until
// its physical request reaches terminal; this helper never extends them.
template <typename Prep, typename... Args>
[[nodiscard]]
auto MakeSqePrep(Prep&& prep, Args&&... args) {
  return [prep = std::forward<Prep>(prep), ... args = std::forward<Args>(args)](
             io_uring_sqe* sqe) mutable noexcept { std::invoke(prep, sqe, args...); };
}

[[nodiscard]]
inline auto PrepareNop() {
  return MakeSqePrep(io_uring_prep_nop);
}

[[nodiscard]]
inline auto PrepareReadv(int fd, const iovec* iovs, unsigned count, off_t offset) {
  return MakeSqePrep(io_uring_prep_readv, fd, iovs, count, offset);
}

[[nodiscard]]
inline auto PrepareRecv(int fd, void* buffer, std::size_t size) {
  return MakeSqePrep(io_uring_prep_recv, fd, buffer, size, 0);
}

[[nodiscard]]
inline auto PrepareLinkedRecv(int fd, void* buffer, std::size_t size) {
  return [prep = PrepareRecv(fd, buffer, size)](io_uring_sqe* sqe) mutable noexcept {
    prep(sqe);
    sqe->flags |= IOSQE_IO_LINK;
  };
}

[[nodiscard]]
inline auto PrepareSend(int fd, const void* buffer, std::size_t size, int flags) {
  return MakeSqePrep(io_uring_prep_send, fd, buffer, size, flags);
}

[[nodiscard]]
inline auto PrepareSendZeroCopyReportUsage(int fd, const void* buffer, std::size_t size,
                                           int flags) {
  return MakeSqePrep(io_uring_prep_send_zc, fd, buffer, size, flags,
                     IORING_SEND_ZC_REPORT_USAGE);
}

[[nodiscard]]
inline auto PrepareAccept(int fd, sockaddr* address, socklen_t* address_length, int flags) {
  return MakeSqePrep(io_uring_prep_accept, fd, address, address_length, flags);
}

[[nodiscard]]
inline auto PrepareAcceptSource(int fd, bool multishot, int flags) {
  return [fd, multishot, flags](io_uring_sqe* sqe) noexcept {
    if (multishot) {
      io_uring_prep_multishot_accept(sqe, fd, nullptr, nullptr, flags);
    } else {
      io_uring_prep_accept(sqe, fd, nullptr, nullptr, flags);
    }
  };
}

[[nodiscard]]
inline auto PrepareConnect(int fd, const sockaddr* address, socklen_t address_length) {
  return MakeSqePrep(io_uring_prep_connect, fd, address, address_length);
}

[[nodiscard]]
inline auto PrepareCancelAllByUserData(std::uint64_t target) {
  return MakeSqePrep(io_uring_prep_cancel64, target, IORING_ASYNC_CANCEL_ALL);
}

[[nodiscard]]
inline auto PrepareCancelAllByFd(int fd) {
  return MakeSqePrep(io_uring_prep_cancel_fd, fd, IORING_ASYNC_CANCEL_ALL);
}

[[nodiscard]]
inline auto PrepareCancelAll() {
  return MakeSqePrep(io_uring_prep_cancel, nullptr,
                     IORING_ASYNC_CANCEL_ANY | IORING_ASYNC_CANCEL_ALL);
}

[[nodiscard]]
inline auto PrepareLinkTimeout(__kernel_timespec* timeout) {
  return MakeSqePrep(io_uring_prep_link_timeout, timeout, 0);
}

[[nodiscard]]
inline auto PrepareAbsoluteTimeout(__kernel_timespec* timeout) {
  return MakeSqePrep(io_uring_prep_timeout, timeout, 0, IORING_TIMEOUT_ABS);
}

[[nodiscard]]
inline auto PrepareAbsoluteTimeoutUpdate(__kernel_timespec* timeout, std::uint64_t user_data) {
  return MakeSqePrep(io_uring_prep_timeout_update, timeout, user_data, IORING_TIMEOUT_ABS);
}

[[nodiscard]]
inline auto PreparePollAdd(int fd, unsigned poll_mask) {
  return MakeSqePrep(io_uring_prep_poll_add, fd, poll_mask);
}

[[nodiscard]]
[[nodiscard]]
inline auto PrepareProvidedRecvMultishot(int fd, std::size_t buffer_size,
                                         std::uint16_t buffer_group) {
  return [fd, buffer_size, buffer_group](io_uring_sqe* sqe) noexcept {
    io_uring_prep_recv_multishot(sqe, fd, nullptr, buffer_size, 0);
    sqe->flags |= IOSQE_BUFFER_SELECT;
    sqe->buf_group = buffer_group;
  };
}

}  // namespace coropact::luring::detail
