// SPDX-License-Identifier: MIT

/*
 * Native Stream smoke against TriggerMode::kOneShot.
 */

#include <sys/socket.h>
#include <sys/uio.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstring>
#include <expected>
#include <iostream>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "alyrn/result.h"
#include "alyrn/coro/scheduler.h"
#include "alyrn/coro/spawn.h"
#include "alyrn/coro/sync_wait.h"
#include "alyrn/coro/task.h"
#include "alyrn/coro/work.h"
#include "alyrn/io/async_stream.h"
#include "alyrn/io/buffer.h"
#include "alyrn/io/read_into.h"
#include "alyrn/kqueue/loop.h"
#include "alyrn/kqueue/stream.h"
#include "alyrn/net/detail/socket.h"
#include "alyrn/time/clock.h"

namespace {

using ReadResult = alyrn::Result<std::size_t>;
using WriteResult = alyrn::Result<void>;
using OwnedReadOutcome = alyrn::io::ReadIntoOutcome;

static_assert(alyrn::io::AsyncStream<alyrn::kqueue::Stream>);
static_assert(alyrn::io::AsyncReadIntoStream<alyrn::kqueue::Stream>);

bool Check(bool condition, const char* message) {
  if (!condition) {
    std::cout << "FAIL: " << message << '\n';
    return false;
  }
  return true;
}

bool MakeSocketPair(int sv[2]) {
  if (::socketpair(AF_UNIX, SOCK_STREAM, 0, sv) != 0) {
    return false;
  }
  return alyrn::net::SetNonBlocking(sv[0]).has_value() &&
         alyrn::net::SetNonBlocking(sv[1]).has_value() &&
         alyrn::net::SetCloseOnExec(sv[0]).has_value() &&
         alyrn::net::SetCloseOnExec(sv[1]).has_value();
}

std::string Gather(alyrn::io::Buffer& buffer) {
  std::string out;
  for (const iovec& iov : buffer.ReadableIov(32)) {
    out.append(static_cast<const char*>(iov.iov_base), iov.iov_len);
  }
  return out;
}

alyrn::coro::DetachedTask ReadOnce(alyrn::kqueue::Stream* stream,
                                      alyrn::kqueue::Loop* loop,
                                      alyrn::kqueue::Loop* scheduler,
                                      std::array<std::byte, 16>* buffer,
                                      std::optional<ReadResult>* out,
                                      bool* resumed_with_scheduler) {
  ReadResult result = co_await stream->ReadSome(*buffer);
  *resumed_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == scheduler;
  out->emplace(std::move(result));
  loop->RequestStop();
}

alyrn::coro::DetachedTask ReadWithoutQuit(alyrn::kqueue::Stream* stream,
                                             alyrn::kqueue::Loop* scheduler,
                                             std::array<std::byte, 16>* buffer,
                                             std::optional<ReadResult>* out, int* resume_count,
                                             bool* resumed_with_scheduler) {
  ReadResult result = co_await stream->ReadSome(*buffer);
  ++*resume_count;
  *resumed_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == scheduler;
  out->emplace(std::move(result));
}

alyrn::coro::DetachedTask ReadIntoOnce(alyrn::kqueue::Stream* stream,
                                          alyrn::kqueue::Loop* loop,
                                          alyrn::kqueue::Loop* scheduler,
                                          alyrn::net::Buffer buffer,
                                          std::optional<OwnedReadOutcome>* out,
                                          bool* resumed_with_scheduler) {
  OwnedReadOutcome outcome = co_await stream->ReadInto(std::move(buffer), 32);
  *resumed_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == scheduler;
  out->emplace(std::move(outcome));
  loop->RequestStop();
}

alyrn::coro::DetachedTask WriteOnce(alyrn::kqueue::Stream* stream,
                                       alyrn::kqueue::Loop* loop,
                                       alyrn::kqueue::Loop* scheduler,
                                       std::span<const std::byte> payload,
                                       std::optional<WriteResult>* out,
                                       bool* resumed_with_scheduler) {
  WriteResult result = co_await stream->WriteAll(payload);
  *resumed_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == scheduler;
  out->emplace(std::move(result));
  loop->RequestStop();
}

alyrn::coro::DetachedTask EchoServer(alyrn::kqueue::Stream* stream,
                                        std::array<std::byte, 64>* scratch,
                                        std::optional<alyrn::Result<void>>* out,
                                        int* done_count, alyrn::kqueue::Loop* loop) {
  ReadResult read_result = co_await stream->ReadSome(*scratch);
  if (!read_result.has_value()) {
    out->emplace(std::unexpected(read_result.error()));
  } else if (*read_result == 0) {
    out->emplace(alyrn::Result<void>{});
  } else {
    out->emplace(
        co_await stream->WriteAll(std::span<const std::byte>(scratch->data(), *read_result)));
  }
  if (++(*done_count) == 2) {
    loop->RequestStop();
  }
}

alyrn::coro::DetachedTask EchoClient(alyrn::kqueue::Stream* stream,
                                        std::span<const std::byte> payload,
                                        std::array<std::byte, 64>* received,
                                        std::optional<alyrn::Result<void>>* out,
                                        std::size_t* received_size, int* done_count,
                                        alyrn::kqueue::Loop* loop) {
  alyrn::Result<void> write_result = co_await stream->WriteAll(payload);
  if (!write_result.has_value()) {
    out->emplace(std::unexpected(write_result.error()));
  } else {
    ReadResult read_result = co_await stream->ReadSome(*received);
    if (!read_result.has_value()) {
      out->emplace(std::unexpected(read_result.error()));
    } else {
      *received_size = *read_result;
      out->emplace(co_await stream->Shutdown());
    }
  }

  if (++(*done_count) == 2) {
    loop->RequestStop();
  }
}

alyrn::coro::DetachedTask CloseThenSubmit(alyrn::kqueue::Stream* stream,
                                             alyrn::kqueue::Loop* loop,
                                             std::array<std::byte, 16>* read_buffer,
                                             std::span<const std::byte> write_buffer,
                                             std::optional<ReadResult>* read_result,
                                             std::optional<WriteResult>* write_result) {
  alyrn::Result<void> close_result = co_await stream->Close();
  if (!close_result.has_value()) {
    read_result->emplace(std::unexpected(close_result.error()));
    write_result->emplace(std::unexpected(close_result.error()));
  } else {
    read_result->emplace(co_await stream->ReadSome(*read_buffer));
    write_result->emplace(co_await stream->WriteAll(write_buffer));
  }
  loop->RequestStop();
}

alyrn::coro::DetachedTask ShutdownThenReadAndWrite(
    alyrn::kqueue::Stream* stream, alyrn::kqueue::Loop* loop,
    std::array<std::byte, 16>* read_buffer, std::span<const std::byte> write_buffer,
    std::optional<WriteResult>* first_shutdown, std::optional<WriteResult>* second_shutdown,
    std::optional<WriteResult>* write_result, std::optional<ReadResult>* read_result) {
  first_shutdown->emplace(co_await stream->CloseWrite());
  second_shutdown->emplace(co_await stream->Shutdown());
  write_result->emplace(co_await stream->WriteAll(write_buffer));
  read_result->emplace(co_await stream->ReadSome(*read_buffer));
  loop->RequestStop();
}

alyrn::coro::DetachedTask CloseReadThenReadAndWrite(
    alyrn::kqueue::Stream* stream, alyrn::kqueue::Loop* loop,
    std::array<std::byte, 16>* read_buffer, std::span<const std::byte> write_buffer,
    std::optional<WriteResult>* first_close_read, std::optional<WriteResult>* second_close_read,
    std::optional<WriteResult>* write_result, std::optional<ReadResult>* read_result) {
  first_close_read->emplace(co_await stream->CloseRead());
  second_close_read->emplace(co_await stream->CloseRead());
  read_result->emplace(co_await stream->ReadSome(*read_buffer));
  write_result->emplace(co_await stream->WriteAll(write_buffer));
  loop->RequestStop();
}

alyrn::coro::Task<ReadResult> ReadFromForeignLoopThread(alyrn::kqueue::Stream* stream,
                                                           std::array<std::byte, 16>* buffer) {
  co_return co_await stream->ReadSome(*buffer);
}

class PeerWriteWork final : public alyrn::coro::Work {
public:
  PeerWriteWork(int fd, std::string_view payload) noexcept : fd_(fd), payload_(payload) {
    SetRun(&RunWrite);
  }

private:
  static void RunWrite(alyrn::coro::Work* work) noexcept {
    auto* self = static_cast<PeerWriteWork*>(work);
    (void)::write(self->fd_, self->payload_.data(), self->payload_.size());
  }

  int fd_;
  std::string_view payload_;
};

class CloseStreamWork final : public alyrn::coro::Work {
public:
  CloseStreamWork(alyrn::kqueue::Loop* loop, alyrn::kqueue::Stream* stream) noexcept
      : loop_(loop), stream_(stream) {
    SetRun(&RunClose);
  }

private:
  static void RunClose(alyrn::coro::Work* work) noexcept {
    auto* self = static_cast<CloseStreamWork*>(work);
    alyrn::coro::Spawn(*self->loop_, self->stream_->Close()).Detach();
  }

  alyrn::kqueue::Loop* loop_;
  alyrn::kqueue::Stream* stream_;
};

bool CheckForeignLoopReadTerminates() {
  int sv[2] = {-1, -1};
  if (!MakeSocketPair(sv)) {
    std::cout << "FAIL: socketpair failed for owner-loop check\n";
    return false;
  }

  alyrn::kqueue::Loop loop;
  alyrn::kqueue::Stream stream(&loop, sv[0]);
  const pid_t child = ::fork();
  if (child < 0) {
    ::close(sv[1]);
    return false;
  }
  if (child == 0) {
    ::close(sv[1]);
    ::alarm(3);
    std::thread foreign_thread([&stream] {
      std::array<std::byte, 16> buffer{};
      static_cast<void>(alyrn::coro::SyncWait(ReadFromForeignLoopThread(&stream, &buffer)));
    });
    foreign_thread.join();
    ::_exit(0);
  }

  ::close(sv[1]);
  int status = 0;
  while (::waitpid(child, &status, 0) < 0 && errno == EINTR) {
  }
  return Check(WIFSIGNALED(status), "foreign-loop stream operation did not terminate") &&
         Check(WTERMSIG(status) == SIGABRT,
               "foreign-loop stream operation must terminate through ALYRN_CHECK");
}

bool CheckImmediateRead() {
  int sv[2] = {-1, -1};
  if (!MakeSocketPair(sv)) {
    std::cout << "FAIL: socketpair failed\n";
    return false;
  }

  const char payload[] = "abc";
  if (::write(sv[1], payload, sizeof(payload) - 1) != static_cast<ssize_t>(sizeof(payload) - 1)) {
    std::cout << "FAIL: initial write failed\n";
    ::close(sv[0]);
    ::close(sv[1]);
    return false;
  }

  alyrn::kqueue::Loop loop;
  alyrn::kqueue::Stream stream(&loop, sv[0]);

  std::array<std::byte, 16> buffer{};
  std::optional<ReadResult> result;
  bool resumed_with_scheduler = false;

  alyrn::coro::SpawnDetach(
      loop, ReadOnce(&stream, &loop, &loop, &buffer, &result, &resumed_with_scheduler));
  loop.Run();

  ::close(sv[1]);

  return Check(result.has_value(), "immediate read did not finish") &&
         Check(result->has_value(), "immediate read returned error") &&
         Check(**result == sizeof(payload) - 1, "immediate read byte count mismatch") &&
         Check(std::memcmp(buffer.data(), payload, sizeof(payload) - 1) == 0,
               "immediate read payload mismatch") &&
         Check(resumed_with_scheduler, "immediate read resumed without current scheduler");
}

bool CheckImmediateWrite() {
  int sv[2] = {-1, -1};
  if (!MakeSocketPair(sv)) {
    std::cout << "FAIL: socketpair failed\n";
    return false;
  }

  alyrn::kqueue::Loop loop;
  alyrn::kqueue::Stream stream(&loop, sv[0]);

  const char payload[] = "write";
  auto bytes = std::as_bytes(std::span(payload, sizeof(payload) - 1));
  std::optional<WriteResult> result;
  bool resumed_with_scheduler = false;

  alyrn::coro::SpawnDetach(
      loop, WriteOnce(&stream, &loop, &loop, bytes, &result, &resumed_with_scheduler));
  loop.Run();

  std::array<char, 16> received{};
  const ssize_t n = ::read(sv[1], received.data(), received.size());
  ::close(sv[1]);

  return Check(result.has_value(), "immediate write did not finish") &&
         Check(result->has_value(), "immediate write returned error") &&
         Check(n == static_cast<ssize_t>(sizeof(payload) - 1), "peer read byte count mismatch") &&
         Check(std::memcmp(received.data(), payload, sizeof(payload) - 1) == 0,
               "peer read payload mismatch") &&
         Check(resumed_with_scheduler, "immediate write resumed without current scheduler");
}

bool CheckPendingRead() {
  int sv[2] = {-1, -1};
  if (!MakeSocketPair(sv)) {
    std::cout << "FAIL: socketpair failed\n";
    return false;
  }

  alyrn::kqueue::Loop loop;
  alyrn::kqueue::Stream stream(&loop, sv[0]);

  std::array<std::byte, 16> buffer{};
  std::optional<ReadResult> result;
  bool resumed_with_scheduler = false;
  constexpr std::string_view kPayload = "pending";
  PeerWriteWork write_work{sv[1], kPayload};

  alyrn::coro::SpawnDetach(
      loop, ReadOnce(&stream, &loop, &loop, &buffer, &result, &resumed_with_scheduler));
  loop.Schedule(&write_work);
  loop.Run();

  ::close(sv[1]);

  return Check(result.has_value(), "pending read did not finish") &&
         Check(result->has_value(), "pending read returned error") &&
         Check(**result == kPayload.size(), "pending read byte count mismatch") &&
         Check(std::memcmp(buffer.data(), kPayload.data(), kPayload.size()) == 0,
               "pending read payload mismatch") &&
         Check(resumed_with_scheduler, "pending read resumed without current scheduler");
}

bool CheckOwnedReadIntoReturnsBuffer() {
  int sv[2] = {-1, -1};
  if (!MakeSocketPair(sv)) {
    std::cout << "FAIL: socketpair failed\n";
    return false;
  }

  constexpr std::string_view kPayload = "owned-read";
  if (::write(sv[1], kPayload.data(), kPayload.size()) != static_cast<ssize_t>(kPayload.size())) {
    std::cout << "FAIL: peer write failed\n";
    ::close(sv[0]);
    ::close(sv[1]);
    return false;
  }

  alyrn::kqueue::Loop loop;
  alyrn::kqueue::Stream stream(&loop, sv[0]);
  std::optional<OwnedReadOutcome> outcome;
  bool resumed_with_scheduler = false;

  alyrn::coro::SpawnDetach(loop, ReadIntoOnce(&stream, &loop, &loop, alyrn::net::Buffer(4),
                                                 &outcome, &resumed_with_scheduler));
  loop.Run();

  ::close(sv[1]);
  if (!Check(outcome.has_value(), "owned read did not finish") ||
      !Check(outcome->result.has_value(), "owned read returned an error") ||
      !Check(*outcome->result == kPayload.size(), "owned read byte count mismatch") ||
      !Check(Gather(outcome->buffer) == kPayload, "owned read payload mismatch") ||
      !Check(resumed_with_scheduler, "owned read resumed without current scheduler")) {
    return false;
  }

  auto reusable = outcome->buffer.PrepareWrite(8, 1);
  const bool reusable_after_resume = !reusable.empty();
  outcome->buffer.AbortWrite();
  return Check(reusable_after_resume, "owned read returned a buffer with a live reservation");
}

bool CheckCloseCancelsPendingRead() {
  int sv[2] = {-1, -1};
  if (!MakeSocketPair(sv)) {
    std::cout << "FAIL: socketpair failed\n";
    return false;
  }

  alyrn::kqueue::Loop loop;
  alyrn::kqueue::Stream stream(&loop, sv[0]);

  std::array<std::byte, 16> buffer{};
  std::optional<ReadResult> result;
  bool resumed_with_scheduler = false;
  CloseStreamWork close_work{&loop, &stream};

  alyrn::coro::SpawnDetach(
      loop, ReadOnce(&stream, &loop, &loop, &buffer, &result, &resumed_with_scheduler));
  loop.Schedule(&close_work);
  loop.Run();

  ::close(sv[1]);

  return Check(result.has_value(), "cancelled read did not finish") &&
         Check(!result->has_value(), "cancelled read unexpectedly returned value") &&
         Check(result->error() == std::errc::operation_canceled,
               "cancelled read did not return ECANCELED") &&
         Check(resumed_with_scheduler, "cancelled read resumed without current scheduler");
}

bool CheckLoopStopCancelsPendingRead() {
  int sv[2] = {-1, -1};
  if (!MakeSocketPair(sv)) {
    std::cout << "FAIL: socketpair failed\n";
    return false;
  }

  alyrn::kqueue::Loop loop;
  alyrn::kqueue::Stream stream(&loop, sv[0]);

  std::array<std::byte, 16> buffer{};
  std::optional<ReadResult> result;
  int resume_count = 0;
  bool resumed_with_scheduler = false;

  alyrn::coro::SpawnDetach(loop, ReadWithoutQuit(&stream, &loop, &buffer, &result, &resume_count,
                                                    &resumed_with_scheduler));
  std::thread stopper([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    loop.RequestStop();
  });
  loop.Run();
  stopper.join();

  ::close(sv[1]);

  return Check(result.has_value(), "loop stop did not settle the pending read") &&
         Check(!result->has_value(), "loop stop unexpectedly completed the read") &&
         Check(result->error() == std::errc::operation_canceled,
               "loop stop did not return ECANCELED") &&
         Check(resume_count == 1, "loop stop resumed the read continuation more than once") &&
         Check(resumed_with_scheduler, "loop stop resumed the read without scheduler affinity");
}

bool CheckEchoAlgorithmUsesAsyncStream() {
  int sv[2] = {-1, -1};
  if (!MakeSocketPair(sv)) {
    std::cout << "FAIL: socketpair failed\n";
    return false;
  }

  alyrn::kqueue::Loop loop;
  alyrn::kqueue::Stream server(&loop, sv[0]);
  alyrn::kqueue::Stream client(&loop, sv[1]);

  const char payload[] = "echo-through-async-stream";
  auto bytes = std::as_bytes(std::span(payload, sizeof(payload) - 1));

  std::array<std::byte, 64> server_buffer{};
  std::array<std::byte, 64> client_buffer{};
  std::optional<alyrn::Result<void>> server_result;
  std::optional<alyrn::Result<void>> client_result;
  std::size_t received_size = 0;
  int done_count = 0;

  alyrn::coro::SpawnDetach(
      loop, EchoServer(&server, &server_buffer, &server_result, &done_count, &loop));
  alyrn::coro::SpawnDetach(loop, EchoClient(&client, bytes, &client_buffer, &client_result,
                                               &received_size, &done_count, &loop));

  loop.Run();

  return Check(server_result.has_value(), "echo server did not finish") &&
         Check(server_result->has_value(), "echo server returned error") &&
         Check(client_result.has_value(), "echo client did not finish") &&
         Check(client_result->has_value(), "echo client returned error") &&
         Check(received_size == sizeof(payload) - 1, "echo client byte count mismatch") &&
         Check(std::memcmp(client_buffer.data(), payload, sizeof(payload) - 1) == 0,
               "echo client payload mismatch");
}

bool CheckCloseRejectsLaterSubmit() {
  int sv[2] = {-1, -1};
  if (!MakeSocketPair(sv)) {
    std::cout << "FAIL: socketpair failed\n";
    return false;
  }

  alyrn::kqueue::Loop loop;
  alyrn::kqueue::Stream stream(&loop, sv[0]);

  std::array<std::byte, 16> read_buffer{};
  const char payload[] = "after-close";
  auto bytes = std::as_bytes(std::span(payload, sizeof(payload) - 1));
  std::optional<ReadResult> read_result;
  std::optional<WriteResult> write_result;

  alyrn::coro::SpawnDetach(
      loop, CloseThenSubmit(&stream, &loop, &read_buffer, bytes, &read_result, &write_result));

  loop.Run();

  std::array<char, 1> peer_buffer{};
  const ssize_t peer_read = ::read(sv[1], peer_buffer.data(), peer_buffer.size());
  ::close(sv[1]);

  return Check(read_result.has_value(), "read after close did not finish") &&
         Check(!read_result->has_value(), "read after close unexpectedly succeeded") &&
         Check(read_result->error() == std::errc::bad_file_descriptor,
               "read after close did not return EBADF") &&
         Check(write_result.has_value(), "write after close did not finish") &&
         Check(!write_result->has_value(), "write after close unexpectedly succeeded") &&
         Check(write_result->error() == std::errc::bad_file_descriptor,
               "write after close did not return EBADF") &&
         Check(peer_read == 0, "peer did not observe local close");
}

bool CheckShutdownKeepsReadOpen() {
  int sv[2] = {-1, -1};
  if (!MakeSocketPair(sv)) {
    std::cout << "FAIL: socketpair failed\n";
    return false;
  }

  constexpr std::string_view kPayload = "still-readable";
  if (::write(sv[1], kPayload.data(), kPayload.size()) != static_cast<ssize_t>(kPayload.size())) {
    std::cout << "FAIL: peer write failed\n";
    ::close(sv[0]);
    ::close(sv[1]);
    return false;
  }

  alyrn::kqueue::Loop loop;
  alyrn::kqueue::Stream stream(&loop, sv[0]);

  std::array<std::byte, 16> read_buffer{};
  const char after_shutdown[] = "blocked";
  auto bytes = std::as_bytes(std::span(after_shutdown, sizeof(after_shutdown) - 1));
  std::optional<WriteResult> first_shutdown;
  std::optional<WriteResult> second_shutdown;
  std::optional<WriteResult> write_result;
  std::optional<ReadResult> read_result;

  alyrn::coro::SpawnDetach(loop, ShutdownThenReadAndWrite(&stream, &loop, &read_buffer, bytes,
                                                             &first_shutdown, &second_shutdown,
                                                             &write_result, &read_result));
  loop.Run();
  ::close(sv[1]);

  return Check(first_shutdown.has_value() && first_shutdown->has_value(),
               "first shutdown failed") &&
         Check(second_shutdown.has_value() && second_shutdown->has_value(),
               "idempotent shutdown failed") &&
         Check(write_result.has_value() && !write_result->has_value(),
               "write after shutdown should fail") &&
         Check(write_result->error() == std::errc::broken_pipe,
               "write after shutdown should return EPIPE") &&
         Check(read_result.has_value() && read_result->has_value(), "read after shutdown failed") &&
         Check(**read_result == kPayload.size(), "read after shutdown byte count mismatch");
}

bool CheckCloseReadKeepsWriteOpen() {
  int sv[2] = {-1, -1};
  if (!MakeSocketPair(sv)) {
    std::cout << "FAIL: socketpair failed\n";
    return false;
  }

  constexpr std::string_view kDiscarded = "discarded";
  if (::write(sv[1], kDiscarded.data(), kDiscarded.size()) !=
      static_cast<ssize_t>(kDiscarded.size())) {
    std::cout << "FAIL: peer write failed\n";
    ::close(sv[0]);
    ::close(sv[1]);
    return false;
  }

  alyrn::kqueue::Loop loop;
  alyrn::kqueue::Stream stream(&loop, sv[0]);
  std::array<std::byte, 16> read_buffer{};
  constexpr std::string_view kOutgoing = "still-writable";
  const auto write_buffer =
      std::as_bytes(std::span<const char>(kOutgoing.data(), kOutgoing.size()));
  std::optional<WriteResult> first_close_read;
  std::optional<WriteResult> second_close_read;
  std::optional<WriteResult> write_result;
  std::optional<ReadResult> read_result;

  alyrn::coro::SpawnDetach(
      loop, CloseReadThenReadAndWrite(&stream, &loop, &read_buffer, write_buffer,
                                      &first_close_read, &second_close_read, &write_result,
                                      &read_result));
  loop.Run();

  std::array<char, 32> peer_buffer{};
  const ssize_t peer_read = ::read(sv[1], peer_buffer.data(), peer_buffer.size());
  ::close(sv[1]);

  return Check(first_close_read.has_value() && first_close_read->has_value(),
               "first CloseRead failed") &&
         Check(second_close_read.has_value() && second_close_read->has_value(),
               "second CloseRead was not idempotent") &&
         Check(read_result.has_value() && read_result->has_value() && **read_result == 0,
               "ReadSome after CloseRead did not return EOF") &&
         Check(write_result.has_value() && write_result->has_value(),
               "WriteAll after CloseRead failed") &&
         Check(peer_read == static_cast<ssize_t>(kOutgoing.size()) &&
                   std::string_view(peer_buffer.data(), static_cast<std::size_t>(peer_read)) ==
                       kOutgoing,
               "CloseRead disabled the write direction");
}

}  // namespace

int main() {
  if (!CheckForeignLoopReadTerminates()) return 1;
  if (!CheckImmediateRead()) return 1;
  if (!CheckImmediateWrite()) return 1;
  if (!CheckPendingRead()) return 1;
  if (!CheckOwnedReadIntoReturnsBuffer()) return 1;
  if (!CheckCloseCancelsPendingRead()) return 1;
  if (!CheckLoopStopCancelsPendingRead()) return 1;
  if (!CheckEchoAlgorithmUsesAsyncStream()) return 1;
  if (!CheckCloseRejectsLaterSubmit()) return 1;
  if (!CheckShutdownKeepsReadOpen()) return 1;
  if (!CheckCloseReadKeepsWriteOpen()) return 1;
  std::cout << "kqueue stream smoke: PASS\n";
  return 0;
}
