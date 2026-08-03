// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <sys/socket.h>
#include <sys/uio.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <csignal>
#include <cstddef>
#include <cstring>
#include <expected>
#include <iostream>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include "coropact/base/error.h"
#include "coropact/coro/scheduler.h"
#include "coropact/coro/spawn.h"
#include "coropact/coro/sync_wait.h"
#include "coropact/coro/task.h"
#include "coropact/io/async_stream.h"
#include "coropact/io/buffer.h"
#include "coropact/io/stream_algorithms.h"
#include "coropact/reactor/event_loop.h"
#include "coropact/reactor/reactor_stream.h"

namespace {

using ReadResult = coropact::base::Result<std::size_t>;
using WriteResult = coropact::base::Result<std::size_t>;
using OwnedReadOutcome = coropact::net::ReadIntoOutcome;

static_assert(coropact::io::AsyncStream<coropact::reactor::ReactorStream>);
static_assert(coropact::io::AsyncTimedStream<coropact::reactor::ReactorStream>);
static_assert(coropact::io::AsyncOwnedReadStream<coropact::reactor::ReactorStream>);

bool Check(bool condition, const char* message) {
  if (!condition) {
    std::cout << "FAIL: " << message << '\n';
    return false;
  }
  return true;
}

bool MakeSocketPair(int sv[2]) {
  return ::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sv) == 0;
}

std::string Gather(coropact::io::Buffer& buffer) {
  std::string out;
  for (const iovec& iov : buffer.ReadableIov(32)) {
    out.append(static_cast<const char*>(iov.iov_base), iov.iov_len);
  }
  return out;
}

coropact::coro::DetachedTask ReadOnce(coropact::reactor::ReactorStream* stream,
                                      coropact::reactor::EventLoop* loop,
                                      coropact::reactor::EventLoop* scheduler,
                                      std::array<std::byte, 16>* buffer,
                                      std::optional<ReadResult>* out,
                                      bool* resumed_with_scheduler) {
  ReadResult result = co_await stream->ReadSome(*buffer);
  *resumed_with_scheduler = coropact::coro::Scheduler::Current() == scheduler;
  out->emplace(std::move(result));
  loop->Quit();
}

coropact::coro::DetachedTask ReadWithoutQuit(coropact::reactor::ReactorStream* stream,
                                             coropact::reactor::EventLoop* scheduler,
                                             std::array<std::byte, 16>* buffer,
                                             std::optional<ReadResult>* out, int* resume_count,
                                             bool* resumed_with_scheduler) {
  ReadResult result = co_await stream->ReadSome(*buffer);
  ++*resume_count;
  *resumed_with_scheduler = coropact::coro::Scheduler::Current() == scheduler;
  out->emplace(std::move(result));
}

coropact::coro::DetachedTask ReadBufferOnce(coropact::reactor::ReactorStream* stream,
                                            coropact::reactor::EventLoop* loop,
                                            coropact::reactor::EventLoop* scheduler,
                                            coropact::io::Buffer* buffer,
                                            std::optional<ReadResult>* out,
                                            bool* resumed_with_scheduler) {
  ReadResult result = co_await stream->ReadSome(*buffer, 32);
  *resumed_with_scheduler = coropact::coro::Scheduler::Current() == scheduler;
  out->emplace(std::move(result));
  loop->Quit();
}

coropact::coro::DetachedTask ReadIntoOnce(coropact::reactor::ReactorStream* stream,
                                          coropact::reactor::EventLoop* loop,
                                          coropact::reactor::EventLoop* scheduler,
                                          coropact::net::Buffer buffer,
                                          std::optional<OwnedReadOutcome>* out,
                                          bool* resumed_with_scheduler) {
  OwnedReadOutcome outcome = co_await stream->ReadInto(std::move(buffer), 32);
  *resumed_with_scheduler = coropact::coro::Scheduler::Current() == scheduler;
  out->emplace(std::move(outcome));
  loop->Quit();
}

coropact::coro::DetachedTask WriteOnce(coropact::reactor::ReactorStream* stream,
                                       coropact::reactor::EventLoop* loop,
                                       coropact::reactor::EventLoop* scheduler,
                                       std::span<const std::byte> payload,
                                       std::optional<WriteResult>* out,
                                       bool* resumed_with_scheduler) {
  WriteResult result = co_await stream->WriteSome(payload);
  *resumed_with_scheduler = coropact::coro::Scheduler::Current() == scheduler;
  out->emplace(std::move(result));
  loop->Quit();
}

coropact::coro::DetachedTask WriteBufferOnce(coropact::reactor::ReactorStream* stream,
                                             coropact::reactor::EventLoop* loop,
                                             coropact::reactor::EventLoop* scheduler,
                                             coropact::io::Buffer* buffer,
                                             std::optional<WriteResult>* out,
                                             bool* resumed_with_scheduler) {
  WriteResult result = co_await stream->WriteSome(*buffer);
  *resumed_with_scheduler = coropact::coro::Scheduler::Current() == scheduler;
  out->emplace(std::move(result));
  loop->Quit();
}

coropact::coro::DetachedTask EchoServer(coropact::reactor::ReactorStream* stream,
                                        std::array<std::byte, 64>* scratch,
                                        std::optional<coropact::base::Result<void>>* out,
                                        int* done_count, coropact::reactor::EventLoop* loop) {
  out->emplace(co_await coropact::io::EchoOnce(*stream, *scratch));
  if (++(*done_count) == 2) {
    loop->Quit();
  }
}

coropact::coro::DetachedTask EchoClient(coropact::reactor::ReactorStream* stream,
                                        std::span<const std::byte> payload,
                                        std::array<std::byte, 64>* received,
                                        std::optional<coropact::base::Result<void>>* out,
                                        std::size_t* received_size, int* done_count,
                                        coropact::reactor::EventLoop* loop) {
  coropact::base::Result<void> write_result = co_await coropact::io::WriteAll(*stream, payload);
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
    loop->Quit();
  }
}

coropact::coro::DetachedTask CloseThenSubmit(coropact::reactor::ReactorStream* stream,
                                             coropact::reactor::EventLoop* loop,
                                             std::array<std::byte, 16>* read_buffer,
                                             std::span<const std::byte> write_buffer,
                                             std::optional<ReadResult>* read_result,
                                             std::optional<WriteResult>* write_result) {
  coropact::base::Result<void> close_result = co_await stream->Close();
  if (!close_result.has_value()) {
    read_result->emplace(std::unexpected(close_result.error()));
    write_result->emplace(std::unexpected(close_result.error()));
  } else {
    read_result->emplace(co_await stream->ReadSome(*read_buffer));
    write_result->emplace(co_await stream->WriteSome(write_buffer));
  }
  loop->Quit();
}

coropact::coro::Task<ReadResult> ReadFromForeignLoopThread(coropact::reactor::ReactorStream* stream,
                                                           std::array<std::byte, 16>* buffer) {
  co_return co_await stream->ReadSome(*buffer);
}

bool CheckForeignLoopReadTerminates() {
  int sv[2] = {-1, -1};
  if (!MakeSocketPair(sv)) {
    std::cout << "FAIL: socketpair failed for owner-loop check\n";
    return false;
  }

  coropact::reactor::EventLoop loop;
  coropact::reactor::ReactorStream stream(&loop, sv[0]);
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
      static_cast<void>(coropact::coro::SyncWait(ReadFromForeignLoopThread(&stream, &buffer)));
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
               "foreign-loop stream operation must terminate through COROPACT_CHECK");
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

  coropact::reactor::EventLoop loop;
  coropact::reactor::ReactorStream stream(&loop, sv[0]);

  std::array<std::byte, 16> buffer{};
  std::optional<ReadResult> result;
  bool resumed_with_scheduler = false;

  coropact::coro::SpawnDetach(
      loop, ReadOnce(&stream, &loop, &loop, &buffer, &result, &resumed_with_scheduler));
  loop.Loop();

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

  coropact::reactor::EventLoop loop;
  coropact::reactor::ReactorStream stream(&loop, sv[0]);

  const char payload[] = "write";
  auto bytes = std::as_bytes(std::span(payload, sizeof(payload) - 1));
  std::optional<WriteResult> result;
  bool resumed_with_scheduler = false;

  coropact::coro::SpawnDetach(
      loop, WriteOnce(&stream, &loop, &loop, bytes, &result, &resumed_with_scheduler));
  loop.Loop();

  std::array<char, 16> received{};
  const ssize_t n = ::read(sv[1], received.data(), received.size());
  ::close(sv[1]);

  return Check(result.has_value(), "immediate write did not finish") &&
         Check(result->has_value(), "immediate write returned error") &&
         Check(**result == sizeof(payload) - 1, "immediate write byte count mismatch") &&
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

  coropact::reactor::EventLoop loop;
  coropact::reactor::ReactorStreamOptions stream_options{
      .trigger_mode = coropact::reactor::TriggerMode::kLevelTriggered};
  coropact::reactor::ReactorStream stream(&loop, sv[0], coropact::net::Endpoint(0),
                                          stream_options);

  std::array<std::byte, 16> buffer{};
  std::optional<ReadResult> result;
  bool resumed_with_scheduler = false;

  coropact::coro::SpawnDetach(
      loop, ReadOnce(&stream, &loop, &loop, &buffer, &result, &resumed_with_scheduler));

  const char payload[] = "pending";
  loop.RunAfter(0.0, [fd = sv[1]] {
    const char data[] = "pending";
    (void)::write(fd, data, sizeof(data) - 1);
  });

  loop.Loop();

  ::close(sv[1]);

  return Check(result.has_value(), "pending read did not finish") &&
         Check(result->has_value(), "pending read returned error") &&
         Check(**result == sizeof(payload) - 1, "pending read byte count mismatch") &&
         Check(std::memcmp(buffer.data(), payload, sizeof(payload) - 1) == 0,
               "pending read payload mismatch") &&
         Check(resumed_with_scheduler, "pending read resumed without current scheduler");
}

bool CheckReadIntoIoBuffer() {
  int sv[2] = {-1, -1};
  if (!MakeSocketPair(sv)) {
    std::cout << "FAIL: socketpair failed\n";
    return false;
  }

  const char payload[] = "buffer-read";
  if (::write(sv[1], payload, sizeof(payload) - 1) != static_cast<ssize_t>(sizeof(payload) - 1)) {
    std::cout << "FAIL: initial buffer write failed\n";
    ::close(sv[0]);
    ::close(sv[1]);
    return false;
  }

  coropact::reactor::EventLoop loop;
  coropact::reactor::ReactorStream stream(&loop, sv[0]);

  coropact::io::Buffer buffer(4);
  std::optional<ReadResult> result;
  bool resumed_with_scheduler = false;

  coropact::coro::SpawnDetach(loop, ReadBufferOnce(&stream, &loop, &loop, &buffer,
                                                        &result, &resumed_with_scheduler));
  loop.Loop();

  ::close(sv[1]);

  return Check(result.has_value(), "buffer read did not finish") &&
         Check(result->has_value(), "buffer read returned error") &&
         Check(**result == sizeof(payload) - 1, "buffer read byte count mismatch") &&
         Check(buffer.ReadableBytes() == sizeof(payload) - 1,
               "buffer read should commit readable bytes") &&
         Check(Gather(buffer) == std::string(payload, sizeof(payload) - 1),
               "buffer read payload mismatch") &&
         Check(resumed_with_scheduler, "buffer read resumed without current scheduler");
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

  coropact::reactor::EventLoop loop;
  coropact::reactor::ReactorStream stream(&loop, sv[0]);
  std::optional<OwnedReadOutcome> outcome;
  bool resumed_with_scheduler = false;

  coropact::coro::SpawnDetach(
      loop, ReadIntoOnce(&stream, &loop, &loop, coropact::net::Buffer(4), &outcome,
                              &resumed_with_scheduler));
  loop.Loop();

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

bool CheckOwnedReadIntoCloseReturnsBuffer() {
  int sv[2] = {-1, -1};
  if (!MakeSocketPair(sv)) {
    std::cout << "FAIL: socketpair failed\n";
    return false;
  }

  coropact::reactor::EventLoop loop;
  coropact::reactor::ReactorStream stream(&loop, sv[0]);
  std::optional<OwnedReadOutcome> outcome;
  bool resumed_with_scheduler = false;

  coropact::coro::SpawnDetach(
      loop, ReadIntoOnce(&stream, &loop, &loop, coropact::net::Buffer(8), &outcome,
                              &resumed_with_scheduler));
  loop.RunAfter(0.0, [&] { coropact::coro::Spawn(loop, stream.Close()).Detach(); });
  loop.Loop();

  ::close(sv[1]);
  if (!Check(outcome.has_value(), "owned cancelled read did not finish") ||
      !Check(!outcome->result.has_value(), "owned cancelled read unexpectedly succeeded") ||
      !Check(outcome->result.error() == std::errc::operation_canceled,
             "owned cancelled read did not return ECANCELED") ||
      !Check(resumed_with_scheduler, "owned cancelled read resumed without current scheduler")) {
    return false;
  }

  auto reusable = outcome->buffer.PrepareWrite(8, 1);
  const bool reusable_after_cancel = !reusable.empty();
  outcome->buffer.AbortWrite();
  return Check(reusable_after_cancel,
               "owned cancelled read returned a buffer with a live reservation");
}

bool CheckWriteFromIoBuffer() {
  int sv[2] = {-1, -1};
  if (!MakeSocketPair(sv)) {
    std::cout << "FAIL: socketpair failed\n";
    return false;
  }

  coropact::reactor::EventLoop loop;
  coropact::reactor::ReactorStream stream(&loop, sv[0]);

  const std::string payload = "write-from-io-buffer";
  coropact::io::Buffer buffer(5);
  buffer.Append(payload.substr(0, 7));
  buffer.Append(payload.substr(7));

  std::optional<WriteResult> result;
  bool resumed_with_scheduler = false;

  coropact::coro::SpawnDetach(loop, WriteBufferOnce(&stream, &loop, &loop, &buffer,
                                                         &result, &resumed_with_scheduler));
  loop.Loop();

  std::array<char, 64> received{};
  const ssize_t n = ::read(sv[1], received.data(), received.size());
  ::close(sv[1]);

  return Check(result.has_value(), "buffer write did not finish") &&
         Check(result->has_value(), "buffer write returned error") &&
         Check(**result == payload.size(), "buffer write byte count mismatch") &&
         Check(buffer.Empty(), "buffer write should drain written bytes") &&
         Check(n == static_cast<ssize_t>(payload.size()), "buffer write peer count mismatch") &&
         Check(std::string(received.data(), static_cast<std::size_t>(n)) == payload,
               "buffer write payload mismatch") &&
         Check(resumed_with_scheduler, "buffer write resumed without current scheduler");
}

bool CheckCloseCancelsPendingRead() {
  int sv[2] = {-1, -1};
  if (!MakeSocketPair(sv)) {
    std::cout << "FAIL: socketpair failed\n";
    return false;
  }

  coropact::reactor::EventLoop loop;
  coropact::reactor::ReactorStream stream(&loop, sv[0]);

  std::array<std::byte, 16> buffer{};
  std::optional<ReadResult> result;
  bool resumed_with_scheduler = false;

  coropact::coro::SpawnDetach(
      loop, ReadOnce(&stream, &loop, &loop, &buffer, &result, &resumed_with_scheduler));
  loop.RunAfter(0.0, [&] { coropact::coro::Spawn(loop, stream.Close()).Detach(); });

  loop.Loop();

  ::close(sv[1]);

  return Check(result.has_value(), "cancelled read did not finish") &&
         Check(!result->has_value(), "cancelled read unexpectedly returned value") &&
         Check(result->error() == std::errc::operation_canceled,
               "cancelled read did not return ECANCELED") &&
         Check(resumed_with_scheduler, "cancelled read resumed without current scheduler");
}

bool CheckReadableThenCloseResumesOnce() {
  int sv[2] = {-1, -1};
  if (!MakeSocketPair(sv)) {
    std::cout << "FAIL: socketpair failed\n";
    return false;
  }

  coropact::reactor::EventLoop loop;
  coropact::reactor::ReactorStream stream(&loop, sv[0]);

  std::array<std::byte, 16> buffer{};
  std::optional<ReadResult> result;
  int resume_count = 0;
  bool resumed_with_scheduler = false;

  coropact::coro::SpawnDetach(loop, ReadWithoutQuit(&stream, &loop, &buffer, &result,
                                                         &resume_count, &resumed_with_scheduler));
  loop.RunAfter(0.0, [&] {
    const char payload[] = "race";
    COROPACT_IGNORE_RESULT(::write(sv[1], payload, sizeof(payload) - 1));
    coropact::coro::Spawn(loop, stream.Close()).Detach();
  });
  loop.RunAfter(0.01, [&] { loop.Quit(); });
  loop.Loop();

  ::close(sv[1]);

  if (!Check(result.has_value(), "readable-close race did not finish") ||
      !Check(resume_count == 1, "readable-close race resumed the coroutine more than once") ||
      !Check(resumed_with_scheduler,
             "readable-close race resumed without the captured scheduler")) {
    return false;
  }

  if (result->has_value()) {
    return Check(**result == 4, "readable-close race returned wrong byte count");
  }
  return Check(result->error() == std::errc::operation_canceled,
               "readable-close race failed with an unexpected error");
}

bool CheckEchoAlgorithmUsesAsyncStream() {
  int sv[2] = {-1, -1};
  if (!MakeSocketPair(sv)) {
    std::cout << "FAIL: socketpair failed\n";
    return false;
  }

  coropact::reactor::EventLoop loop;
  coropact::reactor::ReactorStream server(&loop, sv[0]);
  coropact::reactor::ReactorStream client(&loop, sv[1]);

  const char payload[] = "echo-through-async-stream";
  auto bytes = std::as_bytes(std::span(payload, sizeof(payload) - 1));

  std::array<std::byte, 64> server_buffer{};
  std::array<std::byte, 64> client_buffer{};
  std::optional<coropact::base::Result<void>> server_result;
  std::optional<coropact::base::Result<void>> client_result;
  std::size_t received_size = 0;
  int done_count = 0;

  coropact::coro::SpawnDetach(
      loop, EchoServer(&server, &server_buffer, &server_result, &done_count, &loop));
  coropact::coro::SpawnDetach(loop, EchoClient(&client, bytes, &client_buffer, &client_result,
                                                    &received_size, &done_count, &loop));

  loop.Loop();

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

  coropact::reactor::EventLoop loop;
  coropact::reactor::ReactorStream stream(&loop, sv[0]);

  std::array<std::byte, 16> read_buffer{};
  const char payload[] = "after-close";
  auto bytes = std::as_bytes(std::span(payload, sizeof(payload) - 1));
  std::optional<ReadResult> read_result;
  std::optional<WriteResult> write_result;

  coropact::coro::SpawnDetach(
      loop, CloseThenSubmit(&stream, &loop, &read_buffer, bytes, &read_result, &write_result));

  loop.Loop();

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

}  // namespace

int main() {
  if (!CheckForeignLoopReadTerminates()) return 1;
  if (!CheckImmediateRead()) return 1;
  if (!CheckImmediateWrite()) return 1;
  if (!CheckPendingRead()) return 1;
  if (!CheckReadIntoIoBuffer()) return 1;
  if (!CheckOwnedReadIntoReturnsBuffer()) return 1;
  if (!CheckOwnedReadIntoCloseReturnsBuffer()) return 1;
  if (!CheckWriteFromIoBuffer()) return 1;
  if (!CheckCloseCancelsPendingRead()) return 1;
  if (!CheckReadableThenCloseResumesOnce()) return 1;
  if (!CheckEchoAlgorithmUsesAsyncStream()) return 1;
  if (!CheckCloseRejectsLaterSubmit()) return 1;

  std::cout << "reactor stream smoke: PASS\n";
  return 0;
}
