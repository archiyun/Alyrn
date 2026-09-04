// SPDX-License-Identifier: MIT

#include <sys/socket.h>
#include <sys/uio.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <expected>
#include <iostream>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "alyrn/coro/scheduler.h"
#include "alyrn/coro/spawn.h"
#include "alyrn/coro/sync_wait.h"
#include "alyrn/coro/task.h"
#include "alyrn/epoll/loop.h"
#include "alyrn/epoll/stream.h"
#include "alyrn/io/async_stream.h"
#include "alyrn/io/buffer.h"
#include "alyrn/io/recv.h"
#include "alyrn/result.h"

namespace {

using ReadResult = alyrn::Result<std::size_t>;
using WriteResult = alyrn::Result<void>;
using OwnedRecvOutcome = alyrn::io::RecvOutcome;

static_assert(alyrn::io::AsyncStream<alyrn::epoll::Stream>);
static_assert(alyrn::io::AsyncRecvStream<alyrn::epoll::Stream>);
static_assert(alyrn::io::AsyncRecvCopyStream<alyrn::epoll::Stream>);

bool Check(bool condition, const char* message) {
  if (!condition) {
    std::cout << "FAIL: " << message << '\n';
    return false;
  }
  return true;
}

bool ExpectChildAbort(void (*entry)(), const char* message) {
  const pid_t child = ::fork();
  if (child < 0) {
    return Check(false, "fork failed for Stream affinity test");
  }
  if (child == 0) {
    (void)::freopen("/dev/null", "w", stderr);
    entry();
    ::_exit(0);
  }

  int status = 0;
  while (::waitpid(child, &status, 0) < 0 && errno == EINTR) {
  }
  return Check(WIFSIGNALED(status), message) &&
         Check(WTERMSIG(status) == SIGABRT,
               "stream-affinity invariant must terminate with SIGABRT");
}

bool MakeSocketPair(int sv[2]) {
  return ::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0, sv) == 0;
}

std::string Gather(alyrn::io::Buffer& buffer) {
  std::string out;
  for (const iovec& iov : buffer.ReadableIov(32)) {
    out.append(static_cast<const char*>(iov.iov_base), iov.iov_len);
  }
  return out;
}

alyrn::coro::DetachedTask ReadOnce(alyrn::epoll::Stream* stream, alyrn::epoll::Loop* loop,
                                   alyrn::epoll::Loop* scheduler, std::array<std::byte, 16>* buffer,
                                   std::optional<ReadResult>* out, bool* resumed_with_scheduler) {
  ReadResult result = co_await stream->Read(*buffer);
  *resumed_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == scheduler;
  out->emplace(std::move(result));
  loop->RequestStop();
}

alyrn::coro::DetachedTask ReadWithDeadline(alyrn::epoll::Stream* stream,
                                           alyrn::epoll::Loop* loop,
                                           std::array<std::byte, 16>* buffer,
                                           std::optional<ReadResult>* out) {
  auto configured =
      stream->SetReadDeadline(alyrn::time::SteadyNow() + alyrn::time::Milliseconds(10));
  if (!configured.HasValue()) {
    out->emplace(std::unexpected(configured.Error()));
    loop->RequestStop();
    co_return;
  }

  out->emplace(co_await stream->Read(*buffer));
  loop->RequestStop();
}

alyrn::coro::DetachedTask ReadWithoutQuit(alyrn::epoll::Stream* stream,
                                          alyrn::epoll::Loop* scheduler,
                                          std::array<std::byte, 16>* buffer,
                                          std::optional<ReadResult>* out, int* resume_count,
                                          bool* resumed_with_scheduler) {
  ReadResult result = co_await stream->Read(*buffer);
  ++*resume_count;
  *resumed_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == scheduler;
  out->emplace(std::move(result));
}

alyrn::coro::DetachedTask RecvOnce(alyrn::epoll::Stream* stream, alyrn::epoll::Loop* loop,
                                   alyrn::epoll::Loop* scheduler, alyrn::net::Buffer buffer,
                                   std::optional<OwnedRecvOutcome>* out,
                                   bool* resumed_with_scheduler) {
  OwnedRecvOutcome outcome = co_await stream->Recv(std::move(buffer), 32);
  *resumed_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == scheduler;
  out->emplace(std::move(outcome));
  loop->RequestStop();
}

alyrn::coro::DetachedTask RecvCopyOnce(alyrn::epoll::Stream* stream, alyrn::epoll::Loop* loop,
                                       alyrn::epoll::Loop* scheduler,
                                       std::optional<alyrn::Result<alyrn::net::Buffer>>* out,
                                       bool* resumed_with_scheduler) {
  auto buffer = co_await stream->Recv();
  *resumed_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == scheduler;
  out->emplace(std::move(buffer));
  loop->RequestStop();
}

alyrn::coro::DetachedTask WriteOnce(alyrn::epoll::Stream* stream, alyrn::epoll::Loop* loop,
                                    alyrn::epoll::Loop* scheduler,
                                    std::span<const std::byte> payload,
                                    std::optional<WriteResult>* out, bool* resumed_with_scheduler) {
  WriteResult result = co_await stream->Write(payload);
  *resumed_with_scheduler = alyrn::coro::Scheduler::TryCurrent() == scheduler;
  out->emplace(std::move(result));
  loop->RequestStop();
}

alyrn::coro::DetachedTask EchoServer(alyrn::epoll::Stream* stream,
                                     std::array<std::byte, 64>* scratch,
                                     std::optional<alyrn::Result<void>>* out, int* done_count,
                                     alyrn::epoll::Loop* loop) {
  ReadResult read_result = co_await stream->Read(*scratch);
  if (!read_result.HasValue()) {
    out->emplace(std::unexpected(read_result.Error()));
  } else if (*read_result == 0) {
    out->emplace(alyrn::Result<void>{});
  } else {
    out->emplace(co_await stream->Write(std::span<const std::byte>(scratch->data(), *read_result)));
  }
  if (++(*done_count) == 2) {
    loop->RequestStop();
  }
}

alyrn::coro::DetachedTask EchoClient(alyrn::epoll::Stream* stream,
                                     std::span<const std::byte> payload,
                                     std::array<std::byte, 64>* received,
                                     std::optional<alyrn::Result<void>>* out,
                                     std::size_t* received_size, int* done_count,
                                     alyrn::epoll::Loop* loop) {
  alyrn::Result<void> write_result = co_await stream->Write(payload);
  if (!write_result.HasValue()) {
    out->emplace(std::unexpected(write_result.Error()));
  } else {
    ReadResult read_result = co_await stream->Read(*received);
    if (!read_result.HasValue()) {
      out->emplace(std::unexpected(read_result.Error()));
    } else {
      *received_size = *read_result;
      out->emplace(co_await stream->Shutdown());
    }
  }

  if (++(*done_count) == 2) {
    loop->RequestStop();
  }
}

alyrn::coro::DetachedTask CloseThenSubmit(alyrn::epoll::Stream* stream, alyrn::epoll::Loop* loop,
                                          std::array<std::byte, 16>* read_buffer,
                                          std::span<const std::byte> write_buffer,
                                          std::optional<ReadResult>* read_result,
                                          std::optional<WriteResult>* write_result) {
  alyrn::Result<void> close_result = co_await stream->Close();
  if (!close_result.HasValue()) {
    read_result->emplace(std::unexpected(close_result.Error()));
    write_result->emplace(std::unexpected(close_result.Error()));
  } else {
    read_result->emplace(co_await stream->Read(*read_buffer));
    write_result->emplace(co_await stream->Write(write_buffer));
  }
  loop->RequestStop();
}

alyrn::coro::DetachedTask ShutdownThenReadAndWrite(
    alyrn::epoll::Stream* stream, alyrn::epoll::Loop* loop, std::array<std::byte, 16>* read_buffer,
    std::span<const std::byte> write_buffer, std::optional<WriteResult>* first_shutdown,
    std::optional<WriteResult>* second_shutdown, std::optional<WriteResult>* write_result,
    std::optional<ReadResult>* read_result) {
  first_shutdown->emplace(co_await stream->CloseWrite());
  second_shutdown->emplace(co_await stream->Shutdown());
  write_result->emplace(co_await stream->Write(write_buffer));
  read_result->emplace(co_await stream->Read(*read_buffer));
  loop->RequestStop();
}

alyrn::coro::Task<ReadResult> ReadFromForeignLoopThread(alyrn::epoll::Stream* stream,
                                                        std::array<std::byte, 16>* buffer) {
  co_return co_await stream->Read(*buffer);
}

alyrn::coro::Task<WriteResult> WriteFromForeignLoopThread(alyrn::epoll::Stream* stream,
                                                          std::span<const std::byte> buffer) {
  co_return co_await stream->Write(buffer);
}

void OpenStreamOnOwner(alyrn::epoll::Loop* loop, alyrn::epoll::Stream** stream) {
  int sv[2] = {-1, -1};
  if (!MakeSocketPair(sv)) {
    ::_exit(1);
  }
  *stream = new alyrn::epoll::Stream(loop, sv[0]);
  (void)::close(sv[1]);
}

void ReadFromForeignThread() {
  alyrn::epoll::Loop loop;
  alyrn::epoll::Stream* stream = nullptr;
  OpenStreamOnOwner(&loop, &stream);
  std::thread foreign([stream] {
    std::array<std::byte, 16> buffer{};
    static_cast<void>(alyrn::coro::SyncWait(ReadFromForeignLoopThread(stream, &buffer)));
  });
  foreign.join();
}

void WriteFromForeignThread() {
  alyrn::epoll::Loop loop;
  alyrn::epoll::Stream* stream = nullptr;
  OpenStreamOnOwner(&loop, &stream);
  std::thread foreign([stream] {
    const std::array<std::byte, 1> buffer{};
    static_cast<void>(alyrn::coro::SyncWait(WriteFromForeignLoopThread(stream, buffer)));
  });
  foreign.join();
}

void CloseFromForeignThread() {
  alyrn::epoll::Loop loop;
  alyrn::epoll::Stream* stream = nullptr;
  OpenStreamOnOwner(&loop, &stream);
  std::thread foreign([stream] { static_cast<void>(alyrn::coro::SyncWait(stream->Close())); });
  foreign.join();
}

void MoveFromForeignThread() {
  alyrn::epoll::Loop loop;
  alyrn::epoll::Stream* stream = nullptr;
  OpenStreamOnOwner(&loop, &stream);
  std::thread foreign([stream] { alyrn::epoll::Stream moved(std::move(*stream)); });
  foreign.join();
}

void DestroyStreamFromForeignThread() {
  alyrn::epoll::Loop loop;
  alyrn::epoll::Stream* stream = nullptr;
  OpenStreamOnOwner(&loop, &stream);
  std::thread foreign([stream] { delete stream; });
  foreign.join();
}

bool CheckStreamAffinityIsEnforcedInRelease() {
  return ExpectChildAbort(&ReadFromForeignThread,
                          "Read from a foreign thread must terminate in Release") &&
         ExpectChildAbort(&WriteFromForeignThread,
                          "Write from a foreign thread must terminate in Release") &&
         ExpectChildAbort(&CloseFromForeignThread,
                          "Close from a foreign thread must terminate in Release") &&
         ExpectChildAbort(&MoveFromForeignThread,
                          "Stream move from a foreign thread must terminate in Release") &&
         ExpectChildAbort(&DestroyStreamFromForeignThread,
                          "Stream destruction from a foreign thread must terminate in Release");
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

  alyrn::epoll::Loop loop;
  alyrn::epoll::Stream stream(&loop, sv[0]);

  std::array<std::byte, 16> buffer{};
  std::optional<ReadResult> result;
  bool resumed_with_scheduler = false;

  alyrn::coro::SpawnDetach(
      loop, ReadOnce(&stream, &loop, &loop, &buffer, &result, &resumed_with_scheduler));
  loop.Run();

  ::close(sv[1]);

  return Check(result.has_value(), "immediate read did not finish") &&
         Check(result->HasValue(), "immediate read returned error") &&
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

  alyrn::epoll::Loop loop;
  alyrn::epoll::Stream stream(&loop, sv[0]);
  std::cout << "write test fds=" << sv[0] << "," << sv[1] << '\n';

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
         Check(result->HasValue(), "immediate write returned error") &&
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

  alyrn::epoll::Loop loop;
  alyrn::epoll::StreamOptions stream_options{.trigger_mode =
                                                 alyrn::epoll::TriggerMode::kLevelTriggered};
  alyrn::epoll::Stream stream(&loop, sv[0], alyrn::net::Endpoint(0), stream_options);

  std::array<std::byte, 16> buffer{};
  std::optional<ReadResult> result;
  bool resumed_with_scheduler = false;

  alyrn::coro::SpawnDetach(
      loop, ReadOnce(&stream, &loop, &loop, &buffer, &result, &resumed_with_scheduler));

  const char payload[] = "pending";
  loop.RunAfter(alyrn::time::Duration::zero(), [fd = sv[1]] {
    const char data[] = "pending";
    (void)::write(fd, data, sizeof(data) - 1);
  });

  loop.Run();

  ::close(sv[1]);

  return Check(result.has_value(), "pending read did not finish") &&
         Check(result->HasValue(), "pending read returned error") &&
         Check(**result == sizeof(payload) - 1, "pending read byte count mismatch") &&
         Check(std::memcmp(buffer.data(), payload, sizeof(payload) - 1) == 0,
               "pending read payload mismatch") &&
         Check(resumed_with_scheduler, "pending read resumed without current scheduler");
}

bool CheckReadDeadline() {
  int sv[2] = {-1, -1};
  if (!MakeSocketPair(sv)) {
    std::cout << "FAIL: socketpair failed\n";
    return false;
  }

  alyrn::epoll::Loop loop;
  alyrn::epoll::Stream stream(&loop, sv[0]);
  std::array<std::byte, 16> buffer{};
  std::optional<ReadResult> result;

  alyrn::coro::SpawnDetach(loop, ReadWithDeadline(&stream, &loop, &buffer, &result));
  loop.Run();
  ::close(sv[1]);

  return Check(result.has_value(), "read deadline did not finish") &&
         Check(!result->HasValue(), "read deadline unexpectedly succeeded") &&
         Check(result->Error() == std::errc::timed_out,
               "read deadline returned an unexpected error");
}

bool CheckOwnedRecvReturnsBuffer() {
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

  alyrn::epoll::Loop loop;
  alyrn::epoll::Stream stream(&loop, sv[0]);
  std::optional<OwnedRecvOutcome> outcome;
  bool resumed_with_scheduler = false;

  alyrn::coro::SpawnDetach(loop, RecvOnce(&stream, &loop, &loop, alyrn::net::Buffer(4), &outcome,
                                          &resumed_with_scheduler));
  loop.Run();

  ::close(sv[1]);
  if (!Check(outcome.has_value(), "owned read did not finish") ||
      !Check(outcome->result.HasValue(), "owned read returned an error") ||
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

bool CheckPooledRecvCopiesPayload() {
  int sv[2] = {-1, -1};
  if (!MakeSocketPair(sv)) {
    std::cout << "FAIL: socketpair failed\n";
    return false;
  }

  constexpr std::string_view kPayload = "pooled-recv";
  if (::write(sv[1], kPayload.data(), kPayload.size()) != static_cast<ssize_t>(kPayload.size())) {
    std::cout << "FAIL: peer write failed\n";
    ::close(sv[0]);
    ::close(sv[1]);
    return false;
  }

  alyrn::epoll::Loop loop;
  alyrn::epoll::Stream stream(&loop, sv[0]);
  std::optional<alyrn::Result<alyrn::net::Buffer>> outcome;
  bool resumed_with_scheduler = false;

  alyrn::coro::SpawnDetach(loop,
                           RecvCopyOnce(&stream, &loop, &loop, &outcome, &resumed_with_scheduler));
  loop.Run();

  ::close(sv[1]);
  if (!Check(outcome.has_value(), "pooled Recv did not finish") ||
      !Check(outcome->HasValue(), "pooled Recv returned an error") ||
      !Check(resumed_with_scheduler, "pooled Recv resumed without current scheduler")) {
    return false;
  }

  auto& buffer = **outcome;
  if (!Check(Gather(buffer) == kPayload, "pooled Recv payload mismatch")) {
    return false;
  }
  auto reusable = buffer.PrepareWrite(8, 1);
  const bool reusable_after_resume = !reusable.empty();
  buffer.AbortWrite();
  return Check(reusable_after_resume, "pooled Recv returned a buffer with a live reservation");
}

bool CheckOwnedRecvCloseReturnsBuffer() {
  int sv[2] = {-1, -1};
  if (!MakeSocketPair(sv)) {
    std::cout << "FAIL: socketpair failed\n";
    return false;
  }

  alyrn::epoll::Loop loop;
  alyrn::epoll::Stream stream(&loop, sv[0]);
  std::optional<OwnedRecvOutcome> outcome;
  bool resumed_with_scheduler = false;

  alyrn::coro::SpawnDetach(loop, RecvOnce(&stream, &loop, &loop, alyrn::net::Buffer(8), &outcome,
                                          &resumed_with_scheduler));
  loop.RunAfter(alyrn::time::Duration::zero(),
                [&] { alyrn::coro::Spawn(loop, stream.Close()).Detach(); });
  loop.Run();

  ::close(sv[1]);
  if (!Check(outcome.has_value(), "owned cancelled read did not finish") ||
      !Check(!outcome->result.HasValue(), "owned cancelled read unexpectedly succeeded") ||
      !Check(outcome->result.Error() == std::errc::operation_canceled,
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

bool CheckCloseCancelsPendingRead() {
  int sv[2] = {-1, -1};
  if (!MakeSocketPair(sv)) {
    std::cout << "FAIL: socketpair failed\n";
    return false;
  }

  alyrn::epoll::Loop loop;
  alyrn::epoll::Stream stream(&loop, sv[0]);

  std::array<std::byte, 16> buffer{};
  std::optional<ReadResult> result;
  bool resumed_with_scheduler = false;

  alyrn::coro::SpawnDetach(
      loop, ReadOnce(&stream, &loop, &loop, &buffer, &result, &resumed_with_scheduler));
  loop.RunAfter(alyrn::time::Duration::zero(),
                [&] { alyrn::coro::Spawn(loop, stream.Close()).Detach(); });

  loop.Run();

  ::close(sv[1]);

  return Check(result.has_value(), "cancelled read did not finish") &&
         Check(!result->HasValue(), "cancelled read unexpectedly returned value") &&
         Check(result->Error() == std::errc::operation_canceled,
               "cancelled read did not return ECANCELED") &&
         Check(resumed_with_scheduler, "cancelled read resumed without current scheduler");
}

bool CheckLoopStopCancelsPendingRead() {
  int sv[2] = {-1, -1};
  if (!MakeSocketPair(sv)) {
    std::cout << "FAIL: socketpair failed\n";
    return false;
  }

  alyrn::epoll::Loop loop;
  alyrn::epoll::Stream stream(&loop, sv[0]);

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
         Check(!result->HasValue(), "loop stop unexpectedly completed the read") &&
         Check(result->Error() == std::errc::operation_canceled,
               "loop stop did not return ECANCELED") &&
         Check(resume_count == 1, "loop stop resumed the read continuation more than once") &&
         Check(resumed_with_scheduler, "loop stop resumed the read without scheduler affinity");
}

bool CheckReadableThenCloseResumesOnce() {
  int sv[2] = {-1, -1};
  if (!MakeSocketPair(sv)) {
    std::cout << "FAIL: socketpair failed\n";
    return false;
  }

  alyrn::epoll::Loop loop;
  alyrn::epoll::Stream stream(&loop, sv[0]);

  std::array<std::byte, 16> buffer{};
  std::optional<ReadResult> result;
  int resume_count = 0;
  bool resumed_with_scheduler = false;

  alyrn::coro::SpawnDetach(loop, ReadWithoutQuit(&stream, &loop, &buffer, &result, &resume_count,
                                                 &resumed_with_scheduler));
  loop.RunAfter(alyrn::time::Duration::zero(), [&] {
    const char payload[] = "race";
    (void)(::write(sv[1], payload, sizeof(payload) - 1));
    alyrn::coro::Spawn(loop, stream.Close()).Detach();
  });
  loop.RunAfter(alyrn::time::Milliseconds(10), [&] { loop.RequestStop(); });
  loop.Run();

  ::close(sv[1]);

  if (!Check(result.has_value(), "readable-close race did not finish") ||
      !Check(resume_count == 1, "readable-close race resumed the coroutine more than once") ||
      !Check(resumed_with_scheduler,
             "readable-close race resumed without the captured scheduler")) {
    return false;
  }

  if (result->HasValue()) {
    return Check(**result == 4, "readable-close race returned wrong byte count");
  }
  return Check(result->Error() == std::errc::operation_canceled,
               "readable-close race failed with an unexpected error");
}

bool CheckEchoAlgorithmUsesAsyncStream() {
  int sv[2] = {-1, -1};
  if (!MakeSocketPair(sv)) {
    std::cout << "FAIL: socketpair failed\n";
    return false;
  }

  alyrn::epoll::Loop loop;
  alyrn::epoll::Stream server(&loop, sv[0]);
  alyrn::epoll::Stream client(&loop, sv[1]);

  const char payload[] = "echo-through-async-stream";
  auto bytes = std::as_bytes(std::span(payload, sizeof(payload) - 1));

  std::array<std::byte, 64> server_buffer{};
  std::array<std::byte, 64> client_buffer{};
  std::optional<alyrn::Result<void>> server_result;
  std::optional<alyrn::Result<void>> client_result;
  std::size_t received_size = 0;
  int done_count = 0;

  alyrn::coro::SpawnDetach(loop,
                           EchoServer(&server, &server_buffer, &server_result, &done_count, &loop));
  alyrn::coro::SpawnDetach(loop, EchoClient(&client, bytes, &client_buffer, &client_result,
                                            &received_size, &done_count, &loop));

  loop.Run();

  return Check(server_result.has_value(), "echo server did not finish") &&
         Check(server_result->HasValue(), "echo server returned error") &&
         Check(client_result.has_value(), "echo client did not finish") &&
         Check(client_result->HasValue(), "echo client returned error") &&
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

  alyrn::epoll::Loop loop;
  alyrn::epoll::Stream stream(&loop, sv[0]);

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
         Check(!read_result->HasValue(), "read after close unexpectedly succeeded") &&
         Check(read_result->Error() == std::errc::bad_file_descriptor,
               "read after close did not return EBADF") &&
         Check(write_result.has_value(), "write after close did not finish") &&
         Check(!write_result->HasValue(), "write after close unexpectedly succeeded") &&
         Check(write_result->Error() == std::errc::bad_file_descriptor,
               "write after close did not return EBADF") &&
         Check(peer_read == 0, "peer did not observe local close");
}

bool CheckShutdownKeepsReadOpen() {
  int sv[2] = {-1, -1};
  if (!MakeSocketPair(sv)) {
    std::cout << "FAIL: socketpair failed\n";
    return false;
  }

  constexpr char kReply[] = "reply";
  if (::write(sv[1], kReply, sizeof(kReply) - 1) != static_cast<ssize_t>(sizeof(kReply) - 1)) {
    std::cout << "FAIL: peer write failed\n";
    ::close(sv[0]);
    ::close(sv[1]);
    return false;
  }

  alyrn::epoll::Loop loop;
  alyrn::epoll::Stream stream(&loop, sv[0]);
  std::array<std::byte, 16> read_buffer{};
  constexpr char kWrite[] = "must-not-send";
  auto write_buffer = std::as_bytes(std::span(kWrite, sizeof(kWrite) - 1));
  std::optional<WriteResult> first_shutdown;
  std::optional<WriteResult> second_shutdown;
  std::optional<WriteResult> write_result;
  std::optional<ReadResult> read_result;

  alyrn::coro::SpawnDetach(
      loop, ShutdownThenReadAndWrite(&stream, &loop, &read_buffer, write_buffer, &first_shutdown,
                                     &second_shutdown, &write_result, &read_result));
  loop.Run();

  std::array<char, 1> peer_buffer{};
  const ssize_t peer_read = ::read(sv[1], peer_buffer.data(), peer_buffer.size());
  ::close(sv[1]);

  const std::string_view actual(reinterpret_cast<const char*>(read_buffer.data()),
                                sizeof(kReply) - 1);
  return Check(first_shutdown.has_value() && first_shutdown->HasValue(),
               "first Shutdown failed") &&
         Check(second_shutdown.has_value() && second_shutdown->HasValue(),
               "second Shutdown was not idempotent") &&
         Check(write_result.has_value() && !write_result->HasValue(),
               "Write after Shutdown unexpectedly succeeded") &&
         Check(write_result->Error() == std::errc::broken_pipe,
               "Write after Shutdown did not return EPIPE") &&
         Check(read_result.has_value() && read_result->HasValue() &&
                   **read_result == sizeof(kReply) - 1,
               "Read after Shutdown did not remain usable") &&
         Check(actual == std::string_view(kReply, sizeof(kReply) - 1),
               "Read after Shutdown returned wrong payload") &&
         Check(peer_read == 0, "peer did not observe Shutdown EOF");
}

}  // namespace

int main() {
  if (!CheckStreamAffinityIsEnforcedInRelease()) return 1;
  if (!CheckImmediateRead()) return 1;
  if (!CheckImmediateWrite()) return 1;
  if (!CheckPendingRead()) return 1;
  if (!CheckReadDeadline()) return 1;
  if (!CheckOwnedRecvReturnsBuffer()) return 1;
  if (!CheckPooledRecvCopiesPayload()) return 1;
  if (!CheckOwnedRecvCloseReturnsBuffer()) return 1;
  if (!CheckCloseCancelsPendingRead()) return 1;
  if (!CheckLoopStopCancelsPendingRead()) return 1;
  if (!CheckReadableThenCloseResumesOnce()) return 1;
  if (!CheckEchoAlgorithmUsesAsyncStream()) return 1;
  if (!CheckCloseRejectsLaterSubmit()) return 1;
  if (!CheckShutdownKeepsReadOpen()) return 1;

  std::cout << "epoll stream smoke: PASS\n";
  return 0;
}
