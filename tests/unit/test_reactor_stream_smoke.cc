// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

#include <array>
#include <cstddef>
#include <cstring>
#include <expected>
#include <iostream>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "coropact/base/error.h"
#include "coropact/coro/scheduler.h"
#include "coropact/coro/spawn.h"
#include "coropact/coro/task.h"
#include "coropact/io/stream_algorithms.h"
#include "coropact/net/event_loop.h"
#include "coropact/net/event_loop_scheduler.h"
#include "coropact/net/reactor_stream.h"

namespace {

using ReadResult = coropact::base::Result<std::size_t>;
using WriteResult = coropact::base::Result<std::size_t>;

static_assert(coropact::io::AsyncStream<coropact::net::ReactorStream>);

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

coropact::coro::Task<void> ReadOnce(coropact::net::ReactorStream* stream, coropact::net::EventLoop* loop,
                                coropact::net::EventLoopScheduler* scheduler,
                                std::array<std::byte, 16>* buffer, std::optional<ReadResult>* out,
                                bool* resumed_with_scheduler) {
  ReadResult result = co_await stream->ReadSome(*buffer);
  *resumed_with_scheduler = coropact::coro::Scheduler::Current() == scheduler;
  out->emplace(std::move(result));
  loop->Quit();
}

coropact::coro::Task<void> ReadBufferOnce(coropact::net::ReactorStream* stream, coropact::net::EventLoop* loop,
                                      coropact::net::EventLoopScheduler* scheduler,
                                      coropact::io::Buffer* buffer, std::optional<ReadResult>* out,
                                      bool* resumed_with_scheduler) {
  ReadResult result = co_await stream->ReadSome(*buffer, 32);
  *resumed_with_scheduler = coropact::coro::Scheduler::Current() == scheduler;
  out->emplace(std::move(result));
  loop->Quit();
}

coropact::coro::Task<void> WriteOnce(coropact::net::ReactorStream* stream, coropact::net::EventLoop* loop,
                                 coropact::net::EventLoopScheduler* scheduler,
                                 std::span<const std::byte> payload,
                                 std::optional<WriteResult>* out, bool* resumed_with_scheduler) {
  WriteResult result = co_await stream->WriteSome(payload);
  *resumed_with_scheduler = coropact::coro::Scheduler::Current() == scheduler;
  out->emplace(std::move(result));
  loop->Quit();
}

coropact::coro::Task<void> WriteBufferOnce(coropact::net::ReactorStream* stream, coropact::net::EventLoop* loop,
                                       coropact::net::EventLoopScheduler* scheduler,
                                       coropact::io::Buffer* buffer, std::optional<WriteResult>* out,
                                       bool* resumed_with_scheduler) {
  WriteResult result = co_await stream->WriteSome(*buffer);
  *resumed_with_scheduler = coropact::coro::Scheduler::Current() == scheduler;
  out->emplace(std::move(result));
  loop->Quit();
}

coropact::coro::Task<void> EchoServer(coropact::net::ReactorStream* stream,
                                  std::array<std::byte, 64>* scratch,
                                  std::optional<coropact::base::Result<void>>* out, int* done_count,
                                  coropact::net::EventLoop* loop) {
  out->emplace(co_await coropact::io::EchoOnce(*stream, *scratch));
  if (++(*done_count) == 2) {
    loop->Quit();
  }
}

coropact::coro::Task<void> EchoClient(coropact::net::ReactorStream* stream,
                                  std::span<const std::byte> payload,
                                  std::array<std::byte, 64>* received,
                                  std::optional<coropact::base::Result<void>>* out,
                                  std::size_t* received_size, int* done_count,
                                  coropact::net::EventLoop* loop) {
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

coropact::coro::Task<void> CloseThenSubmit(coropact::net::ReactorStream* stream, coropact::net::EventLoop* loop,
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

  coropact::net::EventLoop loop;
  coropact::net::ReactorStream stream(&loop, sv[0]);
  coropact::net::EventLoopScheduler scheduler(&loop);

  std::array<std::byte, 16> buffer{};
  std::optional<ReadResult> result;
  bool resumed_with_scheduler = false;

  coropact::coro::Spawn(scheduler,
                    ReadOnce(&stream, &loop, &scheduler, &buffer, &result, &resumed_with_scheduler))
      .Detach();
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

  coropact::net::EventLoop loop;
  coropact::net::ReactorStream stream(&loop, sv[0]);
  coropact::net::EventLoopScheduler scheduler(&loop);

  const char payload[] = "write";
  auto bytes = std::as_bytes(std::span(payload, sizeof(payload) - 1));
  std::optional<WriteResult> result;
  bool resumed_with_scheduler = false;

  coropact::coro::Spawn(scheduler,
                    WriteOnce(&stream, &loop, &scheduler, bytes, &result, &resumed_with_scheduler))
      .Detach();
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

  coropact::net::EventLoop loop;
  coropact::net::ReactorStream stream(&loop, sv[0]);
  coropact::net::EventLoopScheduler scheduler(&loop);

  std::array<std::byte, 16> buffer{};
  std::optional<ReadResult> result;
  bool resumed_with_scheduler = false;

  coropact::coro::Spawn(scheduler,
                    ReadOnce(&stream, &loop, &scheduler, &buffer, &result, &resumed_with_scheduler))
      .Detach();

  const char payload[] = "pending";
  loop.QueueInLoop([fd = sv[1]] {
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

  coropact::net::EventLoop loop;
  coropact::net::ReactorStream stream(&loop, sv[0]);
  coropact::net::EventLoopScheduler scheduler(&loop);

  coropact::io::Buffer buffer(4);
  std::optional<ReadResult> result;
  bool resumed_with_scheduler = false;

  coropact::coro::Spawn(scheduler, ReadBufferOnce(&stream, &loop, &scheduler, &buffer, &result,
                                              &resumed_with_scheduler))
      .Detach();
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

bool CheckWriteFromIoBuffer() {
  int sv[2] = {-1, -1};
  if (!MakeSocketPair(sv)) {
    std::cout << "FAIL: socketpair failed\n";
    return false;
  }

  coropact::net::EventLoop loop;
  coropact::net::ReactorStream stream(&loop, sv[0]);
  coropact::net::EventLoopScheduler scheduler(&loop);

  const std::string payload = "write-from-io-buffer";
  coropact::io::Buffer buffer(5);
  buffer.Append(payload.substr(0, 7));
  buffer.Append(payload.substr(7));

  std::optional<WriteResult> result;
  bool resumed_with_scheduler = false;

  coropact::coro::Spawn(scheduler, WriteBufferOnce(&stream, &loop, &scheduler, &buffer, &result,
                                               &resumed_with_scheduler))
      .Detach();
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

  coropact::net::EventLoop loop;
  coropact::net::ReactorStream stream(&loop, sv[0]);
  coropact::net::EventLoopScheduler scheduler(&loop);

  std::array<std::byte, 16> buffer{};
  std::optional<ReadResult> result;
  bool resumed_with_scheduler = false;

  coropact::coro::Spawn(scheduler,
                    ReadOnce(&stream, &loop, &scheduler, &buffer, &result, &resumed_with_scheduler))
      .Detach();
  loop.QueueInLoop([&] { coropact::coro::Spawn(scheduler, stream.Close()).Detach(); });

  loop.Loop();

  ::close(sv[1]);

  return Check(result.has_value(), "cancelled read did not finish") &&
         Check(!result->has_value(), "cancelled read unexpectedly returned value") &&
         Check(result->error() == std::errc::operation_canceled,
               "cancelled read did not return ECANCELED") &&
         Check(resumed_with_scheduler, "cancelled read resumed without current scheduler");
}

bool CheckEchoAlgorithmUsesAsyncStream() {
  int sv[2] = {-1, -1};
  if (!MakeSocketPair(sv)) {
    std::cout << "FAIL: socketpair failed\n";
    return false;
  }

  coropact::net::EventLoop loop;
  coropact::net::ReactorStream server(&loop, sv[0]);
  coropact::net::ReactorStream client(&loop, sv[1]);
  coropact::net::EventLoopScheduler scheduler(&loop);

  const char payload[] = "echo-through-async-stream";
  auto bytes = std::as_bytes(std::span(payload, sizeof(payload) - 1));

  std::array<std::byte, 64> server_buffer{};
  std::array<std::byte, 64> client_buffer{};
  std::optional<coropact::base::Result<void>> server_result;
  std::optional<coropact::base::Result<void>> client_result;
  std::size_t received_size = 0;
  int done_count = 0;

  coropact::coro::Spawn(scheduler,
                    EchoServer(&server, &server_buffer, &server_result, &done_count, &loop))
      .Detach();
  coropact::coro::Spawn(scheduler, EchoClient(&client, bytes, &client_buffer, &client_result,
                                          &received_size, &done_count, &loop))
      .Detach();

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

  coropact::net::EventLoop loop;
  coropact::net::ReactorStream stream(&loop, sv[0]);
  coropact::net::EventLoopScheduler scheduler(&loop);

  std::array<std::byte, 16> read_buffer{};
  const char payload[] = "after-close";
  auto bytes = std::as_bytes(std::span(payload, sizeof(payload) - 1));
  std::optional<ReadResult> read_result;
  std::optional<WriteResult> write_result;

  coropact::coro::Spawn(
      scheduler, CloseThenSubmit(&stream, &loop, &read_buffer, bytes, &read_result, &write_result))
      .Detach();

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
  if (!CheckImmediateRead()) return 1;
  if (!CheckImmediateWrite()) return 1;
  if (!CheckPendingRead()) return 1;
  if (!CheckReadIntoIoBuffer()) return 1;
  if (!CheckWriteFromIoBuffer()) return 1;
  if (!CheckCloseCancelsPendingRead()) return 1;
  if (!CheckEchoAlgorithmUsesAsyncStream()) return 1;
  if (!CheckCloseRejectsLaterSubmit()) return 1;

  std::cout << "reactor stream smoke: PASS\n";
  return 0;
}
