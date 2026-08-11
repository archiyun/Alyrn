// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <sys/uio.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <exception>
#include <iostream>
#include <limits>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "coropact/io/buffer.h"
#include "coropact/io/read_into.h"

namespace {

static_assert(std::same_as<coropact::io::ReadIntoOutcome, coropact::net::ReadIntoOutcome>);

bool Expect(bool condition, std::string_view message) {
  if (!condition) {
    std::cerr << "[FAIL] " << message << '\n';
    return false;
  }
  return true;
}

bool ExpectChildAbort(void (*entry)(), std::string_view message) {
  const pid_t child = ::fork();
  if (child < 0) {
    return Expect(false, "fork failed for buffer invariant test");
  }
  if (child == 0) {
    (void)::freopen("/dev/null", "w", stderr);
    entry();
    ::_exit(0);
  }

  int status = 0;
  while (::waitpid(child, &status, 0) < 0 && errno == EINTR) {
  }
  return Expect(WIFSIGNALED(status), message) &&
         Expect(WTERMSIG(status) == SIGABRT, "buffer invariant must terminate with SIGABRT");
}

std::string Gather(coropact::io::Buffer& buffer) {
  std::string out;
  for (const iovec& iov : buffer.ReadableIov(32)) {
    out.append(static_cast<const char*>(iov.iov_base), iov.iov_len);
  }
  return out;
}

void CopyIntoIov(const std::vector<iovec>& iovs, std::string_view text) {
  std::size_t copied = 0;
  for (const iovec& iov : iovs) {
    if (copied == text.size()) break;
    const std::size_t n = std::min(iov.iov_len, text.size() - copied);
    std::memcpy(iov.iov_base, text.data() + copied, n);
    copied += n;
  }
}

bool AppendAndDrainPreserveOrder() {
  coropact::io::Buffer buffer(4);

  buffer.Append("ab");
  buffer.Append("cdefg");

  bool ok = Expect(buffer.ReadableBytes() == 7, "append should update readable byte count");
  ok &= Expect(Gather(buffer) == "abcdefg", "append should preserve byte order across blocks");

  buffer.Drain(3);
  ok &= Expect(buffer.ReadableBytes() == 4, "drain should update readable byte count");
  ok &= Expect(Gather(buffer) == "defg", "drain should remove only the prefix");

  buffer.Drain(1);
  ok &= Expect(Gather(buffer) == "efg", "drain across block boundary should preserve suffix");
  return ok;
}

bool PreparedWriteAppendsAtTailOnly() {
  coropact::io::Buffer buffer(4);

  buffer.Append("abcd");
  buffer.Append("ef");

  auto iovs = buffer.PrepareWrite(4, 2);
  if (!Expect(iovs.size() == 2, "prepare write should expose tail writable ranges")) {
    return false;
  }

  CopyIntoIov(iovs, "GHIJ");
  buffer.CommitWrite(4);

  return Expect(Gather(buffer) == "abcdefGHIJ",
                "committed write should append after the full logical buffer");
}

bool AbortWriteDiscardsReservation() {
  coropact::io::Buffer buffer(4);

  auto iovs = buffer.PrepareWrite(4, 1);
  if (!Expect(iovs.size() == 1, "prepare write should reserve a writable block")) {
    return false;
  }

  CopyIntoIov(iovs, "drop");
  buffer.AbortWrite();

  bool ok = Expect(buffer.Empty(), "aborted write should not create readable bytes");
  buffer.Append("keep");
  ok &= Expect(Gather(buffer) == "keep", "buffer should remain usable after abort");
  return ok;
}

bool MoveLeavesSourceEmpty() {
  coropact::io::Buffer source(4);
  source.Append("hello");

  coropact::io::Buffer moved(std::move(source));

  bool ok = Expect(source.Empty(), "move construction should leave source empty");
  ok &= Expect(source.ReadableBytes() == 0, "moved-from source should report zero readable bytes");
  ok &= Expect(Gather(moved) == "hello", "moved buffer should keep original data");

  coropact::io::Buffer assigned(4);
  assigned.Append("old");
  assigned = std::move(moved);

  ok &= Expect(moved.Empty(), "move assignment should leave source empty");
  ok &= Expect(Gather(assigned) == "hello", "move assignment should replace destination data");
  return ok;
}

bool EmptyReservationDoesNotHideLaterData() {
  coropact::io::Buffer buffer(4);

  auto iovs = buffer.PrepareWrite(4, 1);
  if (!Expect(iovs.size() == 1, "prepare write should expose a writable block")) {
    return false;
  }

  buffer.CommitWrite(0);
  buffer.Append("abc");

  return Expect(buffer.ContiguousText() == "abc",
                "empty reserved block should not hide later readable data");
}

bool ZeroHintUsesBlockSize() {
  coropact::io::Buffer buffer(4);

  auto iovs = buffer.PrepareWrite(0, 1);
  const bool ok = Expect(iovs.size() == 1 && iovs.front().iov_len == 4,
                         "zero write hint should reserve one default-sized block");
  buffer.AbortWrite();
  return ok;
}

bool FailedPreparationReleasesReservation() {
  coropact::io::Buffer buffer(4);
  bool threw = false;
  try {
    (void)buffer.PrepareWrite(1, std::numeric_limits<std::size_t>::max());
  } catch (const std::exception&) {
    threw = true;
  }

  if (!Expect(threw, "oversized iovec preparation should fail")) {
    return false;
  }

  auto retry = buffer.PrepareWrite(4, 1);
  const bool reusable =
      Expect(retry.size() == 1, "failed preparation must not retain a write reservation");
  buffer.AbortWrite();
  return reusable;
}

void BufferCommitWithoutReservation() {
  coropact::io::Buffer buffer(4);
  buffer.CommitWrite(1);
}

void BufferNestedPreparation() {
  coropact::io::Buffer buffer(4);
  (void)buffer.PrepareWrite(4, 1);
  (void)buffer.PrepareWrite(4, 1);
}

void BufferCommitBeyondReservation() {
  coropact::io::Buffer buffer(4);
  auto iovs = buffer.PrepareWrite(4, 1);
  buffer.CommitWrite(iovs.front().iov_len + 1);
}

void BufferMutationDuringReservation() {
  coropact::io::Buffer buffer(4);
  (void)buffer.PrepareWrite(4, 1);
  buffer.Append("x");
}

bool ReservationInvariantsFailClosed() {
  return ExpectChildAbort(&BufferCommitWithoutReservation,
                          "Buffer CommitWrite without PrepareWrite must terminate in Release") &&
         ExpectChildAbort(&BufferNestedPreparation,
                          "nested Buffer PrepareWrite must terminate in Release") &&
         ExpectChildAbort(&BufferCommitBeyondReservation,
                          "Buffer CommitWrite beyond reservation must terminate in Release") &&
         ExpectChildAbort(&BufferMutationDuringReservation,
                          "Buffer mutation during reservation must terminate in Release");
}

}  // namespace

int main() {
  bool ok = true;
  ok &= AppendAndDrainPreserveOrder();
  ok &= PreparedWriteAppendsAtTailOnly();
  ok &= AbortWriteDiscardsReservation();
  ok &= MoveLeavesSourceEmpty();
  ok &= EmptyReservationDoesNotHideLaterData();
  ok &= ZeroHintUsesBlockSize();
  ok &= FailedPreparationReleasesReservation();
  ok &= ReservationInvariantsFailClosed();

  if (!ok) return 1;

  std::cout << "[PASS] io_buffer_smoke_test\n";
  return 0;
}
