// SPDX-License-Identifier: MIT

/*
 * Native kqueue TriggerMode::kOneShot behaviour on a real BSD/Darwin host.
 *
 * Unlike the Linux fake_kqueue shim suite, this file never inspects changelist
 * logs. It only asserts observable delivery: callback counts, Channel interest
 * after retirement, and whether a second arm is required for the next fire.
 */

#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <iostream>
#include <thread>

#include "alyrn/detail/check.h"
#include "alyrn/kqueue/detail/channel.h"
#include "alyrn/kqueue/loop.h"
#include "alyrn/kqueue/options.h"
#include "alyrn/net/detail/socket.h"

namespace {

using alyrn::kqueue::Loop;
using alyrn::kqueue::TriggerMode;
using alyrn::kqueue::detail::Channel;

bool Check(bool condition, const char* message) {
  if (!condition) {
    std::cout << "FAIL: " << message << '\n';
    return false;
  }
  return true;
}

void ConfigureFd(int fd) {
  ALYRN_CHECK(alyrn::net::SetNonBlocking(fd).has_value(), "SetNonBlocking failed");
  ALYRN_CHECK(alyrn::net::SetCloseOnExec(fd).has_value(), "SetCloseOnExec failed");
}

class ScopedPipe {
public:
  ScopedPipe() {
    ALYRN_CHECK(::pipe(fds_) == 0, "pipe creation failed");
    ConfigureFd(fds_[0]);
    ConfigureFd(fds_[1]);
  }

  ~ScopedPipe() {
    if (fds_[0] >= 0) {
      ::close(fds_[0]);
    }
    if (fds_[1] >= 0) {
      ::close(fds_[1]);
    }
  }

  ScopedPipe(const ScopedPipe&) = delete;
  ScopedPipe& operator=(const ScopedPipe&) = delete;

  int ReadFd() const { return fds_[0]; }
  int WriteFd() const { return fds_[1]; }

private:
  int fds_[2]{-1, -1};
};

class ScopedSocketPair {
public:
  ScopedSocketPair() {
    ALYRN_CHECK(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds_) == 0, "socketpair failed");
    ConfigureFd(fds_[0]);
    ConfigureFd(fds_[1]);
  }

  ~ScopedSocketPair() {
    if (fds_[0] >= 0) {
      ::close(fds_[0]);
    }
    if (fds_[1] >= 0) {
      ::close(fds_[1]);
    }
  }

  ScopedSocketPair(const ScopedSocketPair&) = delete;
  ScopedSocketPair& operator=(const ScopedSocketPair&) = delete;

  int Local() const { return fds_[0]; }
  int Peer() const { return fds_[1]; }

private:
  int fds_[2]{-1, -1};
};

void Write(int fd, const char* bytes, std::size_t length) {
  std::size_t offset = 0;
  while (offset < length) {
    const ssize_t written = ::write(fd, bytes + offset, length - offset);
    if (written > 0) {
      offset += static_cast<std::size_t>(written);
      continue;
    }
    if (written < 0 && errno == EINTR) {
      continue;
    }
    ALYRN_CHECK(false, "write failed");
  }
}

void Drain(int fd) {
  char buffer[256];
  for (;;) {
    const ssize_t bytes = ::read(fd, buffer, sizeof(buffer));
    if (bytes > 0) {
      continue;
    }
    if (bytes < 0 && errno == EINTR) {
      continue;
    }
    ALYRN_CHECK(bytes < 0 && (errno == EAGAIN || errno == EWOULDBLOCK), "drain read failed");
    return;
  }
}

/*
 * One-shot delivery consumes the filter. Extra readable data without a re-arm
 * must not produce a second callback; a separate always-writable sentinel is
 * what forces the next poll turn so that absence can be observed.
 */
bool CheckOneShotDoesNotRefireWithoutRearm() {
  Loop loop;
  ScopedPipe data;
  ScopedPipe sentinel_pipe;

  struct Observation {
    int reads{0};
    int sentinels{0};
  } observation;

  Channel data_channel{&loop, data.ReadFd()};
  Channel sentinel{&loop, sentinel_pipe.WriteFd()};

  struct Context {
    Observation* observation;
    Loop* loop;
    Channel* sentinel;
    int data_write_fd;
    int data_read_fd;
  } context{&observation, &loop, &sentinel, data.WriteFd(), data.ReadFd()};

  data_channel.SetTriggerMode(TriggerMode::kOneShot);
  data_channel.SetReadCallback(
      [](void* raw) noexcept {
        auto* ctx = static_cast<Context*>(raw);
        ++ctx->observation->reads;
        Drain(ctx->data_read_fd);
        Write(ctx->data_write_fd, "x", 1);
        ctx->sentinel->SetTriggerMode(TriggerMode::kOneShot);
        ctx->sentinel->EnableWriting();
      },
      &context);

  sentinel.SetTriggerMode(TriggerMode::kOneShot);
  sentinel.SetWriteCallback(
      [](void* raw) noexcept {
        auto* ctx = static_cast<Context*>(raw);
        ++ctx->observation->sentinels;
        ctx->loop->RequestStop();
      },
      &context);

  Write(data.WriteFd(), "a", 1);
  data_channel.EnableReading();
  loop.Run();

  const bool read_once = observation.reads == 1;
  const bool sentinel_fired = observation.sentinels == 1;
  const bool interest_cleared = data_channel.IsNoneEvent();

  data_channel.Remove();
  if (!sentinel.IsNoneEvent()) {
    sentinel.DisableAll();
  }
  if (sentinel.IsRegistered()) {
    sentinel.Remove();
  }

  return Check(read_once, "one-shot read must fire exactly once without re-arm") &&
         Check(sentinel_fired, "sentinel must run a second poll turn") &&
         Check(interest_cleared, "one-shot delivery must clear Channel interest");
}

bool CheckReArmDeliversAgain() {
  Loop loop;
  ScopedPipe pipe;

  struct Observation {
    int reads{0};
  } observation;

  Channel channel{&loop, pipe.ReadFd()};

  struct Context {
    Observation* observation;
    Loop* loop;
    Channel* channel;
    int write_fd;
    int read_fd;
  } context{&observation, &loop, &channel, pipe.WriteFd(), pipe.ReadFd()};

  channel.SetTriggerMode(TriggerMode::kOneShot);
  channel.SetReadCallback(
      [](void* raw) noexcept {
        auto* ctx = static_cast<Context*>(raw);
        ++ctx->observation->reads;
        Drain(ctx->read_fd);
        if (ctx->observation->reads == 1) {
          Write(ctx->write_fd, "b", 1);
          ctx->channel->EnableReading();
          return;
        }
        ctx->loop->RequestStop();
      },
      &context);

  Write(pipe.WriteFd(), "a", 1);
  channel.EnableReading();
  loop.Run();

  channel.DisableAll();
  channel.Remove();

  return Check(observation.reads == 2, "re-armed one-shot read must fire a second time");
}

bool CheckRetiredFilterRemoveIsSafe() {
  Loop loop;
  ScopedPipe pipe;

  struct Observation {
    int reads{0};
  } observation;

  Channel channel{&loop, pipe.ReadFd()};

  struct Context {
    Observation* observation;
    Loop* loop;
  } context{&observation, &loop};

  channel.SetTriggerMode(TriggerMode::kOneShot);
  channel.SetReadCallback(
      [](void* raw) noexcept {
        auto* ctx = static_cast<Context*>(raw);
        ++ctx->observation->reads;
        ctx->loop->RequestStop();
      },
      &context);

  Write(pipe.WriteFd(), "a", 1);
  channel.EnableReading();
  loop.Run();

  // Interest was consumed by delivery; Remove must not issue EV_DELETE.
  const bool interest_cleared = channel.IsNoneEvent();
  channel.Remove();

  return Check(observation.reads == 1, "one-shot read must dispatch") &&
         Check(interest_cleared, "retired one-shot must leave an empty interest set");
}

/*
 * Level-triggered contrast: unread data keeps the condition true, so a second
 * poll turn delivers again without EnableReading().
 */
bool CheckLevelTriggeredRefiresWhileReadable() {
  Loop loop;
  ScopedPipe pipe;

  struct Observation {
    int reads{0};
  } observation;

  Channel channel{&loop, pipe.ReadFd()};

  struct Context {
    Observation* observation;
    Loop* loop;
    int read_fd;
  } context{&observation, &loop, pipe.ReadFd()};

  channel.SetReadCallback(
      [](void* raw) noexcept {
        auto* ctx = static_cast<Context*>(raw);
        ++ctx->observation->reads;
        if (ctx->observation->reads == 1) {
          return;
        }
        Drain(ctx->read_fd);
        ctx->loop->RequestStop();
      },
      &context);

  Write(pipe.WriteFd(), "abc", 3);
  channel.EnableReading();
  loop.Run();

  channel.DisableAll();
  channel.Remove();

  return Check(observation.reads >= 2,
               "level-triggered read must fire again while data remains unread");
}

bool CheckBothFiltersRetireInOneTurn() {
  Loop loop;
  ScopedSocketPair pair;

  struct Observation {
    int reads{0};
    int writes{0};
  } observation;

  Channel channel{&loop, pair.Local()};

  struct Context {
    Observation* observation;
    Loop* loop;
  } context{&observation, &loop};

  channel.SetTriggerMode(TriggerMode::kOneShot);
  channel.SetReadCallback(
      [](void* raw) noexcept { ++static_cast<Context*>(raw)->observation->reads; }, &context);
  channel.SetWriteCallback(
      [](void* raw) noexcept {
        auto* ctx = static_cast<Context*>(raw);
        ++ctx->observation->writes;
        ctx->loop->RequestStop();
      },
      &context);

  channel.EnableReading();
  channel.EnableWriting();
  Write(pair.Peer(), "a", 1);
  loop.Run();

  const bool read_once = observation.reads == 1;
  const bool write_once = observation.writes == 1;
  const bool interest_cleared = channel.IsNoneEvent();

  channel.Remove();

  return Check(read_once, "merged turn must run the read callback exactly once") &&
         Check(write_once, "merged turn must run the write callback exactly once") &&
         Check(interest_cleared, "merged one-shot turn must clear both interest bits");
}

bool CheckRequestStopWakesBlockedLoop() {
  Loop loop;
  std::atomic<bool> started{false};

  std::thread stopper([&] {
    while (!started.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    // Give Run() a moment to enter kevent before requesting stop.
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    loop.RequestStop();
  });

  started.store(true, std::memory_order_release);
  loop.Run();
  stopper.join();

  return Check(loop.State() == ::alyrn::backend::LoopState::kStopped,
               "RequestStop must wake a blocked Loop");
}

}  // namespace

int main() {
  if (!CheckOneShotDoesNotRefireWithoutRearm()) return 1;
  if (!CheckReArmDeliversAgain()) return 1;
  if (!CheckRetiredFilterRemoveIsSafe()) return 1;
  if (!CheckLevelTriggeredRefiresWhileReadable()) return 1;
  if (!CheckBothFiltersRetireInOneTurn()) return 1;
  if (!CheckRequestStopWakesBlockedLoop()) return 1;
  std::cout << "kqueue native one-shot smoke: PASS\n";
  return 0;
}
