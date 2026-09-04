#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <csignal>
#include <cstdio>
#include <optional>

#include "alyrn/coro/channel.h"
#include "alyrn/coro/scheduler.h"
#include "alyrn/coro/spawn.h"
#include "alyrn/coro/task.h"
#include "alyrn/coro/work.h"
#include "alyrn/detail/check.h"

namespace {

using alyrn::coro::Channel;
using alyrn::coro::Scheduler;
using alyrn::coro::Spawn;
using alyrn::coro::Task;
using alyrn::coro::Work;
using alyrn::coro::WorkQueue;

class DrainScheduler final : public Scheduler {
public:
  void Schedule(Work* work) noexcept override {
    const bool queued = queue_.PushBack(work);
    assert(queued);
  }

  bool DrainOne() {
    Work* work = queue_.PopFront();
    if (work == nullptr) return false;
    Run(work);
    return true;
  }

  void Drain() {
    while (DrainOne()) {
    }
  }

private:
  WorkQueue queue_;
};

Task<void> BufferedCase(Channel<int>& channel, bool& passed) {
  ALYRN_CHECK((co_await (channel << 1)).HasValue(), "first buffered send failed");
  ALYRN_CHECK((co_await (channel << 2)).HasValue(), "second buffered send failed");
  channel.Close();

  std::optional<int> first;
  std::optional<int> second;
  ALYRN_CHECK((co_await (channel >> first)).HasValue() && first.has_value() && *first == 1,
              "first buffered value was not FIFO");
  ALYRN_CHECK((co_await (channel >> second)).HasValue() && second.has_value() && *second == 2,
              "second buffered value was not FIFO");

  std::optional<int> closed{99};
  ALYRN_CHECK((co_await (channel >> closed)).HasValue() && !closed.has_value(),
              "closed empty channel did not report end of stream");
  passed = true;
}

Task<void> SendOne(Channel<int>& channel, bool& passed) {
  const auto sent = co_await (channel << 42);
  ALYRN_CHECK(sent.HasValue(), "rendezvous send failed");
  passed = true;
}

Task<void> ReceiveOne(Channel<int>& channel, bool& passed) {
  std::optional<int> received;
  const auto result = co_await (channel >> received);
  ALYRN_CHECK(result.HasValue() && received.has_value() && *received == 42,
              "rendezvous receive failed");
  passed = true;
}

Task<void> WaitForClose(Channel<int>& channel, bool& passed) {
  std::optional<int> received{99};
  const auto result = co_await (channel >> received);
  ALYRN_CHECK(result.HasValue() && !received.has_value(),
              "Close did not wake the pending receiver");
  passed = true;
}

Task<void> Close(Channel<int>& channel) {
  channel.Close();
  co_return;
}

Task<void> WaitForever(Channel<int>& channel) {
  std::optional<int> received;
  [[maybe_unused]] const auto result = co_await (channel >> received);
}

Task<void> SendOnClosed(Channel<int>& channel) {
  channel.Close();
  [[maybe_unused]] const auto result = co_await (channel << 1);
}

Task<void> CloseTwice(Channel<int>& channel) {
  channel.Close();
  channel.Close();
  co_return;
}

bool ExpectChildAbort(void (*entry)(), const char* message) {
  const pid_t child = ::fork();
  if (child < 0) return false;
  if (child == 0) {
    entry();
    ::_exit(0);
  }

  int status = 0;
  while (::waitpid(child, &status, 0) < 0 && errno == EINTR) {
  }
  const bool aborted = WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT;
  if (!aborted) std::fprintf(stderr, "FAIL: %s\n", message);
  return aborted;
}

bool TestBufferedAndClose() {
  DrainScheduler scheduler;
  Channel<int> channel{scheduler, 2};
  bool passed = false;
  auto task = Spawn(scheduler, BufferedCase(channel, passed));
  scheduler.Drain();
  task.Wait();
  return passed;
}

bool TestUnbufferedRendezvous() {
  DrainScheduler scheduler;
  Channel<int> channel{scheduler, 0};
  bool sent = false;
  bool received = false;
  auto sender = Spawn(scheduler, SendOne(channel, sent));
  auto receiver = Spawn(scheduler, ReceiveOne(channel, received));
  scheduler.Drain();
  sender.Wait();
  receiver.Wait();
  auto closer = Spawn(scheduler, Close(channel));
  scheduler.Drain();
  closer.Wait();
  return sent && received;
}

bool TestCloseWakesReceiver() {
  DrainScheduler scheduler;
  Channel<int> channel{scheduler, 1};
  bool received = false;
  auto receiver = Spawn(scheduler, WaitForClose(channel, received));
  ALYRN_CHECK(scheduler.DrainOne(), "receiver root was not scheduled");
  auto closer = Spawn(scheduler, Close(channel));
  scheduler.Drain();
  receiver.Wait();
  closer.Wait();
  return received;
}

void TriggerSendOnClosed() {
  DrainScheduler scheduler;
  Channel<int> channel{scheduler, 0};
  auto task = Spawn(scheduler, SendOnClosed(channel));
  scheduler.Drain();
  task.Wait();
}

void TriggerCloseTwice() {
  DrainScheduler scheduler;
  Channel<int> channel{scheduler, 0};
  auto task = Spawn(scheduler, CloseTwice(channel));
  scheduler.Drain();
  task.Wait();
}

void TriggerInvalidCapacity() {
  DrainScheduler scheduler;
  Channel<int> channel{scheduler, 3};
}

void TriggerCloseWithPendingSender() {
  DrainScheduler scheduler;
  Channel<int> channel{scheduler, 0};
  bool sent = false;
  auto sender = Spawn(scheduler, SendOne(channel, sent));
  ALYRN_CHECK(scheduler.DrainOne(), "sender root was not scheduled");
  auto closer = Spawn(scheduler, Close(channel));
  scheduler.Drain();
  sender.Wait();
  closer.Wait();
}

bool TestClosePanics() {
  return ExpectChildAbort(&TriggerSendOnClosed, "send on closed channel must panic") &&
         ExpectChildAbort(&TriggerCloseTwice, "closing a channel twice must panic") &&
         ExpectChildAbort(&TriggerCloseWithPendingSender,
                          "closing with a pending sender must panic");
}

bool TestCapacityContract() {
  return ExpectChildAbort(&TriggerInvalidCapacity, "non-power-of-two channel capacity must panic");
}

void DestroyChannelWithWaiter() {
  DrainScheduler scheduler;
  std::optional<alyrn::coro::JoinHandle<void>> join;
  {
    Channel<int> channel{scheduler, 1};
    join.emplace(Spawn(scheduler, WaitForever(channel)));
    ALYRN_CHECK(scheduler.DrainOne(), "receiver root was not scheduled");
  }
}

bool TestPendingWaiterDestructionFailsFast() {
  const pid_t child = ::fork();
  if (child < 0) return false;
  if (child == 0) {
    DestroyChannelWithWaiter();
    ::_exit(0);
  }

  int status = 0;
  while (::waitpid(child, &status, 0) < 0 && errno == EINTR) {
  }
  return WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT;
}

}  // namespace

int main() {
  return TestBufferedAndClose() && TestUnbufferedRendezvous() && TestCloseWakesReceiver() &&
                 TestClosePanics() && TestCapacityContract() &&
                 TestPendingWaiterDestructionFailsFast()
             ? 0
             : 1;
}
