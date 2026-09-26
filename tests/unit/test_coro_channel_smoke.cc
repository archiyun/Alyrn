#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <csignal>
#include <cstdio>
#include <memory>
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

Task<void> TryBufferedCase(Channel<int>& channel) {
  std::optional<int> value{99};
  ALYRN_CHECK(!channel.TryReceive(value) && value == 99, "would-block changed output");
  ALYRN_CHECK(channel.TrySend(1) && channel.TrySend(2), "buffered TrySend failed");
  ALYRN_CHECK(!channel.TrySend(9), "full channel accepted a value");
  ALYRN_CHECK(channel.TryReceive(value) && value == 1, "TryReceive broke FIFO");
  ALYRN_CHECK(channel.TrySend(3), "buffer did not reuse its free slot");
  channel.Close();
  ALYRN_CHECK(channel.TryReceive(value) && value == 2, "close lost buffered value");
  ALYRN_CHECK(channel.TryReceive(value) && value == 3, "wraparound broke FIFO");
  ALYRN_CHECK(channel.TryReceive(value) && !value, "closed receive is not ready EOF");
  ALYRN_CHECK(channel.TryReceive(value) && !value, "EOF is not repeatable");
  co_return;
}

Task<void> TryMoveOnlyCase(Channel<std::unique_ptr<int>>& channel) {
  auto first = std::make_unique<int>(42);
  ALYRN_CHECK(channel.TrySend(std::move(first)) && !first, "TrySend did not transfer ownership");
  auto rejected = std::make_unique<int>(9);
  ALYRN_CHECK(!channel.TrySend(std::move(rejected)) && !rejected,
              "by-value TrySend must consume its argument even on failure");
  std::optional<std::unique_ptr<int>> value;
  ALYRN_CHECK(channel.TryReceive(value) && **value == 42, "move-only receive lost value");
  ALYRN_CHECK(!channel.TryReceive(value) && **value == 42, "empty receive changed output");
  channel.Close();
  ALYRN_CHECK(channel.TryReceive(value) && !value, "closed receive retained old value");
  co_return;
}

Task<void> TryWakeReceiver(Channel<int>& channel, const bool& received) {
  ALYRN_CHECK(channel.TrySend(42), "TrySend failed to match waiting receiver");
  ALYRN_CHECK(!received, "TrySend resumed receiver inline");
  channel.Close();
  co_return;
}

Task<void> TryWakeSender(Channel<int>& channel, const bool& sent) {
  std::optional<int> value;
  ALYRN_CHECK(channel.TryReceive(value) && value == 42, "TryReceive missed waiting sender");
  ALYRN_CHECK(!sent, "TryReceive resumed sender inline");
  channel.Close();
  co_return;
}

Task<void> TryUnbufferedEmpty(Channel<int>& channel) {
  std::optional<int> value{99};
  ALYRN_CHECK(!channel.TrySend(1), "unbuffered send succeeded without receiver");
  ALYRN_CHECK(!channel.TryReceive(value) && value == 99, "unbuffered receive did not block");
  co_return;
}

Task<void> SendValue(Channel<int>& channel, int value) {
  ALYRN_CHECK((co_await (channel << value)).HasValue(), "queued send failed");
}

Task<void> FillBuffer(Channel<int>& channel) {
  ALYRN_CHECK(channel.TrySend(1), "initial buffer fill failed");
  co_return;
}

Task<void> TryDrainQueuedSenders(Channel<int>& channel) {
  std::optional<int> value;
  for (int expected : {1, 2, 3}) {
    ALYRN_CHECK(channel.TryReceive(value) && value == expected,
                "TryReceive did not refill buffer from oldest waiting sender");
  }
  channel.Close();
  co_return;
}

bool TestTryOperations() {
  DrainScheduler scheduler;
  Channel<int> buffered{scheduler, 2};
  Channel<std::unique_ptr<int>> owned{scheduler, 1};
  auto buffer_task = Spawn(scheduler, TryBufferedCase(buffered));
  auto owned_task = Spawn(scheduler, TryMoveOnlyCase(owned));
  scheduler.Drain();
  buffer_task.Wait();
  owned_task.Wait();

  Channel<int> rendezvous{scheduler, 0};
  auto empty = Spawn(scheduler, TryUnbufferedEmpty(rendezvous));
  scheduler.Drain();
  empty.Wait();
  bool received = false;
  auto receiver = Spawn(scheduler, ReceiveOne(rendezvous, received));
  ALYRN_CHECK(scheduler.DrainOne(), "receiver was not started");
  auto sender = Spawn(scheduler, TryWakeReceiver(rendezvous, received));
  scheduler.Drain();
  sender.Wait();
  receiver.Wait();
  ALYRN_CHECK(received, "TrySend did not schedule waiting receiver");

  Channel<int> incoming{scheduler, 0};
  bool sent = false;
  auto waiting = Spawn(scheduler, SendOne(incoming, sent));
  ALYRN_CHECK(scheduler.DrainOne(), "sender was not started");
  auto reader = Spawn(scheduler, TryWakeSender(incoming, sent));
  scheduler.Drain();
  waiting.Wait();
  reader.Wait();
  ALYRN_CHECK(sent, "TryReceive did not schedule waiting sender");

  Channel<int> fifo{scheduler, 1};
  auto fill = Spawn(scheduler, FillBuffer(fifo));
  auto second = Spawn(scheduler, SendValue(fifo, 2));
  auto third = Spawn(scheduler, SendValue(fifo, 3));
  scheduler.Drain();
  auto drain = Spawn(scheduler, TryDrainQueuedSenders(fifo));
  scheduler.Drain();
  fill.Wait();
  second.Wait();
  third.Wait();
  drain.Wait();
  return true;
}

Task<void> TrySendOnClosed(Channel<int>& channel) {
  channel.Close();
  (void)channel.TrySend(1);
  co_return;
}

void TriggerTrySendOnClosed() {
  DrainScheduler scheduler;
  Channel<int> channel{scheduler, 1};
  auto task = Spawn(scheduler, TrySendOnClosed(channel));
  scheduler.Drain();
  task.Wait();
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

Task<void> OddCapacityCase(Channel<int>& channel) {
  // Three rounds over a capacity-3 channel: the second and third wrap around
  // the power-of-two ring storage, which must stay invisible to callers.
  for (int round = 0; round < 3; ++round) {
    for (int i = 0; i < 3; ++i) {
      ALYRN_CHECK(channel.TrySend(round * 10 + i), "send within an odd capacity failed");
    }
    ALYRN_CHECK(!channel.TrySend(99), "channel buffered a value beyond its capacity");
    ALYRN_CHECK(channel.Size() == 3 && channel.Capacity() == 3,
                "odd-capacity channel reported the wrong size");
    for (int i = 0; i < 3; ++i) {
      std::optional<int> value;
      ALYRN_CHECK(channel.TryReceive(value) && value == round * 10 + i,
                  "odd-capacity channel lost FIFO order");
    }
  }
  channel.Close();
  co_return;
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

bool TestArbitraryCapacity() {
  DrainScheduler scheduler;
  Channel<int> channel{scheduler, 3};
  auto task = Spawn(scheduler, OddCapacityCase(channel));
  scheduler.Drain();
  task.Wait();
  return true;
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
  return TestTryOperations() &&
                 ExpectChildAbort(&TriggerTrySendOnClosed,
                                  "TrySend on closed channel must panic") &&
                 TestBufferedAndClose() && TestUnbufferedRendezvous() && TestCloseWakesReceiver() &&
                 TestClosePanics() && TestArbitraryCapacity() &&
                 TestPendingWaiterDestructionFailsFast()
             ? 0
             : 1;
}
