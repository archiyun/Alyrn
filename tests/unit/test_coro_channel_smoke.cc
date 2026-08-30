#include <cerrno>
#include <csignal>
#include <optional>
#include <sys/wait.h>
#include <unistd.h>

#include "alyrn/detail/check.h"
#include "alyrn/coro/channel.h"
#include "alyrn/coro/scheduler.h"
#include "alyrn/coro/spawn.h"
#include "alyrn/coro/task.h"
#include "alyrn/coro/work.h"

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
  ALYRN_CHECK((co_await channel.Send(1)).has_value(), "first buffered send failed");
  ALYRN_CHECK((co_await channel.Send(2)).has_value(), "second buffered send failed");
  ALYRN_CHECK(!channel.TrySend(3).has_value(), "full channel accepted a value");
  channel.Close();

  auto first = co_await channel.Receive();
  auto second = co_await channel.Receive();
  ALYRN_CHECK(first.has_value() && first->has_value() && **first == 1,
                 "first buffered value was not FIFO");
  ALYRN_CHECK(second.has_value() && second->has_value() && **second == 2,
                 "second buffered value was not FIFO");

  ALYRN_CHECK(!(co_await channel.Send(3)).has_value(), "closed channel accepted a send");
  auto closed = co_await channel.Receive();
  ALYRN_CHECK(closed.has_value() && !closed->has_value(),
                 "closed empty channel did not report end of stream");
  passed = true;
}

Task<void> SendOne(Channel<int>& channel, bool& passed) {
  const auto sent = co_await channel.Send(42);
  ALYRN_CHECK(sent.has_value(), "rendezvous send failed");
  passed = true;
}

Task<void> ReceiveOne(Channel<int>& channel, bool& passed) {
  const auto received = co_await channel.Receive();
  ALYRN_CHECK(received.has_value() && received->has_value() && **received == 42,
                 "rendezvous receive failed");
  passed = true;
}

Task<void> WaitForClose(Channel<int>& channel, bool& passed) {
  const auto received = co_await channel.Receive();
  ALYRN_CHECK(received.has_value() && !received->has_value(),
                 "Close did not wake the pending receiver");
  passed = true;
}

Task<void> Close(Channel<int>& channel) {
  channel.Close();
  co_return;
}

Task<void> WaitForever(Channel<int>& channel) {
  [[maybe_unused]] const auto received = co_await channel.Receive();
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
                 TestPendingWaiterDestructionFailsFast()
             ? 0
             : 1;
}
