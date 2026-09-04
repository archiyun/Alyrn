#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <csignal>
#include <cstddef>
#include <optional>
#include <string>

#include "alyrn/coro/channel.h"
#include "alyrn/coro/scheduler.h"
#include "alyrn/coro/select.h"
#include "alyrn/coro/spawn.h"
#include "alyrn/coro/task.h"
#include "alyrn/coro/work.h"
#include "alyrn/detail/check.h"

namespace {

using alyrn::coro::Channel;
using alyrn::coro::Scheduler;
using alyrn::coro::Select;
using alyrn::coro::SelectDefault;
using alyrn::coro::Spawn;
using alyrn::coro::Task;
using alyrn::coro::Work;
using alyrn::coro::WorkQueue;

class DrainScheduler final : public Scheduler {
public:
  void Schedule(Work* work) noexcept override {
    ALYRN_CHECK(queue_.PushBack(work), "work queued twice");
  }

  bool DrainOne() {
    auto* work = queue_.PopFront();
    if (work == nullptr) {
      return false;
    }
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

class DestroyTaskWork final : public Work {
public:
  explicit DestroyTaskWork(Task<void>::Handle handle) noexcept : handle_(handle) {
    SetRun([](Work* work) noexcept {
      auto* destroy = static_cast<DestroyTaskWork*>(work);
      ALYRN_CHECK(destroy->handle_, "task frame was already destroyed");
      destroy->handle_.destroy();
      destroy->handle_ = {};
      destroy->destroyed_ = true;
    });
  }

  bool Destroyed() const noexcept { return destroyed_; }

private:
  Task<void>::Handle handle_;
  bool destroyed_{false};
};

Task<void> CloseBoth(Channel<int>& first, Channel<int>& second) {
  first.Close();
  second.Close();
  co_return;
}

Task<void> CloseOne(Channel<int>& channel) {
  channel.Close();
  co_return;
}

Task<void> SendValue(Channel<int>& channel, int value) {
  ALYRN_CHECK((co_await (channel << value)).HasValue(), "Channel send failed");
}

Task<void> SelectImmediateReceive(Channel<int>& ready, Channel<int>& blocked, bool& passed) {
  ALYRN_CHECK((co_await (ready << 7)).HasValue(), "buffered send failed");

  std::optional<int> received;
  std::optional<int> ignored;
  const std::size_t selected = co_await Select(ready >> received, blocked >> ignored);
  ALYRN_CHECK(selected == 0, "Select chose the wrong ready receive case");
  ALYRN_CHECK(received.has_value() && *received == 7, "Select lost the received value");
  ALYRN_CHECK(!ignored.has_value(), "Select changed a losing receive output");

  ready.Close();
  blocked.Close();
  passed = true;
}

Task<void> SelectSecondReceive(Channel<int>& first, Channel<int>& second, std::size_t& selected,
                               std::optional<int>& received) {
  std::optional<int> ignored;
  selected = co_await Select(first >> ignored, second >> received);
  ALYRN_CHECK(!ignored.has_value(), "Select changed a losing receive output");
}

Task<void> SelectImmediateSend(Channel<int>& output, Channel<int>& blocked, bool& passed) {
  std::optional<int> ignored;
  const std::size_t selected = co_await Select(output << 23, blocked >> ignored);
  ALYRN_CHECK(selected == 0, "Select chose the wrong ready send case");

  std::optional<int> received;
  ALYRN_CHECK((co_await (output >> received)).HasValue(), "buffered receive failed");
  ALYRN_CHECK(received.has_value() && *received == 23, "Select sent the wrong value");

  output.Close();
  blocked.Close();
  passed = true;
}

Task<void> SelectDefaultCase(Channel<int>& channel, bool& passed) {
  std::optional<int> received{99};
  const std::size_t selected = co_await Select(channel >> received, SelectDefault());
  ALYRN_CHECK(selected == 1, "Select did not choose its default case");
  ALYRN_CHECK(received.has_value() && *received == 99,
              "default Select changed a losing receive output");
  channel.Close();
  passed = true;
}

Task<void> SelectClosedReceive(Channel<int>& channel, bool& passed) {
  channel.Close();
  std::optional<int> received{99};
  const std::size_t selected = co_await Select(channel >> received);
  ALYRN_CHECK(selected == 0, "Select did not choose a closed receive case");
  ALYRN_CHECK(!received.has_value(), "closed Select receive did not clear its output");
  passed = true;
}

Task<void> SelectHeterogeneousReceive(Channel<int>& numbers, Channel<std::string>& strings,
                                      bool& passed) {
  ALYRN_CHECK((co_await (strings << std::string{"ready"})).HasValue(), "string send failed");

  std::optional<int> number;
  std::optional<std::string> text;
  const std::size_t selected = co_await Select(numbers >> number, strings >> text);
  ALYRN_CHECK(selected == 1, "heterogeneous Select chose the wrong case");
  ALYRN_CHECK(text.has_value() && *text == "ready", "heterogeneous Select lost its value");

  numbers.Close();
  strings.Close();
  passed = true;
}

Task<void> SelectClosedSend(Channel<int>& channel) {
  channel.Close();
  [[maybe_unused]] const std::size_t selected = co_await Select(channel << 1);
}

Task<void> WaitInSelect(Channel<int>& first, Channel<int>& second) {
  std::optional<int> first_value;
  std::optional<int> second_value;
  [[maybe_unused]] const std::size_t selected =
      co_await Select(first >> first_value, second >> second_value);
  ALYRN_CHECK(false, "cancelled Select resumed");
}

Task<void> SelectSameChannel(Channel<int>& channel, std::size_t& selected,
                             std::optional<int>& received) {
  selected = co_await Select(channel << 7, channel >> received);
}

Task<void> ExerciseReadyCaseFairness(Channel<int>& first, Channel<int>& second,
                                     std::size_t& first_wins, std::size_t& second_wins) {
  constexpr std::size_t kIterations = 256;
  for (std::size_t iteration = 0; iteration != kIterations; ++iteration) {
    const int value = static_cast<int>(iteration);
    ALYRN_CHECK((co_await (first << value)).HasValue(), "first fairness send failed");
    ALYRN_CHECK((co_await (second << value)).HasValue(), "second fairness send failed");

    std::optional<int> first_value;
    std::optional<int> second_value;
    const std::size_t selected = co_await Select(first >> first_value, second >> second_value);
    if (selected == 0) {
      ++first_wins;
      ALYRN_CHECK(first_value.has_value() && *first_value == value,
                  "first fairness case received the wrong value");
      ALYRN_CHECK((co_await (second >> second_value)).HasValue() && second_value.has_value() &&
                      *second_value == value,
                  "second fairness channel did not retain its value");
    } else {
      ALYRN_CHECK(selected == 1, "fairness Select returned an invalid case index");
      ++second_wins;
      ALYRN_CHECK(second_value.has_value() && *second_value == value,
                  "second fairness case received the wrong value");
      ALYRN_CHECK((co_await (first >> first_value)).HasValue() && first_value.has_value() &&
                      *first_value == value,
                  "first fairness channel did not retain its value");
    }
  }

  first.Close();
  second.Close();
}

bool TestImmediateReceive() {
  DrainScheduler scheduler;
  Channel<int> ready{scheduler, 1};
  Channel<int> blocked{scheduler, 0};
  bool passed = false;
  auto task = Spawn(scheduler, SelectImmediateReceive(ready, blocked, passed));
  scheduler.Drain();
  task.Wait();
  return passed;
}

bool TestWaitForSecondReceive() {
  DrainScheduler scheduler;
  Channel<int> first{scheduler, 0};
  Channel<int> second{scheduler, 0};
  std::size_t selected = 99;
  std::optional<int> received;

  auto select = Spawn(scheduler, SelectSecondReceive(first, second, selected, received));
  ALYRN_CHECK(scheduler.DrainOne(), "Select root was not scheduled");
  auto sender = Spawn(scheduler, SendValue(second, 42));
  scheduler.Drain();
  select.Wait();
  sender.Wait();

  auto closer = Spawn(scheduler, CloseBoth(first, second));
  scheduler.Drain();
  closer.Wait();
  return selected == 1 && received.has_value() && *received == 42;
}

bool TestImmediateSend() {
  DrainScheduler scheduler;
  Channel<int> output{scheduler, 1};
  Channel<int> blocked{scheduler, 0};
  bool passed = false;
  auto task = Spawn(scheduler, SelectImmediateSend(output, blocked, passed));
  scheduler.Drain();
  task.Wait();
  return passed;
}

bool TestDefault() {
  DrainScheduler scheduler;
  Channel<int> channel{scheduler, 0};
  bool passed = false;
  auto task = Spawn(scheduler, SelectDefaultCase(channel, passed));
  scheduler.Drain();
  task.Wait();
  return passed;
}

bool TestClosedReceive() {
  DrainScheduler scheduler;
  Channel<int> channel{scheduler, 0};
  bool passed = false;
  auto task = Spawn(scheduler, SelectClosedReceive(channel, passed));
  scheduler.Drain();
  task.Wait();
  return passed;
}

bool TestHeterogeneousReceive() {
  DrainScheduler scheduler;
  Channel<int> numbers{scheduler, 0};
  Channel<std::string> strings{scheduler, 1};
  bool passed = false;
  auto task = Spawn(scheduler, SelectHeterogeneousReceive(numbers, strings, passed));
  scheduler.Drain();
  task.Wait();
  return passed;
}

void TriggerSelectClosedSend() {
  DrainScheduler scheduler;
  Channel<int> channel{scheduler, 0};
  auto task = Spawn(scheduler, SelectClosedSend(channel));
  scheduler.Drain();
  task.Wait();
}

bool TestClosedSendPanics() {
  const pid_t child = ::fork();
  if (child < 0) {
    return false;
  }
  if (child == 0) {
    TriggerSelectClosedSend();
    ::_exit(0);
  }

  int status = 0;
  while (::waitpid(child, &status, 0) < 0 && errno == EINTR) {
  }
  return WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT;
}

bool TestSuspendedSelectCanBeCancelled() {
  DrainScheduler scheduler;
  Channel<int> first{scheduler, 0};
  Channel<int> second{scheduler, 0};

  auto task = WaitInSelect(first, second);
  auto handle = task.Release();
  alyrn::coro::ResumeWork starter{handle};
  scheduler.Schedule(&starter);
  ALYRN_CHECK(scheduler.DrainOne(), "Select task was not scheduled");

  DestroyTaskWork destroy{handle};
  scheduler.Schedule(&destroy);
  ALYRN_CHECK(scheduler.DrainOne(), "Select cancellation was not scheduled");

  auto closer = Spawn(scheduler, CloseBoth(first, second));
  scheduler.Drain();
  closer.Wait();
  return destroy.Destroyed();
}

bool TestSameChannelCasesDoNotSelfMatch() {
  DrainScheduler scheduler;
  Channel<int> channel{scheduler, 0};
  std::size_t selected = 99;
  std::optional<int> received;

  auto select = Spawn(scheduler, SelectSameChannel(channel, selected, received));
  ALYRN_CHECK(scheduler.DrainOne(), "same-Channel Select root was not scheduled");
  auto sender = Spawn(scheduler, SendValue(channel, 42));
  scheduler.Drain();
  select.Wait();
  sender.Wait();

  auto closer = Spawn(scheduler, CloseOne(channel));
  scheduler.Drain();
  closer.Wait();
  return selected == 1 && received.has_value() && *received == 42;
}

bool TestReadyCasesAreFair() {
  DrainScheduler scheduler;
  Channel<int> first{scheduler, 1};
  Channel<int> second{scheduler, 1};
  std::size_t first_wins = 0;
  std::size_t second_wins = 0;

  auto task = Spawn(scheduler, ExerciseReadyCaseFairness(first, second, first_wins, second_wins));
  scheduler.Drain();
  task.Wait();
  return first_wins != 0 && second_wins != 0 && first_wins + second_wins == 256;
}

}  // namespace

int main() {
  return TestImmediateReceive() && TestWaitForSecondReceive() && TestImmediateSend() &&
                 TestDefault() && TestClosedReceive() && TestHeterogeneousReceive() &&
                 TestClosedSendPanics() && TestSuspendedSelectCanBeCancelled() &&
                 TestSameChannelCasesDoNotSelfMatch() && TestReadyCasesAreFair()
             ? 0
             : 1;
}
