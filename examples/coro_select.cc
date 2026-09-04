// SPDX-License-Identifier: MIT

#include <iostream>
#include <optional>
#include <string>

#include "alyrn/coro.h"
#include "alyrn/detail/check.h"

namespace {

using alyrn::coro::Channel;
using alyrn::coro::Scheduler;
using alyrn::coro::Select;
using alyrn::coro::Spawn;
using alyrn::coro::Task;
using alyrn::coro::Work;
using alyrn::coro::WorkQueue;

class ExampleScheduler final : public Scheduler {
public:
  void Schedule(Work* work) noexcept override {
    ALYRN_CHECK(ready_.PushBack(work), "example scheduled the same work twice");
  }

  void RunUntilIdle() noexcept {
    while (auto* work = ready_.PopFront()) {
      Run(work);
    }
  }

private:
  WorkQueue ready_;
};

Task<void> ReceiveFirstAvailable(Channel<std::string>& primary, Channel<std::string>& fallback) {
  std::optional<std::string> primary_value;
  std::optional<std::string> fallback_value;

  const std::size_t selected =
      co_await Select(primary >> primary_value, fallback >> fallback_value);
  const auto& value = selected == 0 ? primary_value : fallback_value;
  ALYRN_CHECK(value.has_value(), "selected receive case had no value");
  std::cout << "case " << selected << ": " << *value << '\n';

  primary.Close();
  fallback.Close();
}

Task<void> Produce(Channel<std::string>& channel) {
  ALYRN_CHECK((co_await (channel << std::string{"hello from fallback"})).HasValue(),
              "example send failed");
}

}  // namespace

int main() {
  ExampleScheduler scheduler;
  Channel<std::string> primary{scheduler, 0};
  Channel<std::string> fallback{scheduler, 0};

  auto consumer = Spawn(scheduler, ReceiveFirstAvailable(primary, fallback));
  auto producer = Spawn(scheduler, Produce(fallback));
  scheduler.RunUntilIdle();
  consumer.Wait();
  producer.Wait();
}
