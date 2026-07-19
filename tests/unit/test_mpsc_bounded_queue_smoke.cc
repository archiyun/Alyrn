// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <thread>
#include <vector>

#include "vexo/ds/mpsc_bounded_queue.h"

namespace {

void CheckFifoAndWrapAround() {
  using Queue = vexo::ds::MpscBoundedQueue<int, 8>;
  Queue queue;

  assert(!queue.TryPop().has_value());

  for (int value = 0; value < 8; ++value) {
    assert(queue.TryPush(value) ==
           vexo::ds::MpscQueuePushResult::kPushed);
  }
  assert(queue.TryPush(8) ==
         vexo::ds::MpscQueuePushResult::kFull);

  for (int value = 0; value < 4; ++value) {
    auto popped = queue.TryPop();
    assert(popped.has_value());
    assert(*popped == value);
  }

  for (int value = 8; value < 12; ++value) {
    assert(queue.TryPush(value) ==
           vexo::ds::MpscQueuePushResult::kPushed);
  }

  for (int value = 4; value < 12; ++value) {
    auto popped = queue.TryPop();
    assert(popped.has_value());
    assert(*popped == value);
  }

  assert(queue.Empty());
}

void CheckMultipleProducers() {
  using Queue = vexo::ds::MpscBoundedQueue<int, 1024>;

  constexpr int kProducerCount = 4;
  constexpr int kItemsPerProducer = 10000;
  constexpr int kTotalItems = kProducerCount * kItemsPerProducer;

  Queue queue;
  std::atomic<int> consumed{0};
  std::atomic<long long> sum{0};

  std::thread consumer([&] {
    while (consumed.load(std::memory_order_acquire) < kTotalItems) {
      auto value = queue.TryPop();
      if (!value.has_value()) {
        std::this_thread::yield();
        continue;
      }

      sum.fetch_add(*value, std::memory_order_relaxed);
      consumed.fetch_add(1, std::memory_order_release);
    }
  });

  std::vector<std::thread> producers;
  producers.reserve(kProducerCount);

  for (int producer = 0; producer < kProducerCount; ++producer) {
    producers.emplace_back([&, producer] {
      for (int i = 0; i < kItemsPerProducer; ++i) {
        const int value = producer * kItemsPerProducer + i;

        while (queue.TryPush(value) ==
               vexo::ds::MpscQueuePushResult::kFull) {
          std::this_thread::yield();
        }
      }
    });
  }

  for (auto& producer : producers) {
    producer.join();
  }
  consumer.join();

  const long long expected_sum =
      static_cast<long long>(kTotalItems - 1) * kTotalItems / 2;

  assert(consumed.load(std::memory_order_acquire) == kTotalItems);
  assert(sum.load(std::memory_order_relaxed) == expected_sum);
  assert(queue.Empty());
}

}  // namespace

int main() {
  CheckFifoAndWrapAround();
  CheckMultipleProducers();
  return 0;
}
