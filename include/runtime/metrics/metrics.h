// Copyright (c) 2026 Aresna
// SPDX-License-Identifier: MIT
#pragma once

#include "runtime/base/noncopyable.h"

#include <array>
#include <atomic>
#include <cstdint>
#include <mutex>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace runtime::metrics {

// 无锁原子计数器, 用于单调递增事件 (请求数, 错误数, 拒绝次数等)
class Counter {
public:
  void Inc() { val_.fetch_add(1, std::memory_order_relaxed); }
  void Add(std::uint64_t n) { val_.fetch_add(n, std::memory_order_relaxed); }
  std::uint64_t Value() const { return val_.load(std::memory_order_relaxed); }

private:
  std::atomic<std::uint64_t> val_{0};
};

// 无锁原子标量, 可增可减 (活跃连接数, 队列深度等)
class Gauge {
public:
  void Set(std::int64_t v) { val_.store(v, std::memory_order_relaxed); }
  void Inc() { val_.fetch_add(1, std::memory_order_relaxed); }
  void Dec() { val_.fetch_sub(1, std::memory_order_relaxed); }
  void Add(std::int64_t n) { val_.fetch_add(n, std::memory_order_relaxed); }
  std::int64_t Value() const { return val_.load(std::memory_order_relaxed); }

private:
  std::atomic<std::int64_t> val_{0};
};

// 直方图, 桶边界覆盖 1ms ~ 60s 的请求耗时区间
// 互斥锁实现, 写入路径上有一次加锁; 网关每秒万级 QPS 量级下 contention 可忽略
class Histogram : public runtime::base::NonCopyable {
public:
  static constexpr std::array<double, 14> kBuckets = {
      0.001, 0.005, 0.01,  0.025, 0.05, 0.1,  0.25,
      0.5,   1.0,   2.5,   5.0,   10.0, 30.0, 60.0,
  };

  Histogram(std::string name, std::string help)
      : name_(std::move(name)), help_(std::move(help)),
        bucket_counts_(kBuckets.size(), 0) {}

  void Observe(double seconds) {
    std::lock_guard lk{mu_};
    sum_ += seconds;
    ++count_;
    for (std::size_t i = 0; i < kBuckets.size(); ++i) {
      if (seconds <= kBuckets[i]) ++bucket_counts_[i];
    }
  }

  // Prometheus exposition format (text), spec:
  //   https://prometheus.io/docs/instrumenting/exposition_formats/
  void WritePrometheus(std::ostream& os) const {
    std::lock_guard lk{mu_};
    os << "# HELP " << name_ << ' ' << help_ << '\n';
    os << "# TYPE " << name_ << " histogram\n";
    for (std::size_t i = 0; i < kBuckets.size(); ++i) {
      os << name_ << "_bucket{le=\"" << kBuckets[i] << "\"} "
         << bucket_counts_[i] << '\n';
    }
    os << name_ << "_bucket{le=\"+Inf\"} " << count_ << '\n';
    os << name_ << "_sum " << sum_ << '\n';
    os << name_ << "_count " << count_ << '\n';
  }

private:
  std::string name_;
  std::string help_;
  mutable std::mutex mu_;
  double sum_{0.0};
  std::uint64_t count_{0};
  std::vector<std::uint64_t> bucket_counts_;
};

// 辅助函数, 把 Counter/Gauge 渲染成 Prometheus 文本块
inline void WriteCounter(std::ostream& os, std::string_view name,
                         std::string_view help, const Counter& c) {
  os << "# HELP " << name << ' ' << help << '\n';
  os << "# TYPE " << name << " counter\n";
  os << name << ' ' << c.Value() << '\n';
}

inline void WriteGauge(std::ostream& os, std::string_view name,
                       std::string_view help, const Gauge& g) {
  os << "# HELP " << name << ' ' << help << '\n';
  os << "# TYPE " << name << " gauge\n";
  os << name << ' ' << g.Value() << '\n';
}

}  // namespace runtime::metrics
