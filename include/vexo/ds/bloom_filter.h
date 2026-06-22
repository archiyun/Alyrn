// Copyright (c) 2026  junxu05o3-sketch
// SPDX-License-Identifier: MIT
// bloom_filter.h - Space-efficient probabilistic membership data structure
//
// Compile-time parameterized bloom filter using double hashing
// (Kirsch & Mitzenmacher).  Reuses MurmurHash3_x86_32 with two fixed seeds
// to generate k independent hash values without calling the hash function
// k times.
//
// Reference:
//   - Adam Kirsch, Michael Mitzenmacher, "Less Hashing, Same Performance:
//     Hashing with Two Functions", 2008.

#pragma once

#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <numbers>
#include <string_view>
#include <type_traits>

#include "vexo/base/noncopyable.h"
#include "vexo/ds/murmurhash3.h"

namespace vexo::ds {

namespace detail {

// Constexpr natural logarithm, valid for x > 0.
//
// We cannot use std::log here: it is not constexpr until C++26 (P0533).
// GCC's libstdc++ accepts it as an extension in constant expressions, but
// clang does not — which would break the clang CI builds. So we implement ln
// directly:
//   - Range reduction: write x = 2^e * f with f in [1, 2), then
//         ln(x) = e * ln2 + ln(f).
//   - For f in [1, 2) use the atanh series (t = (f-1)/(f+1) is in [0, 1/3),
//     so ~7 terms give ~1e-12 precision):
//         ln(f) = 2 * (t + t^3/3 + t^5/5 + ...).
consteval double ConstLn(double x) {
  int e = 0;
  while (x >= 2.0) {
    x *= 0.5;
    ++e;
  }
  while (x < 1.0) {
    x *= 2.0;
    --e;
  }
  double t = (x - 1.0) / (x + 1.0);
  double t2 = t * t;
  double sum = 0.0;
  double term = t;
  for (int i = 1; i <= 13; i += 2) {
    sum += term / static_cast<double>(i);
    term *= t2;
  }
  return 2.0 * sum + static_cast<double>(e) * std::numbers::ln2;
}

// Smallest integer >= x, for x > 0. (std::ceil is not constexpr until C++26.)
consteval std::size_t ConstCeil(double x) {
  std::size_t n = static_cast<std::size_t>(x);
  return (static_cast<double>(n) < x) ? n + 1 : n;
}

// Optimal bit count: m = -(n * ln(p)) / (ln2)^2. Floored at 1 so that later
// modular indexing never divides by zero.
consteval std::size_t OptimalBitCount(std::size_t n, double p) {
  std::size_t m = static_cast<std::size_t>(
      -static_cast<double>(n) * ConstLn(p) /
      (std::numbers::ln2 * std::numbers::ln2));
  return m == 0 ? 1 : m;
}

// Optimal hash count: k = ceil((m / n) * ln2). At least one hash function.
consteval std::size_t OptimalHashCount(std::size_t m, std::size_t n) {
  std::size_t k = ConstCeil(static_cast<double>(m) / static_cast<double>(n) *
                            std::numbers::ln2);
  return k == 0 ? 1 : k;
}

// Round up to next multiple of 64 (uint64_t word boundary).
consteval std::size_t WordsNeeded(std::size_t bits) {
  return (bits + 63) / 64;
}

}  // namespace detail

// BloomFilter — probabilistic set membership test.
//
// Template parameters:
//   kExpectedItems      - expected number of insertions
//   kFalsePositiveRate  - target false positive probability (default 1%)
//
// Guarantees:
//   - MayContain returns false  => element is definitely NOT in the set
//   - MayContain returns true   => element is PROBABLY in the set
//     (false positive rate ≤ kFalsePositiveRate when n ≤ kExpectedItems)
//
// Not supported:
//   - Removal (standard bloom filters cannot delete elements)
//
template <std::size_t kExpectedItems, double kFalsePositiveRate = 0.01>
class BloomFilter : public vexo::base::NonCopyable {
  static_assert(kExpectedItems > 0, "kExpectedItems must be positive");
  static_assert(kFalsePositiveRate > 0.0 && kFalsePositiveRate < 1.0,
                "kFalsePositiveRate must be in (0, 1)");

 public:
  BloomFilter() = default;

  // --- Insert ---

  // Insert a string_view key.
  void Insert(std::string_view key) {
    auto [h1, h2] = DoubleHash(key.data(), key.size());
    for (std::size_t i = 0; i < kNumHashes; ++i) {
      SetBit(CombineHash(h1, h2, i));
    }
  }

  // Insert an arbitrary trivially-copyable value.
  template <typename T>
  void Insert(const T& value) {
    static_assert(std::is_trivially_copyable_v<T>,
                  "T must be trivially copyable; use string_view overload "
                  "for complex types");
    Insert(std::string_view{reinterpret_cast<const char*>(&value), sizeof(T)});
  }

  // --- Query ---

  // Query a string_view key.
  bool MayContain(std::string_view key) const {
    auto [h1, h2] = DoubleHash(key.data(), key.size());
    for (std::size_t i = 0; i < kNumHashes; ++i) {
      if (!TestBit(CombineHash(h1, h2, i))) {
        return false;
      }
    }
    return true;
  }

  // Query an arbitrary trivially-copyable value.
  template <typename T>
  bool MayContain(const T& value) const {
    static_assert(std::is_trivially_copyable_v<T>,
                  "T must be trivially copyable; use string_view overload "
                  "for complex types");
    return MayContain(
        std::string_view{reinterpret_cast<const char*>(&value), sizeof(T)});
  }

  // --- Status ---

  // Whether no bits have been set.
  bool empty() const {
    for (auto word : words_) {
      if (word != 0) return false;
    }
    return true;
  }

  // Number of bits in the bit array (m).
  static constexpr std::size_t bit_count() { return kNumBits; }

  // Number of hash functions (k).
  static constexpr std::size_t hash_count() { return kNumHashes; }

  // Estimate the number of inserted elements from the set-bit density.
  // Uses the formula: n_hat = -(m / k) * ln(1 - X / m)
  // where X is the number of set bits.
  std::size_t estimated_count() const {
    std::size_t set_bits = CountSetBits();
    if (set_bits == 0) return 0;
    if (set_bits >= kNumBits) return kExpectedItems;  // saturated
    double ratio = static_cast<double>(set_bits) / static_cast<double>(kNumBits);
    return static_cast<std::size_t>(
        -(static_cast<double>(kNumBits) / static_cast<double>(kNumHashes)) *
        std::log(1.0 - ratio));
  }

  // Reset the filter to an empty state.
  void Clear() { words_.fill(0); }

 private:
  // Compile-time optimal parameters.
  static constexpr std::size_t kNumBits =
      detail::OptimalBitCount(kExpectedItems, kFalsePositiveRate);
  static constexpr std::size_t kNumHashes =
      detail::OptimalHashCount(kNumBits, kExpectedItems);
  static constexpr std::size_t kNumWords = detail::WordsNeeded(kNumBits);

  // Bit storage — stack-allocated, zero-initialized.
  std::array<uint64_t, kNumWords> words_{};

  // --- Double Hashing (Kirsch & Mitzenmacher) ---

  struct HashPair {
    uint32_t h1;
    uint32_t h2;
  };

  static HashPair DoubleHash(const void* data, std::size_t len) {
    return {
        MurmurHash3_x86_32(data, len, 0x9747b28c),
        MurmurHash3_x86_32(data, len, 0x5bd1e995),
    };
  }

  static std::size_t CombineHash(uint32_t h1, uint32_t h2, std::size_t i) {
    // h_i = h1 + i * h2, reduce mod kNumBits
    uint64_t combined = static_cast<uint64_t>(h1) +
                        static_cast<uint64_t>(i) * static_cast<uint64_t>(h2);
    return static_cast<std::size_t>(combined % static_cast<uint64_t>(kNumBits));
  }

  // --- Bit Manipulation ---

  void SetBit(std::size_t pos) {
    words_[pos / 64] |= (uint64_t{1} << (pos % 64));
  }

  bool TestBit(std::size_t pos) const {
    return (words_[pos / 64] & (uint64_t{1} << (pos % 64))) != 0;
  }

  std::size_t CountSetBits() const {
    std::size_t count = 0;
    for (auto word : words_) {
      count += static_cast<std::size_t>(__builtin_popcountll(word));
    }
    return count;
  }
};

}  // namespace vexo::ds
