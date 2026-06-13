#include <cmath>
#include <cstdint>
#include <iostream>
#include <random>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "runtime/ds/bloom_filter.h"

namespace {

bool Expect(bool condition, const char* message) {
  if (!condition) {
    std::cerr << "[FAIL] " << message << '\n';
    return false;
  }
  return true;
}

// --- Basic insert and query ---

bool TestBasicInsertQuery() {
  runtime::ds::BloomFilter<1000> bf;

  bf.Insert(std::string_view("hello"));
  bf.Insert(std::string_view("world"));

  if (!Expect(bf.MayContain(std::string_view("hello")),
              "inserted key 'hello' should be found"))
    return false;
  if (!Expect(bf.MayContain(std::string_view("world")),
              "inserted key 'world' should be found"))
    return false;

  return true;
}

// --- Deterministic negative for uninserted keys ---

bool TestDeterministicNegative() {
  runtime::ds::BloomFilter<1000> bf;

  bf.Insert(std::string_view("exists"));

  if (!Expect(!bf.MayContain(std::string_view("no_such_key")),
              "uninserted key must return false"))
    return false;
  if (!Expect(!bf.MayContain(std::string_view("")),
              "empty string not inserted must return false"))
    return false;

  return true;
}

// --- Empty filter: everything is negative ---

bool TestEmptyFilter() {
  runtime::ds::BloomFilter<1000> bf;

  if (!Expect(bf.empty(), "new filter should be empty")) return false;
  if (!Expect(!bf.MayContain(std::string_view("anything")),
              "empty filter should not contain anything"))
    return false;
  if (!Expect(bf.estimated_count() == 0,
              "empty filter should estimate 0 elements"))
    return false;

  return true;
}

// --- Clear resets the filter ---

bool TestClear() {
  runtime::ds::BloomFilter<1000> bf;

  bf.Insert(std::string_view("key1"));
  bf.Insert(std::string_view("key2"));
  if (!Expect(!bf.empty(), "filter should not be empty after inserts"))
    return false;

  bf.Clear();
  if (!Expect(bf.empty(), "filter should be empty after Clear()")) return false;
  if (!Expect(!bf.MayContain(std::string_view("key1")),
              "cleared filter should not contain old keys"))
    return false;

  return true;
}

// --- Template Insert<T> for trivially copyable types ---

bool TestTriviallyCopyableInsert() {
  runtime::ds::BloomFilter<1000> bf;

  uint32_t value = 0xDEADBEEF;
  bf.Insert(value);

  if (!Expect(bf.MayContain(value),
              "inserted uint32_t should be found"))
    return false;

  uint32_t other = 0xCAFEBABE;
  if (!Expect(!bf.MayContain(other),
              "different uint32_t should not be found"))
    return false;

  return true;
}

// --- Compile-time parameter queries ---

bool TestStaticParameters() {
  using BF = runtime::ds::BloomFilter<1000, 0.01>;

  if (!Expect(BF::bit_count() > 0, "bit_count should be positive"))
    return false;
  if (!Expect(BF::hash_count() > 0, "hash_count should be positive"))
    return false;

  // For n=1000, p=0.01: m ≈ 9585, k ≈ 7
  if (!Expect(BF::bit_count() > 5000 && BF::bit_count() < 20000,
              "bit_count should be in reasonable range for n=1000, p=0.01"))
    return false;
  if (!Expect(BF::hash_count() >= 4 && BF::hash_count() <= 15,
              "hash_count should be in reasonable range"))
    return false;

  return true;
}

// --- Estimated count accuracy ---

bool TestEstimatedCount() {
  runtime::ds::BloomFilter<10000, 0.01> bf;

  constexpr std::size_t kInsertCount = 1000;
  for (std::size_t i = 0; i < kInsertCount; ++i) {
    bf.Insert(std::string_view(reinterpret_cast<const char*>(&i), sizeof(i)));
  }

  auto est = bf.estimated_count();
  // Allow 50% error margin — bloom filter estimates are inherently approximate
  double ratio = static_cast<double>(est) / static_cast<double>(kInsertCount);
  if (!Expect(ratio > 0.5 && ratio < 1.5,
              "estimated_count should be within 50% of actual"))
    return false;

  return true;
}

// --- False positive rate measurement ---

bool TestFalsePositiveRate() {
  // Use a higher FP rate so we can measure it reliably with fewer queries
  runtime::ds::BloomFilter<1000, 0.05> bf;

  // Insert n items
  std::unordered_set<std::string> inserted;
  std::mt19937 rng(42);
  for (int i = 0; i < 1000; ++i) {
    std::string key = "key_" + std::to_string(rng());
    inserted.insert(key);
    bf.Insert(std::string_view(key));
  }

  // Query m items that were NOT inserted
  std::size_t false_positives = 0;
  std::size_t total_negatives = 0;
  for (int i = 0; i < 10000; ++i) {
    std::string probe = "probe_" + std::to_string(rng());
    if (inserted.count(probe)) continue;  // skip if accidentally collides
    total_negatives++;
    if (bf.MayContain(std::string_view(probe))) {
      false_positives++;
    }
  }

  double actual_fp_rate =
      static_cast<double>(false_positives) / static_cast<double>(total_negatives);

  // Actual FP rate should not exceed 3x the target (statistical tolerance)
  if (!Expect(actual_fp_rate < 0.15,
              "false positive rate should be well below 15% for p=0.05"))
    return false;

  std::cout << "  [INFO] measured FP rate: " << actual_fp_rate << '\n';
  return true;
}

// --- Different template instantiations compile ---

bool TestMultipleInstantiations() {
  // Small filter
  runtime::ds::BloomFilter<10, 0.1> small;
  small.Insert(std::string_view("a"));
  if (!Expect(small.MayContain(std::string_view("a")),
              "small filter works"))
    return false;

  // Large filter with tight FP rate
  runtime::ds::BloomFilter<1'000'000, 0.001> large;
  large.Insert(std::string_view("needle"));
  if (!Expect(large.MayContain(std::string_view("needle")),
              "large filter works"))
    return false;

  return true;
}

// --- string_view overload consistency with template T overload ---

bool TestStringViewConsistency() {
  runtime::ds::BloomFilter<1000> bf;

  // Insert via string_view, query via string_view
  std::string hello = "hello";
  bf.Insert(std::string_view(hello));

  if (!Expect(bf.MayContain(std::string_view("hello")),
              "string_view insert/query consistent"))
    return false;

  return true;
}

// --- No false negatives (fundamental guarantee) ---

bool TestNoFalseNegatives() {
  runtime::ds::BloomFilter<500, 0.01> bf;

  std::vector<std::string> keys;
  for (int i = 0; i < 500; ++i) {
    keys.push_back("item_" + std::to_string(i));
  }

  // Insert all keys
  for (const auto& k : keys) {
    bf.Insert(std::string_view(k));
  }

  // Every inserted key MUST be found (zero false negatives)
  for (const auto& k : keys) {
    if (!bf.MayContain(std::string_view(k))) {
      std::cerr << "[FAIL] false negative for key: " << k << '\n';
      return false;
    }
  }

  return true;
}

}  // namespace

int main() {
  if (!TestBasicInsertQuery()) return 1;
  if (!TestDeterministicNegative()) return 1;
  if (!TestEmptyFilter()) return 1;
  if (!TestClear()) return 1;
  if (!TestTriviallyCopyableInsert()) return 1;
  if (!TestStaticParameters()) return 1;
  if (!TestEstimatedCount()) return 1;
  if (!TestFalsePositiveRate()) return 1;
  if (!TestMultipleInstantiations()) return 1;
  if (!TestStringViewConsistency()) return 1;
  if (!TestNoFalseNegatives()) return 1;

  std::cout << "[PASS] bloom_filter_test\n";
  return 0;
}
