// SPDX-License-Identifier: MIT
//
// Randomized differential tests for net::Buffer against a flat std::vector
// reference model. Fixed seeds keep them deterministic and CI-reproducible;
// under the sanitizer build they also exercise every reserved iovec for
// out-of-bounds and use-after-free. Two batteries:
//
//   - search/linearize: Append/Drain/Find/Linearize vs the model, with tiny
//     random block sizes so needles and linearized ranges straddle blocks.
//   - write reservation: PrepareWrite (vector and caller-storage overloads),
//     TryPrepareWriteOne, partial CommitWrite, and AbortWrite. Each reserved
//     iovec is filled from a running byte source, so a commit must append
//     exactly those bytes in block order.

#include <sys/uio.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <optional>
#include <random>
#include <span>
#include <string>
#include <vector>

#include "alyrn/net/buffer.h"

namespace {

using alyrn::net::Buffer;

// Naive search over the flat model, matching Buffer::Find's contract.
long ReferenceFind(const std::vector<std::byte>& hay, const std::vector<std::byte>& needle,
                   std::size_t from) {
  if (from > hay.size()) return -1;
  if (needle.empty()) return static_cast<long>(from);
  if (needle.size() > hay.size() - from) return -1;
  auto it = std::search(hay.begin() + static_cast<std::ptrdiff_t>(from), hay.end(), needle.begin(),
                        needle.end());
  return it == hay.end() ? -1 : static_cast<long>(it - hay.begin());
}

bool SearchBattery(std::uint64_t seed, std::uint64_t iters) {
  std::mt19937_64 rng(seed);
  auto rnd = [&](std::uint64_t n) { return n ? rng() % n : 0; };

  for (std::uint64_t it = 0; it < iters; ++it) {
    Buffer buf(1 + rnd(8));  // tiny blocks force cross-block cases
    std::vector<std::byte> model;
    const int steps = 5 + static_cast<int>(rnd(40));
    for (int s = 0; s < steps; ++s) {
      const int op = static_cast<int>(rnd(5));
      if (op <= 1) {  // Append
        std::vector<std::byte> chunk(rnd(20));
        for (auto& b : chunk) b = std::byte{static_cast<std::uint8_t>('a' + rnd(4))};
        buf.Append(std::span<const std::byte>(chunk.data(), chunk.size()));
        model.insert(model.end(), chunk.begin(), chunk.end());
      } else if (op == 2) {  // Drain
        const std::size_t n = rnd(model.size() + 3);
        buf.Drain(n);
        model.erase(model.begin(), model.begin() + static_cast<std::ptrdiff_t>(std::min(n, model.size())));
      } else if (op == 3) {  // Find
        std::vector<std::byte> needle(rnd(4));
        for (auto& b : needle) b = std::byte{static_cast<std::uint8_t>('a' + rnd(4))};
        const std::size_t from = rnd(model.size() + 2);
        auto got = buf.Find(std::span<const std::byte>(needle.data(), needle.size()), from);
        const long want = ReferenceFind(model, needle, from);
        const long g = got.has_value() ? static_cast<long>(*got) : -1;
        if (g != want) {
          std::printf("FAIL Find: seed=%llu it=%llu from=%zu got=%ld want=%ld\n",
                      static_cast<unsigned long long>(seed), static_cast<unsigned long long>(it),
                      from, g, want);
          return false;
        }
      } else if (!model.empty()) {  // Linearize
        const std::size_t n = 1 + rnd(model.size());
        auto view = buf.Linearize(n);
        if (view.size() != n || !std::equal(view.begin(), view.end(), model.begin())) {
          std::printf("FAIL Linearize: seed=%llu it=%llu n=%zu\n",
                      static_cast<unsigned long long>(seed), static_cast<unsigned long long>(it), n);
          return false;
        }
      }
      if (buf.ReadableBytes() != model.size()) {
        std::printf("FAIL ReadableBytes: seed=%llu it=%llu got=%zu want=%zu\n",
                    static_cast<unsigned long long>(seed), static_cast<unsigned long long>(it),
                    buf.ReadableBytes(), model.size());
        return false;
      }
    }
  }
  return true;
}

bool WriteReservationBattery(std::uint64_t seed, std::uint64_t iters) {
  std::mt19937_64 rng(seed);
  auto rnd = [&](std::uint64_t n) { return n ? rng() % n : 0; };

  for (std::uint64_t it = 0; it < iters; ++it) {
    Buffer buf(1 + rnd(64));
    std::vector<std::byte> model;
    std::uint8_t gen = static_cast<std::uint8_t>(rnd(256));  // running byte source
    const int steps = 5 + static_cast<int>(rnd(40));
    for (int s = 0; s < steps; ++s) {
      const int op = static_cast<int>(rnd(6));
      if (op == 0) {  // Append
        std::vector<std::byte> chunk(rnd(40));
        for (auto& b : chunk) b = std::byte{gen++};
        buf.Append(std::span<const std::byte>(chunk.data(), chunk.size()));
        model.insert(model.end(), chunk.begin(), chunk.end());
      } else if (op == 1) {  // Drain
        const std::size_t n = rnd(model.size() + 3);
        buf.Drain(n);
        model.erase(model.begin(), model.begin() + static_cast<std::ptrdiff_t>(std::min(n, model.size())));
      } else if (op == 2 || op == 3) {  // PrepareWrite (vector or caller storage)
        const std::size_t hint = rnd(200);
        std::vector<iovec> storage(16);
        std::vector<iovec> iovs;
        if (op == 2) {
          iovs = buf.PrepareWrite(hint, 16);
        } else {
          auto got = buf.PrepareWrite(hint, std::span<iovec>(storage));
          iovs.assign(got.begin(), got.end());
        }
        std::vector<std::byte> written;
        for (const auto& v : iovs) {
          auto* p = static_cast<std::byte*>(v.iov_base);
          for (std::size_t i = 0; i < v.iov_len; ++i) {
            p[i] = std::byte{gen++};
            written.push_back(p[i]);
          }
        }
        if (written.empty() || rnd(4) == 0) {
          buf.AbortWrite();
        } else {
          const std::size_t n = 1 + rnd(written.size());  // partial or full commit
          buf.CommitWrite(n);
          model.insert(model.end(), written.begin(), written.begin() + static_cast<std::ptrdiff_t>(n));
        }
      } else if (op == 4) {  // TryPrepareWriteOne
        const std::size_t hint = 1 + rnd(200);
        auto one = buf.TryPrepareWriteOne(hint);
        if (one.has_value()) {
          auto* p = static_cast<std::byte*>(one->iov_base);
          std::vector<std::byte> written(one->iov_len);
          for (std::size_t i = 0; i < one->iov_len; ++i) {
            p[i] = std::byte{gen++};
            written[i] = p[i];
          }
          if (written.empty() || rnd(3) == 0) {
            buf.AbortWrite();
          } else {
            const std::size_t n = 1 + rnd(written.size());
            buf.CommitWrite(n);
            model.insert(model.end(), written.begin(), written.begin() + static_cast<std::ptrdiff_t>(n));
          }
        }
      } else if (!model.empty()) {  // full-content read-back
        auto view = buf.Linearize(model.size());
        if (view.size() != model.size() || !std::equal(view.begin(), view.end(), model.begin())) {
          std::printf("FAIL content: seed=%llu it=%llu step=%d\n",
                      static_cast<unsigned long long>(seed), static_cast<unsigned long long>(it), s);
          return false;
        }
      }
      if (buf.ReadableBytes() != model.size()) {
        std::printf("FAIL readable: seed=%llu it=%llu step=%d got=%zu want=%zu\n",
                    static_cast<unsigned long long>(seed), static_cast<unsigned long long>(it), s,
                    buf.ReadableBytes(), model.size());
        return false;
      }
    }
  }
  return true;
}

}  // namespace

int main() {
  for (std::uint64_t seed = 1; seed <= 4; ++seed) {
    if (!SearchBattery(seed, 12000)) return 1;
    if (!WriteReservationBattery(seed, 12000)) return 1;
  }
  std::printf("buffer fuzz: PASS\n");
  return 0;
}
