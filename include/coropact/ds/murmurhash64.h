// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
// murmurhash64.h - MurmurHash3 x64 64-bit convenience hash
//
// Based on Austin Appleby's MurmurHash3 x64 128-bit variant. The public
// MurmurHash64() helper returns the first 64 bits after the x64_128 finalizer,
// which is suitable for hash rings that need a wider coordinate space.

#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace coropact::ds {

struct MurmurHash128 {
    uint64_t h1;
    uint64_t h2;
};

inline uint64_t rotl64(uint64_t x, int8_t r) {
    return (x << r) | (x >> (64 - r));
}

inline uint64_t fmix64(uint64_t k) {
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccdULL;
    k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53ULL;
    k ^= k >> 33;
    return k;
}

inline uint64_t Load64LE(const uint8_t* p) {
    return (static_cast<uint64_t>(p[0])) |
           (static_cast<uint64_t>(p[1]) << 8) |
           (static_cast<uint64_t>(p[2]) << 16) |
           (static_cast<uint64_t>(p[3]) << 24) |
           (static_cast<uint64_t>(p[4]) << 32) |
           (static_cast<uint64_t>(p[5]) << 40) |
           (static_cast<uint64_t>(p[6]) << 48) |
           (static_cast<uint64_t>(p[7]) << 56);
}

// MurmurHash3 x64 128-bit core.
// Input: data pointer, byte length, seed.
// Output: 128-bit hash. MurmurHash64() uses h1 as the 64-bit result.
inline MurmurHash128 MurmurHash3_x64_128(const void* key, size_t len, uint32_t seed = 0) {
    const uint8_t* data = static_cast<const uint8_t*>(key);
    const size_t nblocks = len / 16;

    uint64_t h1 = seed;
    uint64_t h2 = seed;

    constexpr uint64_t c1 = 0x87c37b91114253d5ULL;
    constexpr uint64_t c2 = 0x4cf5ad432745937fULL;

    for (size_t i = 0; i < nblocks; i++) {
        uint64_t k1 = Load64LE(data + i * 16);
        uint64_t k2 = Load64LE(data + i * 16 + 8);

        k1 *= c1;
        k1 = rotl64(k1, 31);
        k1 *= c2;
        h1 ^= k1;

        h1 = rotl64(h1, 27);
        h1 += h2;
        h1 = h1 * 5 + 0x52dce729ULL;

        k2 *= c2;
        k2 = rotl64(k2, 33);
        k2 *= c1;
        h2 ^= k2;

        h2 = rotl64(h2, 31);
        h2 += h1;
        h2 = h2 * 5 + 0x38495ab5ULL;
    }

    const uint8_t* tail = data + nblocks * 16;
    uint64_t k1 = 0;
    uint64_t k2 = 0;

    switch (len & 15) {
        case 15: k2 ^= static_cast<uint64_t>(tail[14]) << 48; [[fallthrough]];
        case 14: k2 ^= static_cast<uint64_t>(tail[13]) << 40; [[fallthrough]];
        case 13: k2 ^= static_cast<uint64_t>(tail[12]) << 32; [[fallthrough]];
        case 12: k2 ^= static_cast<uint64_t>(tail[11]) << 24; [[fallthrough]];
        case 11: k2 ^= static_cast<uint64_t>(tail[10]) << 16; [[fallthrough]];
        case 10: k2 ^= static_cast<uint64_t>(tail[9]) << 8; [[fallthrough]];
        case 9:
            k2 ^= static_cast<uint64_t>(tail[8]);
            k2 *= c2;
            k2 = rotl64(k2, 33);
            k2 *= c1;
            h2 ^= k2;
            [[fallthrough]];
        case 8: k1 ^= static_cast<uint64_t>(tail[7]) << 56; [[fallthrough]];
        case 7: k1 ^= static_cast<uint64_t>(tail[6]) << 48; [[fallthrough]];
        case 6: k1 ^= static_cast<uint64_t>(tail[5]) << 40; [[fallthrough]];
        case 5: k1 ^= static_cast<uint64_t>(tail[4]) << 32; [[fallthrough]];
        case 4: k1 ^= static_cast<uint64_t>(tail[3]) << 24; [[fallthrough]];
        case 3: k1 ^= static_cast<uint64_t>(tail[2]) << 16; [[fallthrough]];
        case 2: k1 ^= static_cast<uint64_t>(tail[1]) << 8; [[fallthrough]];
        case 1:
            k1 ^= static_cast<uint64_t>(tail[0]);
            k1 *= c1;
            k1 = rotl64(k1, 31);
            k1 *= c2;
            h1 ^= k1;
    }

    h1 ^= len;
    h2 ^= len;

    h1 += h2;
    h2 += h1;

    h1 = fmix64(h1);
    h2 = fmix64(h2);

    h1 += h2;
    h2 += h1;

    return MurmurHash128{h1, h2};
}

inline uint64_t MurmurHash64(const void* key, size_t len, uint32_t seed = 0) {
    return MurmurHash3_x64_128(key, len, seed).h1;
}

inline uint64_t MurmurHash64(std::string_view s, uint32_t seed = 0) {
    return MurmurHash64(s.data(), s.size(), seed);
}

}  // namespace coropact::ds
