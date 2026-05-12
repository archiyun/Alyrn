// murmurhash3.h - MurmurHash3 32-bit
// 
// 引用: https//github.com/aappleby/smhasher (by Austin Appleby)
// 简化版本: 只保留 x86 32-bit 路径, 适用于一致性哈希场景( 64-bit key 空间过剩， 故采用 32-bit )

#pragma once

#include <cstdint>
#include <cstddef>
#include <string_view>

namespace runtime::base {

inline uint32_t rotl32(uint32_t x, int8_t r) {
    return (x << r) | (x >> (32 - r));
}

// MurmurHash3 x86 32-bit
// 输入: data 指针、字节长度、seed
// 输出: 32 位 hash
inline uint32_t MurmurHash3_x86_32(const void* key, size_t len, uint32_t seed = 0) {
    const uint8_t* data = static_cast<const uint8_t*>(key);
    const int nblocks = static_cast<int>(len / 4);

    uint32_t h1 = seed;

    constexpr uint32_t c1 = 0xcc9e2d51;
    constexpr uint32_t c2 = 0x1b873593;

    // 主循环: 每次处理 4 字节
    const uint32_t* blocks = reinterpret_cast<const uint32_t*>(data + nblocks * 4);
    for (int i = -nblocks; i; i++) {
        uint32_t k1 = blocks[i];

        k1 *= c1;
        k1 = rotl32(k1, 15);
        k1 *= c2;

        h1 ^= k1;
        h1 = rotl32(h1, 13);
        h1 = h1 * 5 + 0xe6546b64;
    }

    // 尾部: 处理剩余的 1~3 字节
    const uint8_t* tail = data + nblocks * 4;
    uint32_t k1 = 0;
    switch (len & 3) {
        case 3: k1 ^= static_cast<uint32_t>(tail[2]) << 16; [[fallthrough]];
        case 2: k1 ^= static_cast<uint32_t>(tail[1]) << 8;  [[fallthrough]];
        case 1:
            k1 ^= static_cast<uint32_t>(tail[0]);
            k1 *= c1;
            k1 = rotl32(k1, 15);
            k1 *= c2;
            h1 ^= k1;
    }

    // Finalization: 雪崩,把 hash 的所有位混合得更均匀
    h1 ^= static_cast<uint32_t>(len);
    h1 ^= h1 >> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >> 16;

    return h1;
}

// 便利重载
inline uint32_t MurmurHash3(std::string_view s, uint32_t seed = 0) {
    return MurmurHash3_x86_32(s.data(), s.size(), seed);
}

} // namespace runtime::base