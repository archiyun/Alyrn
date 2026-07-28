#include <gtest/gtest.h>

#include <cstdint>
#include <thread>
#include <vector>

#include "coropact/memory/memory_pool.h"

TEST(MemoryPoolTest, AllocateAndDeallocateReuseSlots) {
    coropact::memory::MemoryPool<sizeof(int), alignof(int), 4> pool;

    void* a = pool.Allocate();
    void* b = pool.Allocate();

    ASSERT_NE(a, nullptr);
    ASSERT_NE(b, nullptr);
    EXPECT_NE(a, b);
    EXPECT_EQ(pool.FreeCount(), 2u);
    EXPECT_EQ(pool.UsedCount(), 2u);

    pool.Deallocate(a);
    EXPECT_EQ(pool.FreeCount(), 3u);

    void* c = pool.Allocate();
    EXPECT_EQ(c, a);
    EXPECT_EQ(pool.FreeCount(), 2u);
}

TEST(MemoryPoolTest, ReturnsNullWhenExhausted) {
    coropact::memory::MemoryPool<sizeof(int), alignof(int), 2> pool;

    void* a = pool.Allocate();
    void* b = pool.Allocate();
    void* c = pool.Allocate();

    ASSERT_NE(a, nullptr);
    ASSERT_NE(b, nullptr);
    EXPECT_EQ(c, nullptr);
    EXPECT_EQ(pool.FreeCount(), 0u);
    EXPECT_EQ(pool.UsedCount(), 2u);
}

TEST(MemoryPoolTest, OwnsRecognizesPoolAddresses) {
    coropact::memory::MemoryPool<sizeof(std::uint64_t), alignof(std::uint64_t), 4> pool;

    void* p = pool.Allocate();
    ASSERT_NE(p, nullptr);

    EXPECT_TRUE(pool.Owns(p));

    int stack_value = 0;
    EXPECT_FALSE(pool.Owns(&stack_value));
    EXPECT_FALSE(pool.Owns(nullptr));

    pool.Deallocate(p);
}

TEST(MemoryPoolTest, ConcurrentAllocateAndDeallocatePreservesCapacity) {
    coropact::memory::MemoryPool<sizeof(int), alignof(int), 256> pool;
    constexpr int kThreads = 8;
    constexpr int kIterations = 2000;

    std::vector<std::thread> threads;
    threads.reserve(kThreads);

    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&pool] {
            std::vector<void*> local;
            local.reserve(32);

            for (int i = 0; i < kIterations; ++i) {
                void* ptr = nullptr;
                while (ptr == nullptr) {
                    ptr = pool.Allocate();
                    if (ptr == nullptr && !local.empty()) {
                        pool.Deallocate(local.back());
                        local.pop_back();
                    }
                }

                local.push_back(ptr);

                if (local.size() >= 16) {
                    pool.Deallocate(local.back());
                    local.pop_back();
                }
            }

            for (void* ptr : local) {
                pool.Deallocate(ptr);
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(pool.FreeCount(), pool.Capacity());
    EXPECT_EQ(pool.UsedCount(), 0u);
}
