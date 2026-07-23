#include <gtest/gtest.h>

#include <atomic>
#include <string>

#include "coropact/memory/object_pool.h"

namespace {

struct TrackedObject {
    inline static std::atomic<int> live_count{0};
    inline static std::atomic<int> ctor_count{0};
    inline static std::atomic<int> dtor_count{0};

    int value;
    std::string payload;

    TrackedObject(int v, std::string p)
        : value(v), payload(std::move(p)) {
        ++live_count;
        ++ctor_count;
    }

    ~TrackedObject() {
        --live_count;
        ++dtor_count;
    }
};

}  // namespace

TEST(ObjectPoolTest, AcquireAndReleaseManageLifetime) {
    TrackedObject::live_count = 0;
    TrackedObject::ctor_count = 0;
    TrackedObject::dtor_count = 0;

    coropact::memory::ObjectPool<TrackedObject, 2> pool;

    auto* first = pool.Acquire(1, "alpha");
    auto* second = pool.Acquire(2, "beta");

    ASSERT_NE(first, nullptr);
    ASSERT_NE(second, nullptr);
    EXPECT_EQ(first->value, 1);
    EXPECT_EQ(second->payload, "beta");
    EXPECT_EQ(pool.used_count(), 2u);
    EXPECT_EQ(TrackedObject::live_count.load(), 2);

    pool.Release(first);
    pool.Release(second);

    EXPECT_EQ(pool.used_count(), 0u);
    EXPECT_EQ(pool.free_count(), 2u);
    EXPECT_EQ(TrackedObject::live_count.load(), 0);
    EXPECT_EQ(TrackedObject::dtor_count.load(), 2);
}

TEST(ObjectPoolTest, AcquireFallsBackToHeapWhenExhausted) {
    coropact::memory::ObjectPool<TrackedObject, 1> pool;

    auto* first = pool.Acquire(7, "only");
    auto* second = pool.Acquire(8, "extra");

    ASSERT_NE(first, nullptr);
    ASSERT_NE(second, nullptr);
    EXPECT_TRUE(pool.owns(first));
    EXPECT_FALSE(pool.owns(second));
    EXPECT_EQ(pool.overflow_count(), 1u);

    pool.Release(first);
    pool.Release(second);
}

TEST(ObjectPoolTest, AcquireScopedReturnsObjectAutomatically) {
    TrackedObject::live_count = 0;
    TrackedObject::ctor_count = 0;
    TrackedObject::dtor_count = 0;

    coropact::memory::ObjectPool<TrackedObject, 1> pool;

    {
        auto scoped = pool.AcquireScoped(9, "scoped");
        ASSERT_NE(scoped, nullptr);
        EXPECT_EQ(scoped->value, 9);
        EXPECT_EQ(pool.used_count(), 1u);
        EXPECT_EQ(TrackedObject::live_count.load(), 1);
    }

    EXPECT_EQ(pool.used_count(), 0u);
    EXPECT_EQ(pool.free_count(), 1u);
    EXPECT_EQ(TrackedObject::live_count.load(), 0);
    EXPECT_EQ(TrackedObject::dtor_count.load(), 1);
}

TEST(ObjectPoolTest, OwnsRecognizesPoolPointers) {
    coropact::memory::ObjectPool<TrackedObject, 1> pool;

    auto* obj = pool.Acquire(11, "owned");
    ASSERT_NE(obj, nullptr);
    EXPECT_TRUE(pool.owns(obj));

    TrackedObject external(12, "external");
    EXPECT_FALSE(pool.owns(&external));

    pool.Release(obj);
}
