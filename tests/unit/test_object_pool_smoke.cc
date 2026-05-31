#include <atomic>
#include <exception>
#include <iostream>
#include <string>

#include "runtime/memory/object_pool.h"

namespace {

struct TrackedObject {
    inline static std::atomic<int> live_count{0};
    inline static std::atomic<int> dtor_count{0};

    int value;
    std::string payload;

    TrackedObject(int v, std::string p)
        : value(v), payload(std::move(p)) {
        ++live_count;
    }

    ~TrackedObject() {
        --live_count;
        ++dtor_count;
    }
};

bool Expect(bool condition, const char* message) {
    if (!condition) {
        std::cerr << "[FAIL] " << message << '\n';
        return false;
    }
    return true;
}

bool TestAcquireAndRelease() {
    TrackedObject::live_count = 0;
    TrackedObject::dtor_count = 0;

    runtime::memory::ObjectPool<TrackedObject, 2> pool;
    auto* first = pool.Acquire(1, "alpha");
    auto* second = pool.Acquire(2, "beta");

    if (!Expect(first != nullptr, "first acquire should succeed")) return false;
    if (!Expect(second != nullptr, "second acquire should succeed")) return false;
    if (!Expect(pool.used_count() == 2, "used_count should be updated after acquire")) return false;
    if (!Expect(first->payload == "alpha", "first object payload should match")) return false;

    pool.Release(first);
    pool.Release(second);

    if (!Expect(pool.used_count() == 0, "used_count should return to zero after release")) return false;
    if (!Expect(pool.free_count() == pool.capacity(), "all slots should be returned after release")) return false;
    if (!Expect(TrackedObject::live_count.load() == 0, "all tracked objects should be destroyed")) return false;
    if (!Expect(TrackedObject::dtor_count.load() == 2, "destructor should be called for each released object")) return false;
    return true;
}

bool TestAcquireScoped() {
    TrackedObject::live_count = 0;
    TrackedObject::dtor_count = 0;

    runtime::memory::ObjectPool<TrackedObject, 1> pool;
    {
        auto scoped = pool.AcquireScoped(7, "scoped");
        if (!Expect(static_cast<bool>(scoped), "AcquireScoped should return a valid handle")) return false;
        if (!Expect(pool.used_count() == 1, "used_count should increase while scoped object is alive")) return false;
        if (!Expect(scoped->value == 7, "scoped object should preserve constructor value")) return false;
    }

    if (!Expect(pool.used_count() == 0, "scoped object should be returned automatically")) return false;
    if (!Expect(pool.free_count() == pool.capacity(), "all slots should be available after scoped object destruction")) return false;
    if (!Expect(TrackedObject::live_count.load() == 0, "scoped object should be destroyed")) return false;
    return true;
}

bool TestExhaustion() {
    runtime::memory::ObjectPool<TrackedObject, 1> pool;

    auto* first = pool.Acquire(3, "only");
    auto* second = pool.Acquire(4, "extra");

    if (!Expect(first != nullptr, "first acquire should succeed")) return false;
    if (!Expect(second != nullptr, "acquire past capacity should fall back to heap, not return null")) return false;
    if (!Expect(pool.owns(first), "first should be pool-owned")) return false;
    if (!Expect(!pool.owns(second), "overflow object should not be pool-owned")) return false;
    if (!Expect(pool.overflow_count() == 1, "overflow_count should record the spill")) return false;

    pool.Release(first);
    pool.Release(second);
    return true;
}

bool TestOwns() {
    runtime::memory::ObjectPool<TrackedObject, 1> pool;

    auto* obj = pool.Acquire(5, "owned");
    TrackedObject external(6, "external");

    if (!Expect(obj != nullptr, "acquire should succeed")) return false;
    if (!Expect(pool.owns(obj), "owns should accept object allocated from the pool")) return false;
    if (!Expect(!pool.owns(&external), "owns should reject external object")) return false;

    pool.Release(obj);
    return true;
}

}  // namespace

int main() {
    try {
        if (!TestAcquireAndRelease()) return 1;
        if (!TestAcquireScoped()) return 1;
        if (!TestExhaustion()) return 1;
        if (!TestOwns()) return 1;
    } catch (const std::exception& ex) {
        std::cerr << "[FAIL] unexpected exception: " << ex.what() << '\n';
        return 1;
    } catch (...) {
        std::cerr << "[FAIL] unexpected unknown exception\n";
        return 1;
    }

    std::cout << "[PASS] object_pool_smoke_test\n";
    return 0;
}
