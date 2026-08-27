#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <csignal>
#include <exception>
#include <cstdio>
#include <iostream>
#include <string>

#include "alyrn/memory/object_pool.h"

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

bool ExpectChildAbort(void (*entry)(), const char* message) {
    const pid_t child = ::fork();
    if (child < 0) {
        return Expect(false, "fork failed for ObjectPool invariant test");
    }
    if (child == 0) {
        (void)::freopen("/dev/null", "w", stderr);
        entry();
        ::_exit(0);
    }

    int status = 0;
    while (::waitpid(child, &status, 0) < 0 && errno == EINTR) {
    }
    return Expect(WIFSIGNALED(status), message) &&
           Expect(WTERMSIG(status) == SIGABRT,
                  "ObjectPool invariant must terminate with SIGABRT");
}

bool TestAcquireAndRelease() {
    TrackedObject::live_count = 0;
    TrackedObject::dtor_count = 0;

    alyrn::memory::ObjectPool<TrackedObject, 2> pool;
    auto* first = pool.Acquire(1, "alpha");
    auto* second = pool.Acquire(2, "beta");

    if (!Expect(first != nullptr, "first acquire should succeed")) return false;
    if (!Expect(second != nullptr, "second acquire should succeed")) return false;
    if (!Expect(pool.UsedCount() == 2, "UsedCount should be updated after acquire")) return false;
    if (!Expect(first->payload == "alpha", "first object payload should match")) return false;

    pool.Release(first);
    pool.Release(second);

    if (!Expect(pool.UsedCount() == 0, "UsedCount should return to zero after release")) return false;
    if (!Expect(pool.FreeCount() == pool.Capacity(), "all slots should be returned after release")) return false;
    if (!Expect(TrackedObject::live_count.load() == 0, "all tracked objects should be destroyed")) return false;
    if (!Expect(TrackedObject::dtor_count.load() == 2, "destructor should be called for each released object")) return false;
    return true;
}

bool TestAcquireScoped() {
    TrackedObject::live_count = 0;
    TrackedObject::dtor_count = 0;

    alyrn::memory::ObjectPool<TrackedObject, 1> pool;
    {
        auto scoped = pool.AcquireScoped(7, "scoped");
        if (!Expect(static_cast<bool>(scoped), "AcquireScoped should return a valid handle")) return false;
        if (!Expect(pool.UsedCount() == 1, "UsedCount should increase while scoped object is alive")) return false;
        if (!Expect(scoped->value == 7, "scoped object should preserve constructor value")) return false;
    }

    if (!Expect(pool.UsedCount() == 0, "scoped object should be returned automatically")) return false;
    if (!Expect(pool.FreeCount() == pool.Capacity(), "all slots should be available after scoped object destruction")) return false;
    if (!Expect(TrackedObject::live_count.load() == 0, "scoped object should be destroyed")) return false;
    return true;
}

bool TestExhaustion() {
    alyrn::memory::ObjectPool<TrackedObject, 1> pool;

    auto* first = pool.Acquire(3, "only");
    auto* second = pool.Acquire(4, "extra");

    if (!Expect(first != nullptr, "first acquire should succeed")) return false;
    if (!Expect(second != nullptr, "acquire past capacity should fall back to heap, not return null")) return false;
    if (!Expect(pool.Owns(first), "first should be pool-owned")) return false;
    if (!Expect(!pool.Owns(second), "overflow object should not be pool-owned")) return false;
    if (!Expect(pool.OverflowCount() == 1, "OverflowCount should record the spill")) return false;

    pool.Release(first);
    pool.Release(second);
    return true;
}

bool TestOwns() {
    alyrn::memory::ObjectPool<TrackedObject, 1> pool;

    auto* obj = pool.Acquire(5, "owned");
    TrackedObject external(6, "external");

    if (!Expect(obj != nullptr, "acquire should succeed")) return false;
    if (!Expect(pool.Owns(obj), "Owns should accept object allocated from the pool")) return false;
    if (!Expect(!pool.Owns(&external), "Owns should reject external object")) return false;

    pool.Release(obj);
    return true;
}

void ReleaseForeignHeapObject() {
    alyrn::memory::ObjectPool<TrackedObject, 1> pool;
    auto* foreign = new TrackedObject(7, "foreign");
    pool.Release(foreign);
}

void ReleaseOverflowFromAnotherPool() {
    alyrn::memory::ObjectPool<TrackedObject, 1> source;
    alyrn::memory::ObjectPool<TrackedObject, 1> destination;
    (void)source.Acquire(8, "pooled");
    auto* overflow = source.Acquire(9, "overflow");
    destination.Release(overflow);
}

void ReleasePoolObjectTwice() {
    alyrn::memory::ObjectPool<TrackedObject, 1> pool;
    auto* object = pool.Acquire(10, "pooled");
    pool.Release(object);
    pool.Release(object);
}

void DestroyWithLivePoolObject() {
    alyrn::memory::ObjectPool<TrackedObject, 1> pool;
    (void)pool.Acquire(11, "pooled");
}

void DestroyWithLiveOverflowObject() {
    alyrn::memory::ObjectPool<TrackedObject, 1> pool;
    (void)pool.Acquire(12, "pooled");
    (void)pool.Acquire(13, "overflow");
}

bool TestInvalidReleaseTerminates() {
    return Expect(ExpectChildAbort(&ReleaseForeignHeapObject,
                                   "foreign heap object Release must terminate in every build"),
                  "foreign heap object Release was accepted") &&
           Expect(ExpectChildAbort(&ReleaseOverflowFromAnotherPool,
                                   "cross-pool overflow Release must terminate in every build"),
                  "cross-pool overflow Release was accepted") &&
           Expect(ExpectChildAbort(&ReleasePoolObjectTwice,
                                   "duplicate pool Release must terminate before another destructor"),
                  "duplicate pool Release was accepted") &&
           Expect(ExpectChildAbort(&DestroyWithLivePoolObject,
                                   "destroying a pool with live pool objects must terminate"),
                  "live pool object survived ObjectPool destruction") &&
           Expect(ExpectChildAbort(&DestroyWithLiveOverflowObject,
                                   "destroying a pool with live overflow objects must terminate"),
                  "live overflow object survived ObjectPool destruction");
}

}  // namespace

int main() {
    try {
        if (!TestAcquireAndRelease()) return 1;
        if (!TestAcquireScoped()) return 1;
        if (!TestExhaustion()) return 1;
        if (!TestOwns()) return 1;
        if (!TestInvalidReleaseTerminates()) return 1;
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
