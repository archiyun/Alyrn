#include "runtime/memory/memory_pool.h"

#include <cstdint>
#include <exception>
#include <iostream>
#include <thread>
#include <vector>

namespace {

bool Expect(bool condition, const char* message) {
    if (!condition) {
        std::cerr << "[FAIL] " << message << '\n';
        return false;
    }
    return true;
}

bool TestAllocateAndReuse() {
    runtime::memory::MemoryPool<sizeof(int), alignof(int), 4> pool;

    void* a = pool.Allocate();
    void* b = pool.Allocate();
    if (!Expect(a != nullptr, "first allocation should succeed")) return false;
    if (!Expect(b != nullptr, "second allocation should succeed")) return false;
    if (!Expect(a != b, "pool should return distinct slots before free")) return false;

    pool.Deallocate(a);
    void* c = pool.Allocate();

    if (!Expect(c == a, "pool should reuse returned slot")) return false;
    if (!Expect(pool.free_count() == 2, "free_count should match after reuse")) return false;
    return true;
}

bool TestExhaustion() {
    runtime::memory::MemoryPool<sizeof(int), alignof(int), 2> pool;
    void* a = pool.Allocate();
    void* b = pool.Allocate();
    void* c = pool.Allocate();

    if (!Expect(a != nullptr && b != nullptr, "initial allocations should succeed")) return false;
    if (!Expect(c == nullptr, "allocation past capacity should return nullptr")) return false;
    if (!Expect(pool.used_count() == 2, "used_count should equal capacity when exhausted")) return false;
    return true;
}

bool TestOwns() {
    runtime::memory::MemoryPool<sizeof(std::uint64_t), alignof(std::uint64_t), 4> pool;
    void* p = pool.Allocate();
    int stack_value = 0;

    if (!Expect(pool.owns(p), "owns should accept pool pointers")) return false;
    if (!Expect(!pool.owns(&stack_value), "owns should reject stack pointers")) return false;
    if (!Expect(!pool.owns(nullptr), "owns should reject nullptr")) return false;

    pool.Deallocate(p);
    return true;
}

bool TestConcurrentAllocateAndFree() {
    runtime::memory::MemoryPool<sizeof(int), alignof(int), 256> pool;
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

    if (!Expect(pool.free_count() == pool.capacity(), "all slots should be returned after concurrent test")) return false;
    if (!Expect(pool.used_count() == 0, "used_count should be zero after concurrent test")) return false;
    return true;
}

}  // namespace

int main() {
    try {
        if (!TestAllocateAndReuse()) return 1;
        if (!TestExhaustion()) return 1;
        if (!TestOwns()) return 1;
        if (!TestConcurrentAllocateAndFree()) return 1;
    } catch (const std::exception& ex) {
        std::cerr << "[FAIL] unexpected exception: " << ex.what() << '\n';
        return 1;
    } catch (...) {
        std::cerr << "[FAIL] unexpected unknown exception\n";
        return 1;
    }

    std::cout << "[PASS] memory_pool_smoke_test\n";
    return 0;
}
