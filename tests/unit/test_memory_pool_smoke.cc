#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstdint>
#include <exception>
#include <iostream>
#include <new>
#include <thread>
#include <vector>

#include "alyrn/detail/memory_pool.h"

namespace {

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
        return Expect(false, "fork failed for MemoryPool invariant test");
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
                  "MemoryPool invariant must terminate with SIGABRT");
}

void DeallocateForeignPointer() {
    alyrn::detail::MemoryPool<sizeof(int), alignof(int), 2> pool;
    void* foreign = ::operator new(sizeof(void*));
    pool.Deallocate(foreign);
    ::operator delete(foreign);
}

void DeallocateSameSlotTwice() {
    alyrn::detail::MemoryPool<sizeof(int), alignof(int), 2> pool;
    void* slot = pool.Allocate();
    pool.Deallocate(slot);
    pool.Deallocate(slot);
}

bool TestAllocateAndReuse() {
    alyrn::detail::MemoryPool<sizeof(int), alignof(int), 4> pool;

    void* a = pool.Allocate();
    void* b = pool.Allocate();
    if (!Expect(a != nullptr, "first allocation should succeed")) return false;
    if (!Expect(b != nullptr, "second allocation should succeed")) return false;
    if (!Expect(a != b, "pool should return distinct slots before free")) return false;

    pool.Deallocate(a);
    void* c = pool.Allocate();

    if (!Expect(c == a, "pool should reuse returned slot")) return false;
    if (!Expect(pool.FreeCount() == 2, "FreeCount should match after reuse")) return false;
    return true;
}

bool TestExhaustion() {
    alyrn::detail::MemoryPool<sizeof(int), alignof(int), 2> pool;
    void* a = pool.Allocate();
    void* b = pool.Allocate();
    void* c = pool.Allocate();

    if (!Expect(a != nullptr && b != nullptr, "initial allocations should succeed")) return false;
    if (!Expect(c == nullptr, "allocation past capacity should return nullptr")) return false;
    if (!Expect(pool.UsedCount() == 2, "UsedCount should equal capacity when exhausted")) return false;
    return true;
}

bool TestOwns() {
    alyrn::detail::MemoryPool<sizeof(std::uint64_t), alignof(std::uint64_t), 4> pool;
    void* p = pool.Allocate();
    int stack_value = 0;

    if (!Expect(pool.Owns(p), "Owns should accept pool pointers")) return false;
    if (!Expect(!pool.Owns(&stack_value), "Owns should reject stack pointers")) return false;
    if (!Expect(!pool.Owns(nullptr), "Owns should reject nullptr")) return false;

    pool.Deallocate(p);
    return true;
}

bool TestConcurrentAllocateAndFree() {
    alyrn::detail::MemoryPool<sizeof(int), alignof(int), 256> pool;
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

    if (!Expect(pool.FreeCount() == pool.Capacity(), "all slots should be returned after concurrent test")) return false;
    if (!Expect(pool.UsedCount() == 0, "UsedCount should be zero after concurrent test")) return false;
    return true;
}

bool TestInvalidDeallocateTerminates() {
    return Expect(ExpectChildAbort(&DeallocateForeignPointer,
                                   "foreign Deallocate must terminate in every build"),
                  "foreign Deallocate was accepted") &&
           Expect(ExpectChildAbort(&DeallocateSameSlotTwice,
                                   "double Deallocate must terminate in every build"),
                  "double Deallocate was accepted");
}

}  // namespace

int main() {
    try {
        if (!TestAllocateAndReuse()) return 1;
        if (!TestExhaustion()) return 1;
        if (!TestOwns()) return 1;
        if (!TestConcurrentAllocateAndFree()) return 1;
        if (!TestInvalidDeallocateTerminates()) return 1;
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
