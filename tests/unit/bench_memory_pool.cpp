// bench_memory_pool.cpp
//
// Measures throughput of MemoryPool / ObjectPool vs system new/delete.
//
// Scenarios
//   1. Sequential   — fill entire pool, then free entire pool (R rounds)
//   2. Interleaved  — alloc-1 / free-1 per iteration (pool never fills up)
//   3. Batch-32     — alloc 32, free 32, repeat
//   4. ObjectPool   — includes Task construction + destruction
//   5. NullMutex    — pool with lock overhead removed (single-thread ceiling)
//   6. Multi-thread — 8 threads interleaved alloc/free on shared pool

#include "runtime/memory/memory_pool.h"
#include "runtime/memory/object_pool.h"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <thread>
#include <vector>

// ---------------------------------------------------------------------------
// Benchmark object
// ---------------------------------------------------------------------------
struct alignas(64) Task {
    std::uint64_t id;
    std::uint32_t priority;
    std::uint32_t retries;
    char          name[32];

    Task() noexcept : id(0), priority(0), retries(0) { name[0] = '\0'; }
    Task(std::uint64_t i, std::uint32_t p, const char* n) noexcept
        : id(i), priority(p), retries(0) {
        std::size_t k = 0;
        while (n[k] && k < 31) { name[k] = n[k]; ++k; }
        name[k] = '\0';
    }
    ~Task() noexcept { id = 0; }
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
using Clock   = std::chrono::high_resolution_clock;
using Nanosec = std::chrono::nanoseconds;

static Nanosec elapsed(Clock::time_point t0) {
    return std::chrono::duration_cast<Nanosec>(Clock::now() - t0);
}

template <typename T>
static void sink(T const& val) {
    asm volatile("" : : "r,m"(val) : "memory");
}

static void print_header() {
    std::cout
        << '\n'
        << std::left  << std::setw(44) << "Scenario"
        << std::right << std::setw(14) << "Pool (ns/op)"
        << std::setw(14) << "new/del (ns/op)"
        << std::setw(10) << "speedup"
        << '\n'
        << std::string(82, '-') << '\n';
}

static void print_row(const char* label, Nanosec pool_ns, Nanosec sys_ns, long long ops) {
    double pp = static_cast<double>(pool_ns.count()) / ops;
    double sp = static_cast<double>(sys_ns.count())  / ops;
    std::cout
        << std::left  << std::setw(44) << label
        << std::right << std::fixed << std::setprecision(1)
        << std::setw(13) << pp << " "
        << std::setw(13) << sp << " "
        << std::setw(8)  << (sp / pp) << "x"
        << '\n';
}

// ---------------------------------------------------------------------------
// 1. Sequential: fill pool → free pool, repeated R times
// ---------------------------------------------------------------------------
static void bench_sequential() {
    constexpr std::size_t kCap   = 2048;
    constexpr long long   kRound = 2000;
    constexpr long long   kOps   = kCap * kRound;

    void* buf[kCap];

    // pool
    runtime::memory::MemoryPool<sizeof(void*), alignof(void*), kCap> pool;
    auto t0 = Clock::now();
    for (long long r = 0; r < kRound; ++r) {
        for (std::size_t i = 0; i < kCap; ++i) buf[i] = pool.Allocate();
        for (std::size_t i = 0; i < kCap; ++i) pool.Deallocate(buf[i]);
    }
    auto pool_ns = elapsed(t0);

    // new/delete
    t0 = Clock::now();
    for (long long r = 0; r < kRound; ++r) {
        for (std::size_t i = 0; i < kCap; ++i) {
            buf[i] = ::operator new(sizeof(void*));
            sink(buf[i]);
        }
        for (std::size_t i = 0; i < kCap; ++i) ::operator delete(buf[i]);
    }
    auto sys_ns = elapsed(t0);

    print_row("1. sequential fill+drain (raw, mutex)", pool_ns, sys_ns, kOps);
}

// ---------------------------------------------------------------------------
// 2. Interleaved: alloc-1 / free-1, single thread
// ---------------------------------------------------------------------------
static void bench_interleaved() {
    constexpr std::size_t kCap = 64;
    constexpr long long   kN   = 2'000'000LL;

    // pool
    runtime::memory::MemoryPool<sizeof(void*), alignof(void*), kCap> pool;
    auto t0 = Clock::now();
    for (long long i = 0; i < kN; ++i) {
        void* p = pool.Allocate();
        sink(p);
        pool.Deallocate(p);
    }
    auto pool_ns = elapsed(t0);

    // new/delete
    t0 = Clock::now();
    for (long long i = 0; i < kN; ++i) {
        void* p = ::operator new(sizeof(void*));
        sink(p);
        ::operator delete(p);
    }
    auto sys_ns = elapsed(t0);

    print_row("2. interleaved alloc/free (raw, mutex)", pool_ns, sys_ns, kN);
}

// ---------------------------------------------------------------------------
// 3. Batch-32: alloc 32, free 32, repeat
// ---------------------------------------------------------------------------
static void bench_batch() {
    constexpr int         kBatch = 32;
    constexpr std::size_t kCap   = 256;
    constexpr long long   kRound = 100'000LL;
    constexpr long long   kOps   = kBatch * kRound;

    void* buf[kBatch];

    // pool
    runtime::memory::MemoryPool<sizeof(void*), alignof(void*), kCap> pool;
    auto t0 = Clock::now();
    for (long long r = 0; r < kRound; ++r) {
        for (int i = 0; i < kBatch; ++i) buf[i] = pool.Allocate();
        for (int i = 0; i < kBatch; ++i) pool.Deallocate(buf[i]);
    }
    auto pool_ns = elapsed(t0);

    // new/delete
    t0 = Clock::now();
    for (long long r = 0; r < kRound; ++r) {
        for (int i = 0; i < kBatch; ++i) { buf[i] = ::operator new(sizeof(void*)); sink(buf[i]); }
        for (int i = 0; i < kBatch; ++i) ::operator delete(buf[i]);
    }
    auto sys_ns = elapsed(t0);

    print_row("3. batch-32 alloc+free (raw, mutex)", pool_ns, sys_ns, kOps);
}

// ---------------------------------------------------------------------------
// 4. ObjectPool vs new/delete (includes ctor + dtor)
// ---------------------------------------------------------------------------
static void bench_object_pool() {
    constexpr std::size_t kCap   = 2048;
    constexpr long long   kRound = 500;
    constexpr long long   kOps   = kCap * kRound;

    Task* buf[kCap];

    // ObjectPool
    runtime::memory::ObjectPool<Task, kCap> pool;
    auto t0 = Clock::now();
    for (long long r = 0; r < kRound; ++r) {
        for (std::size_t i = 0; i < kCap; ++i)
            buf[i] = pool.Acquire(static_cast<uint64_t>(i), 1u, "task");
        for (std::size_t i = 0; i < kCap; ++i) pool.Release(buf[i]);
    }
    auto pool_ns = elapsed(t0);

    // new/delete
    t0 = Clock::now();
    for (long long r = 0; r < kRound; ++r) {
        for (std::size_t i = 0; i < kCap; ++i) {
            buf[i] = new Task(static_cast<uint64_t>(i), 1u, "task");
            sink(buf[i]);
        }
        for (std::size_t i = 0; i < kCap; ++i) delete buf[i];
    }
    auto sys_ns = elapsed(t0);

    print_row("4. ObjectPool vs new/delete (Task ctor)", pool_ns, sys_ns, kOps);
}

// ---------------------------------------------------------------------------
// 5. NullMutex pool — lock-free single-thread ceiling
// ---------------------------------------------------------------------------
static void bench_null_mutex() {
    constexpr std::size_t kCap = 64;
    constexpr long long   kN   = 2'000'000LL;

    // NullMutex pool
    runtime::memory::MemoryPool<sizeof(void*), alignof(void*), kCap,
                                runtime::memory::NullMutex> pool;
    auto t0 = Clock::now();
    for (long long i = 0; i < kN; ++i) {
        void* p = pool.Allocate();
        sink(p);
        pool.Deallocate(p);
    }
    auto pool_ns = elapsed(t0);

    // new/delete
    t0 = Clock::now();
    for (long long i = 0; i < kN; ++i) {
        void* p = ::operator new(sizeof(void*));
        sink(p);
        ::operator delete(p);
    }
    auto sys_ns = elapsed(t0);

    print_row("5. interleaved (NullMutex, no lock)", pool_ns, sys_ns, kN);
}

// ---------------------------------------------------------------------------
// 6. Multi-thread: 8 threads, interleaved alloc/free on one pool
// ---------------------------------------------------------------------------
static void bench_multithreaded() {
    constexpr int         kThreads     = 8;
    constexpr std::size_t kCap         = 512;    // pool slots
    constexpr long long   kItersThread = 100'000LL;
    constexpr long long   kOps         = kItersThread * kThreads;

    std::atomic<int> ready{0};

    // pool
    {
        runtime::memory::MemoryPool<sizeof(void*), alignof(void*), kCap> pool;
        std::vector<std::thread> ts;
        ts.reserve(kThreads);

        auto t0 = Clock::now();
        for (int t = 0; t < kThreads; ++t) {
            ts.emplace_back([&] {
                ++ready;
                while (ready.load(std::memory_order_acquire) < kThreads) {}
                for (long long i = 0; i < kItersThread; ++i) {
                    void* p = nullptr;
                    do { p = pool.Allocate(); } while (!p);
                    sink(p);
                    pool.Deallocate(p);
                }
            });
        }
        for (auto& t : ts) t.join();
        auto pool_ns = elapsed(t0);

        // new/delete
        ready = 0;
        ts.clear();
        auto t1 = Clock::now();
        for (int t = 0; t < kThreads; ++t) {
            ts.emplace_back([&] {
                ++ready;
                while (ready.load(std::memory_order_acquire) < kThreads) {}
                for (long long i = 0; i < kItersThread; ++i) {
                    void* p = ::operator new(sizeof(void*));
                    sink(p);
                    ::operator delete(p);
                }
            });
        }
        for (auto& t : ts) t.join();
        auto sys_ns = elapsed(t1);

        print_row("6. 8-thread interleaved (mutex pool)", pool_ns, sys_ns, kOps);
    }
}

// ---------------------------------------------------------------------------
int main() {
    std::cout << "Memory pool benchmark\n";
    std::cout << "sizeof(Task) = " << sizeof(Task) << " bytes, "
              << "sizeof(void*) = " << sizeof(void*) << " bytes\n";

    print_header();
    bench_sequential();
    bench_interleaved();
    bench_batch();
    bench_object_pool();
    bench_null_mutex();
    bench_multithreaded();

    std::cout << '\n'
              << "Notes:\n"
              << "  Scenarios 1-3,5: raw bytes (no ctor/dtor)\n"
              << "  Scenario 4: full object lifecycle (ctor + dtor included)\n"
              << "  Scenario 5: NullMutex removes lock cost — shows single-thread ceiling\n"
              << "  Scenario 6: mutex pool under 8-thread contention vs glibc malloc\n";
    return 0;
}
