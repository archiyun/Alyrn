/**
 * EPollPoller smoke tests
 *
 * EPollPoller is the concrete epoll backend accessed through EventLoop.
 * Channel::EnableReading / DisableAll / Remove each delegate to
 * EventLoop::UpdateChannel / RemoveChannel which call into EPollPoller.
 * Every test creates a fresh EventLoop (which owns its own EPollPoller),
 * exercises a specific code path, then tears down cleanly.
 *
 * Tests:
 *   1. Construction          - epoll_create1 succeeds
 *   2. ChannelRegistration   - EnableReading → epoll_ctl ADD, HasChannel = true
 *   3. ChannelRemoval        - Remove → epoll_ctl DEL + map erase, HasChannel = false
 *   4. PollDetectsReadEvent  - write to socketpair, epoll_wait returns readable channel
 *   5. DisableAllKeepsInMap  - DisableAll → kDeleted, still in map, events suppressed
 *   6. ReenableAfterDisable  - kDeleted → EnableReading again (EPOLL_CTL_ADD), events resume
 *   7. EventsVectorResizes   - register > kInitEventListSize(16) channels, all fire
 */

#include "runtime/net/channel.h"
#include "runtime/net/event_loop.h"
#include "runtime/time/timestamp.h"

#include <array>
#include <cerrno>
#include <cstring>
#include <exception>
#include <iostream>
#include <memory>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

namespace {

bool Expect(bool condition, const char* message) {
    if (!condition) {
        std::cerr << "[FAIL] " << message << '\n';
        return false;
    }
    return true;
}

// ──────────────────────────────────────────────
// Test 1: EPollPoller 构造成功 (epoll_create1)
// ──────────────────────────────────────────────
bool TestConstruction() {
    // EventLoop 构造时调用 Poller::NewDefaultPoller → new EPollPoller
    // EPollPoller 构造时调用 epoll_create1(EPOLL_CLOEXEC)
    // 若 epollfd < 0 会 LOG_FATAL + abort，走到这里说明成功
    runtime::net::EventLoop loop;
    return true;
}

// ──────────────────────────────────────────────
// Test 2: Channel 注册 → epoll_ctl ADD，HasChannel = true
// ──────────────────────────────────────────────
bool TestChannelRegistration() {
    runtime::net::EventLoop loop;

    std::array<int, 2> fds{};
    if (!Expect(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds.data()) == 0,
                "socketpair creation failed")) {
        return false;
    }

    runtime::net::Channel ch(&loop, fds[0]);
    ch.SetReadCallback([](runtime::time::Timestamp) {});
    // EnableReading → events_ |= kReadEvent → Update()
    //   → loop.UpdateChannel → EPollPoller::UpdateChannel (kNew → kAdded, epoll_ctl ADD)
    ch.EnableReading();

    const bool registered = Expect(loop.HasChannel(&ch),
        "channel should appear in Poller after EnableReading");

    ch.DisableAll();
    ch.Remove();
    ::close(fds[0]);
    ::close(fds[1]);
    return registered;
}

// ──────────────────────────────────────────────
// Test 3: Channel 移除 → map erase，HasChannel = false
// ──────────────────────────────────────────────
bool TestChannelRemoval() {
    runtime::net::EventLoop loop;

    std::array<int, 2> fds{};
    if (!Expect(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds.data()) == 0,
                "socketpair creation failed")) {
        return false;
    }

    runtime::net::Channel ch(&loop, fds[0]);
    ch.SetReadCallback([](runtime::time::Timestamp) {});
    ch.EnableReading();

    // DisableAll → kAdded → kDeleted (epoll_ctl DEL，但 map 中仍保留)
    // Remove    → EPollPoller::RemoveChannel → channels_.erase(fd)
    ch.DisableAll();
    ch.Remove();

    const bool removed = Expect(!loop.HasChannel(&ch),
        "channel should be absent from Poller after Remove");

    ::close(fds[0]);
    ::close(fds[1]);
    return removed;
}

// ──────────────────────────────────────────────
// Test 4: epoll_wait 检测可读事件
// ──────────────────────────────────────────────
bool TestPollDetectsReadEvent() {
    runtime::net::EventLoop loop;

    std::array<int, 2> fds{};
    if (!Expect(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds.data()) == 0,
                "socketpair creation failed")) {
        return false;
    }

    bool read_called = false;
    runtime::net::Channel ch(&loop, fds[0]);
    ch.SetReadCallback([&](runtime::time::Timestamp) {
        char buf[4];
        ::read(fds[0], buf, sizeof(buf));
        read_called = true;
        loop.Quit();    // 收到读事件后退出 loop
    });
    ch.EnableReading();

    // 先写数据，保证第一轮 epoll_wait 就能返回
    if (!Expect(::write(fds[1], "x", 1) == 1, "write to peer failed")) {
        ch.DisableAll(); ch.Remove();
        ::close(fds[0]); ::close(fds[1]);
        return false;
    }

    loop.Loop();

    ch.DisableAll();
    ch.Remove();
    ::close(fds[0]);
    ::close(fds[1]);
    return Expect(read_called,
        "read callback should be invoked when fd is readable");
}

// ──────────────────────────────────────────────
// Test 5: DisableAll 后 channel 仍在 map，但 epoll 不再分发事件
// ──────────────────────────────────────────────
bool TestDisableAllKeepsChannelInMap() {
    runtime::net::EventLoop loop;

    std::array<int, 2> fds{};
    if (!Expect(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds.data()) == 0,
                "socketpair creation failed")) {
        return false;
    }

    runtime::net::Channel ch(&loop, fds[0]);
    ch.SetReadCallback([](runtime::time::Timestamp) {});
    ch.EnableReading();   // kNew → kAdded

    // DisableAll: kAdded → kDeleted，epoll_ctl DEL，但 channels_ map 不变
    ch.DisableAll();

    const bool still_in_map = Expect(loop.HasChannel(&ch),
        "DisableAll should keep channel in Poller map (kDeleted state)");

    // 向 peer 写数据；channel 已从 epoll 移除，不应触发回调
    bool read_called = false;
    ch.SetReadCallback([&](runtime::time::Timestamp) { read_called = true; });
    ::write(fds[1], "x", 1);

    // 用定时器驱动退出，不依赖 read 事件
    loop.RunAfter(0.02, [&] { loop.Quit(); });
    loop.Loop();

    const bool not_fired = Expect(!read_called,
        "disabled channel must not receive read events");

    ch.Remove();
    ::close(fds[0]);
    ::close(fds[1]);
    return still_in_map && not_fired;
}

// ──────────────────────────────────────────────
// Test 6: DisableAll 后 re-enable → kDeleted → kAdded (EPOLL_CTL_ADD)，事件恢复
// ──────────────────────────────────────────────
bool TestReenableAfterDisable() {
    runtime::net::EventLoop loop;

    std::array<int, 2> fds{};
    if (!Expect(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds.data()) == 0,
                "socketpair creation failed")) {
        return false;
    }

    bool read_called = false;
    runtime::net::Channel ch(&loop, fds[0]);
    ch.SetReadCallback([&](runtime::time::Timestamp) {
        char buf[4];
        ::read(fds[0], buf, sizeof(buf));
        read_called = true;
        loop.Quit();
    });

    ch.EnableReading();   // kNew → kAdded
    ch.DisableAll();      // kAdded → kDeleted, EPOLL_CTL_DEL
    ch.EnableReading();   // kDeleted → kAdded, EPOLL_CTL_ADD (重新注册)

    ::write(fds[1], "x", 1);
    loop.Loop();

    ch.DisableAll();
    ch.Remove();
    ::close(fds[0]);
    ::close(fds[1]);
    return Expect(read_called,
        "re-enabled channel should receive read events after re-registration");
}

// ──────────────────────────────────────────────
// Test 7: events_ 动态扩容（注册超过 kInitEventListSize=16 的 channel）
// ──────────────────────────────────────────────
bool TestEventsVectorResizes() {
    runtime::net::EventLoop loop;

    // 注册 20 个 channel，均在同一轮 epoll_wait 触发
    // 前 16 个塞满 events_，触发扩容到 32；后续轮次处理剩余
    constexpr int N = 20;
    std::array<std::array<int, 2>, N> pairs{};
    for (auto& p : pairs) {
        if (::socketpair(AF_UNIX, SOCK_STREAM, 0, p.data()) < 0) {
            for (auto& q : pairs) {
                if (q[0] >= 0) { ::close(q[0]); ::close(q[1]); }
            }
            return Expect(false, "socketpair creation failed");
        }
    }

    int trigger_count = 0;
    std::vector<std::unique_ptr<runtime::net::Channel>> channels;
    channels.reserve(N);

    for (int i = 0; i < N; ++i) {
        auto ch = std::make_unique<runtime::net::Channel>(&loop, pairs[i][0]);
        // 必须消费数据：level-triggered 模式下若不 read，
        // fd 持续可读，每轮 epoll_wait 都会重复触发，counter 会跳过 == N
        const int read_fd = pairs[i][0];
        ch->SetReadCallback([&, read_fd](runtime::time::Timestamp) {
            char buf[4];
            ::read(read_fd, buf, sizeof(buf));
            ++trigger_count;
            if (trigger_count == N) {
                loop.Quit();
            }
        });
        ch->EnableReading();
        channels.push_back(std::move(ch));
    }

    // 所有 peer 端同时写，让 epoll_wait 在第一次调用就看到 >= 16 个事件
    for (auto& p : pairs) {
        ::write(p[1], "x", 1);
    }

    // 备用：若某些事件延迟分散到多轮，定时器兜底
    loop.RunAfter(2.0, [&] { loop.Quit(); });
    loop.Loop();

    for (auto& ch : channels) { ch->DisableAll(); ch->Remove(); }
    for (auto& p : pairs) { ::close(p[0]); ::close(p[1]); }

    return Expect(trigger_count == N,
        "all 20 channels should fire (exercises events_ resize path)");
}

}  // namespace

int main() {
    struct Test {
        bool (*fn)();
        const char* name;
    };

    const Test tests[] = {
        { TestConstruction,              "Construction" },
        { TestChannelRegistration,       "ChannelRegistration" },
        { TestChannelRemoval,            "ChannelRemoval" },
        { TestPollDetectsReadEvent,      "PollDetectsReadEvent" },
        { TestDisableAllKeepsChannelInMap, "DisableAllKeepsChannelInMap" },
        { TestReenableAfterDisable,      "ReenableAfterDisable" },
        { TestEventsVectorResizes,       "EventsVectorResizes" },
    };

    int failed = 0;
    for (const auto& t : tests) {
        try {
            if (t.fn()) {
                std::cout << "[PASS] " << t.name << '\n';
            } else {
                std::cerr << "[FAIL] " << t.name << '\n';
                ++failed;
            }
        } catch (const std::exception& ex) {
            std::cerr << "[FAIL] " << t.name
                      << " threw: " << ex.what() << '\n';
            ++failed;
        } catch (...) {
            std::cerr << "[FAIL] " << t.name << " threw unknown exception\n";
            ++failed;
        }
    }

    if (failed == 0) {
        std::cout << "[PASS] epoll_poller_smoke_test (" << std::size(tests) << " cases)\n";
    }
    return failed == 0 ? 0 : 1;
}
