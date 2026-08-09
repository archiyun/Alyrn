/**
 * EPollPoller smoke tests
 *
 * detail::EPollPoller is the concrete epoll backend owned by EventLoop.
 * detail::Channel::EnableReading / DisableAll / Remove each delegate to the
 * loop's private poller-registration path.
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

#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstring>
#include <exception>
#include <iostream>
#include <memory>
#include <vector>

#include "coropact/reactor/detail/channel.h"
#include "coropact/reactor/loop.h"

namespace {

bool Expect(bool condition, const char* message) {
    if (!condition) {
        std::cerr << "[FAIL] " << message << '\n';
        return false;
    }
    return true;
}

void NoopRead(void*) noexcept {}

struct ReadAndQuitContext {
    int fd;
    bool* called;
    coropact::reactor::EventLoop* loop;
};

void DrainReadAndQuit(void* raw) noexcept {
    auto& context = *static_cast<ReadAndQuitContext*>(raw);
    char buf[4];
    ::read(context.fd, buf, sizeof(buf));
    *context.called = true;
    context.loop->RequestStop();
}

void MarkRead(void* raw) noexcept {
    *static_cast<bool*>(raw) = true;
}

struct ReadManyContext {
    int fd;
    int* count;
    int total;
    coropact::reactor::EventLoop* loop;
};

void DrainReadMany(void* raw) noexcept {
    auto& context = *static_cast<ReadManyContext*>(raw);
    char buf[4];
    ::read(context.fd, buf, sizeof(buf));
    ++*context.count;
    if (*context.count == context.total) {
        context.loop->RequestStop();
    }
}

// ──────────────────────────────────────────────
// Test 1: EventLoop construction succeeds (epoll_create1).
// ──────────────────────────────────────────────
bool TestConstruction() {
    // EventLoop creates the default poller, which is detail::EPollPoller on Linux.
    // Reaching this point means epoll_create1 succeeded.
    coropact::reactor::EventLoop loop;
    return true;
}

// ──────────────────────────────────────────────
// Test 2: Channel registration issues epoll_ctl ADD.
// ──────────────────────────────────────────────
bool TestChannelRegistration() {
    coropact::reactor::EventLoop loop;

    std::array<int, 2> fds{};
    if (!Expect(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds.data()) == 0,
                "socketpair creation failed")) {
        return false;
    }

    coropact::reactor::detail::Channel ch(&loop, fds[0]);
    ch.SetReadCallback(NoopRead, nullptr);
    ch.EnableReading();

    const bool registered = Expect(ch.IsRegistered(),
        "channel should appear in Poller after EnableReading");

    ch.DisableAll();
    ch.Remove();
    ::close(fds[0]);
    ::close(fds[1]);
    return registered;
}

// ──────────────────────────────────────────────
// Test 3: Channel removal erases the poller map entry.
// ──────────────────────────────────────────────
bool TestChannelRemoval() {
    coropact::reactor::EventLoop loop;

    std::array<int, 2> fds{};
    if (!Expect(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds.data()) == 0,
                "socketpair creation failed")) {
        return false;
    }

    coropact::reactor::detail::Channel ch(&loop, fds[0]);
    ch.SetReadCallback(NoopRead, nullptr);
    ch.EnableReading();

    ch.DisableAll();
    ch.Remove();

    const bool removed = Expect(!ch.IsRegistered(),
        "channel should be absent from Poller after Remove");

    ::close(fds[0]);
    ::close(fds[1]);
    return removed;
}

// ──────────────────────────────────────────────
// Test 4: epoll_wait detects a readable event.
// ──────────────────────────────────────────────
bool TestPollDetectsReadEvent() {
    coropact::reactor::EventLoop loop;

    std::array<int, 2> fds{};
    if (!Expect(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds.data()) == 0,
                "socketpair creation failed")) {
        return false;
    }

    bool read_called = false;
    coropact::reactor::detail::Channel ch(&loop, fds[0]);
    ReadAndQuitContext context{fds[0], &read_called, &loop};
    ch.SetReadCallback(DrainReadAndQuit, &context);
    ch.EnableReading();

    // Write first so the initial epoll_wait returns immediately.
    if (!Expect(::write(fds[1], "x", 1) == 1, "write to peer failed")) {
        ch.DisableAll(); ch.Remove();
        ::close(fds[0]); ::close(fds[1]);
        return false;
    }

    loop.Run();

    ch.DisableAll();
    ch.Remove();
    ::close(fds[0]);
    ::close(fds[1]);
    return Expect(read_called,
        "read callback should be invoked when fd is readable");
}

// ──────────────────────────────────────────────
// Test 5: DisableAll keeps the map entry but suppresses delivery.
// ──────────────────────────────────────────────
bool TestDisableAllKeepsChannelInMap() {
    coropact::reactor::EventLoop loop;

    std::array<int, 2> fds{};
    if (!Expect(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds.data()) == 0,
                "socketpair creation failed")) {
        return false;
    }

    coropact::reactor::detail::Channel ch(&loop, fds[0]);
    ch.SetReadCallback(NoopRead, nullptr);
    ch.EnableReading();

    ch.DisableAll();

    const bool still_in_map = Expect(ch.IsRegistered(),
        "DisableAll should keep channel in Poller map (kDeleted state)");

    // The channel is removed from epoll, so no callback should fire.
    bool read_called = false;
    ch.SetReadCallback(MarkRead, &read_called);
    ::write(fds[1], "x", 1);

    // Use a timer to exit instead of depending on a read event.
    loop.RunAfter(0.02, [&] { loop.RequestStop(); });
    loop.Run();

    const bool not_fired = Expect(!read_called,
        "disabled channel must not receive read events");

    ch.Remove();
    ::close(fds[0]);
    ::close(fds[1]);
    return still_in_map && not_fired;
}

// ──────────────────────────────────────────────
// Test 6: Re-enabling after DisableAll registers the channel again.
// ──────────────────────────────────────────────
bool TestReenableAfterDisable() {
    coropact::reactor::EventLoop loop;

    std::array<int, 2> fds{};
    if (!Expect(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds.data()) == 0,
                "socketpair creation failed")) {
        return false;
    }

    bool read_called = false;
    coropact::reactor::detail::Channel ch(&loop, fds[0]);
    ReadAndQuitContext context{fds[0], &read_called, &loop};
    ch.SetReadCallback(DrainReadAndQuit, &context);

    ch.EnableReading();
    ch.DisableAll();
    ch.EnableReading();

    ::write(fds[1], "x", 1);
    loop.Run();

    ch.DisableAll();
    ch.Remove();
    ::close(fds[0]);
    ::close(fds[1]);
    return Expect(read_called,
        "re-enabled channel should receive read events after re-registration");
}

// ──────────────────────────────────────────────
// Test 7: The events_ vector grows when many channels fire together.
// ──────────────────────────────────────────────
bool TestEventsVectorResizes() {
    coropact::reactor::EventLoop loop;

    // Register 20 channels so the initial event list must grow.
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
    std::vector<std::unique_ptr<coropact::reactor::detail::Channel>> channels;
    channels.reserve(N);
    std::array<ReadManyContext, N> contexts{};

    for (int i = 0; i < N; ++i) {
        auto ch = std::make_unique<coropact::reactor::detail::Channel>(&loop, pairs[i][0]);
        // Consume the byte to avoid repeated delivery in level-triggered mode.
        contexts[i] = ReadManyContext{pairs[i][0], &trigger_count, N, &loop};
        ch->SetReadCallback(DrainReadMany, &contexts[i]);
        ch->EnableReading();
        channels.push_back(std::move(ch));
    }

    // Write to every peer so epoll_wait sees many ready events immediately.
    for (auto& p : pairs) {
        ::write(p[1], "x", 1);
    }

    // Backstop in case delivery is spread across multiple poll rounds.
    loop.RunAfter(2.0, [&] { loop.RequestStop(); });
    loop.Run();

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
