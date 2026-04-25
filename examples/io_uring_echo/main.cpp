/*
 * io_uring TCP Echo Server — liburing 版本
 *
 * 知识点覆盖（与 raw syscall 版本一一对应，但代码量减少 ~60%）：
 *  §1  Ring 初始化      — io_uring_queue_init_params()
 *  §2  SQE 提交         — io_uring_get_sqe() + io_uring_submit()
 *  §3  CQE 消费         — io_uring_peek_cqe() / io_uring_wait_cqe() + io_uring_cqe_seen()
 *  §4  Multishot accept — io_uring_prep_multishot_accept()
 *  §5  RECV / SEND      — io_uring_prep_recv() / io_uring_prep_send()
 *  §6  链式 SQE         — io_uring_sqe_set_flags(sqe, IOSQE_IO_LINK)
 *  §7  Provided Buffers — io_uring_prep_provide_buffers()，见【进阶说明】
 *  §8  Cancel           — io_uring_prep_cancel_fd()
 *
 * liburing vs raw syscall 对应关系：
 *   io_uring_queue_init()    → io_uring_setup() + 三段 mmap（自动完成）
 *   io_uring_get_sqe()       → GetSqe()（自动处理 SQ 满的情况）
 *   io_uring_prep_recv/send  → PrepRecv() / PrepSend()
 *   io_uring_submit()        → Flush()（写 sq_ktail + io_uring_enter）
 *   io_uring_wait_cqe()      → sys_io_uring_enter(..., IORING_ENTER_GETEVENTS)
 *   io_uring_cqe_seen()      → __atomic_store_n(cq_khead, ...)
 *
 * 内核要求：5.19+（multishot accept）
 * 依赖：liburing 2.3（headers vendored in third_party/liburing）
 *
 * 编译：
 *   cmake -B build-tests && cmake --build build-tests --target io_uring_echo -j$(nproc)
 *
 * 运行：
 *   ./build-tests/examples/io_uring_echo/io_uring_echo [port]
 *   echo "hello" | nc -q1 127.0.0.1 8080
 */

#include <linux/openat2.h>  // open_how — required before liburing.h
#include <liburing.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <csignal>
#include <memory>
#include <unordered_map>

// ─────────────────────────────── 常量 ────────────────────────────────────────

static constexpr int    kPort      = 8080;
static constexpr int    kRingDepth = 256;
static constexpr size_t kBufSize   = 4096;

// ──────────── 操作类型，编码进 user_data（liburing 原样回填到 CQE）────────────

enum Op : uint8_t { kAccept = 0, kRecv = 1, kSend = 2, kClose = 3, kCancel = 4 };

static uint64_t Encode(Op op, int fd) {
    return (static_cast<uint64_t>(op) << 56) | static_cast<uint32_t>(fd);
}
static Op  DecodeOp(uint64_t ud) { return static_cast<Op>(ud >> 56); }
static int DecodeFd(uint64_t ud) { return static_cast<int>(ud & 0xFFFF'FFFFu); }

// ──────────────────── SQE 准备函数（liburing io_uring_prep_*）────────────────

// §4 Multishot accept：一次提交，持续 accept 新连接
// 每次 accept 成功产生一个 CQE；IORING_CQE_F_MORE 为 0 时才需重投
static void PrepAccept(io_uring* ring, int listen_fd,
                       sockaddr* addr, socklen_t* addrlen) {
    io_uring_sqe* sqe = io_uring_get_sqe(ring);  // 自动处理 SQ 满（内部 flush）
    io_uring_prep_multishot_accept(sqe, listen_fd, addr, addrlen,
                                   SOCK_NONBLOCK | SOCK_CLOEXEC);
    io_uring_sqe_set_data64(sqe, Encode(kAccept, listen_fd));
}

// §5 RECV：sqe_flags 可携带 IOSQE_IO_LINK（§6）
static void PrepRecv(io_uring* ring, int fd, void* buf, unsigned len,
                     unsigned sqe_flags = 0) {
    io_uring_sqe* sqe = io_uring_get_sqe(ring);
    io_uring_prep_recv(sqe, fd, buf, len, 0);
    io_uring_sqe_set_flags(sqe, sqe_flags);
    io_uring_sqe_set_data64(sqe, Encode(kRecv, fd));
}

// §5 SEND + §6 链式标志（IOSQE_IO_LINK 使紧随其后的 SQE 成为链接目标）
static void PrepSend(io_uring* ring, int fd, const void* buf, unsigned len,
                     unsigned sqe_flags = 0) {
    io_uring_sqe* sqe = io_uring_get_sqe(ring);
    io_uring_prep_send(sqe, fd, buf, len, 0);
    io_uring_sqe_set_flags(sqe, sqe_flags);
    io_uring_sqe_set_data64(sqe, Encode(kSend, fd));
}

// §8 取消 fd 上所有挂起的操作
static void PrepCancel(io_uring* ring, int fd) {
    io_uring_sqe* sqe = io_uring_get_sqe(ring);
    io_uring_prep_cancel_fd(sqe, fd, IORING_ASYNC_CANCEL_ALL);
    io_uring_sqe_set_data64(sqe, Encode(kCancel, fd));
}

static void PrepClose(io_uring* ring, int fd) {
    io_uring_sqe* sqe = io_uring_get_sqe(ring);
    io_uring_prep_close(sqe, fd);
    io_uring_sqe_set_data64(sqe, Encode(kClose, fd));
}

// ─────────────────────────── 连接状态 ────────────────────────────────────────

struct Conn {
    char buf[kBufSize];
};

// ─────────────────────────────── 主函数 ──────────────────────────────────────

static std::atomic<bool> g_running{true};
static void SigHandler(int) { g_running.store(false, std::memory_order_relaxed); }

int main(int argc, char** argv) {
    const int port = (argc > 1) ? std::atoi(argv[1]) : kPort;

    signal(SIGINT,  SigHandler);
    signal(SIGTERM, SigHandler);
    signal(SIGPIPE, SIG_IGN);

    // ── 监听 socket ──────────────────────────────────────────────────────────
    int listen_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (listen_fd < 0) { perror("socket"); return 1; }
    const int on = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEPORT, &on, sizeof(on));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(static_cast<uint16_t>(port));
    if (bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        perror("bind"); return 1;
    }
    if (listen(listen_fd, SOMAXCONN) < 0) { perror("listen"); return 1; }

    // ── §1: 初始化 Ring ───────────────────────────────────────────────────────
    // io_uring_queue_init_params() 封装了 io_uring_setup() + 三段 mmap
    io_uring ring{};
    io_uring_params params{};
    // flags=0: 中断驱动模式。
    // 备选：IORING_SETUP_SQPOLL（内核轮询 SQ，零提交 syscall，需 root）
    if (io_uring_queue_init_params(kRingDepth, &ring, &params) < 0) {
        perror("io_uring_queue_init"); return 1;
    }
    printf("io_uring echo server  port=%d  ring_fd=%d\n", port, ring.ring_fd);

    std::unordered_map<int, std::unique_ptr<Conn>> conns;

    sockaddr_in peer_addr{};
    socklen_t   peer_addrlen = sizeof(peer_addr);

    // ── §4: 提交首个 multishot accept ────────────────────────────────────────
    PrepAccept(&ring, listen_fd,
               reinterpret_cast<sockaddr*>(&peer_addr), &peer_addrlen);
    io_uring_submit(&ring);

    // ── §3: CQE 消费主循环 ────────────────────────────────────────────────────
    while (g_running.load(std::memory_order_relaxed)) {

        // 阻塞等待至少 1 个 CQE（等价于 raw 版本的 io_uring_enter + GETEVENTS）
        io_uring_cqe* cqe = nullptr;
        const int wait_ret = io_uring_wait_cqe(&ring, &cqe);
        if (wait_ret < 0) {
            if (wait_ret == -EINTR) continue;
            fprintf(stderr, "io_uring_wait_cqe: %s\n", strerror(-wait_ret));
            break;
        }

        // 批量消费所有就绪的 CQE（io_uring_peek_cqe 不阻塞）
        do {
            const Op  op  = DecodeOp(cqe->user_data);
            const int fd  = DecodeFd(cqe->user_data);
            const int res = cqe->res;

            switch (op) {

            // §4 续: res = 新 conn fd
            case kAccept:
                if (res >= 0) {
                    auto conn = std::make_unique<Conn>();
                    PrepRecv(&ring, res, conn->buf, kBufSize);
                    conns.emplace(res, std::move(conn));
                } else {
                    fprintf(stderr, "accept: %s\n", strerror(-res));
                }
                // IORING_CQE_F_MORE 未置位 → multishot SQE 已失效，重新提交
                if (!(cqe->flags & IORING_CQE_F_MORE))
                    PrepAccept(&ring, listen_fd,
                               reinterpret_cast<sockaddr*>(&peer_addr),
                               &peer_addrlen);
                break;

            case kRecv: {
                auto it = conns.find(fd);
                if (res <= 0 || it == conns.end()) {
                    // res==0: 对端关闭；res<0: 错误或 IOSQE_IO_LINK 取消
                    if (res < 0 && res != -ECANCELED && res != -ECONNRESET)
                        fprintf(stderr, "recv fd=%d: %s\n", fd, strerror(-res));
                    conns.erase(fd);
                    break;
                }
                // §6 链式 SQE: send[IOSQE_IO_LINK] → recv
                // send 失败时 recv 收到 -ECANCELED，由上面的 res<0 分支清理连接
                PrepSend(&ring, fd, it->second->buf,
                         static_cast<unsigned>(res),
                         IOSQE_IO_LINK);       // ← 链接标志
                PrepRecv(&ring, fd, it->second->buf, kBufSize);
                break;
            }

            case kSend:
                if (res < 0 && res != -ECANCELED)
                    fprintf(stderr, "send fd=%d: %s\n", fd, strerror(-res));
                break;

            case kClose:
            case kCancel:
                break;
            }

            // §3: 标记 CQE 已消费（推进 cq_head）
            io_uring_cqe_seen(&ring, cqe);

        } while (io_uring_peek_cqe(&ring, &cqe) == 0);

        // §2: 批量提交本轮新增的 SQE
        io_uring_submit(&ring);
    }

    // ── §8: 清理 ─────────────────────────────────────────────────────────────
    for (auto& [conn_fd, _] : conns) {
        PrepCancel(&ring, conn_fd);
        PrepClose(&ring, conn_fd);
    }
    if (!conns.empty()) {
        io_uring_submit_and_wait(&ring,
                                 static_cast<unsigned>(conns.size() * 2));
    }

    io_uring_queue_exit(&ring);  // munmap + close ring_fd
    close(listen_fd);
    printf("Server shut down.\n");
    return 0;
}

/*
 * ══════════════════ §7 进阶：Provided Buffers ══════════════════════════════
 *
 * 目的：预分配缓冲区池，recv 时由内核从池中选取，避免每连接独立分配 Conn。
 *
 * ── 改动步骤 ────────────────────────────────────────────────────────────────
 *
 *  1. 声明缓冲区池：
 *       static constexpr int kBufCount   = 64;
 *       static constexpr int kBufGroupId = 1;
 *       static char pool[kBufCount][kBufSize];
 *
 *  2. Ring 初始化后注册缓冲区池（等待 CQE 确认）：
 *       io_uring_sqe* sqe = io_uring_get_sqe(&ring);
 *       io_uring_prep_provide_buffers(sqe, pool, kBufSize,
 *                                     kBufCount, kBufGroupId, 0);
 *       io_uring_sqe_set_data64(sqe, Encode(kProvide, 0));
 *       io_uring_submit_and_wait(&ring, 1);   // 等注册完成
 *
 *  3. PrepRecv 改为让内核自动选 buffer：
 *       io_uring_prep_recv(sqe, fd, nullptr, kBufSize, 0);
 *       io_uring_sqe_set_flags(sqe, IOSQE_BUFFER_SELECT);
 *       sqe->buf_group = kBufGroupId;
 *       // 不再需要 Conn 结构体
 *
 *  4. kRecv CQE 中取出 bid 和数据：
 *       int   bid  = cqe->flags >> IORING_CQE_BUFFER_SHIFT;  // = 16
 *       char* data = pool[bid];
 *       int   len  = res;
 *
 *  5. Send 完成后归还 buffer（kSend 分支）：
 *       io_uring_sqe* sqe = io_uring_get_sqe(&ring);
 *       io_uring_prep_provide_buffers(sqe, pool[bid], kBufSize,
 *                                     1, kBufGroupId, bid);
 *
 * ── 注意事项 ────────────────────────────────────────────────────────────────
 *
 *   IOSQE_BUFFER_SELECT 与 IOSQE_IO_LINK 不可同时用于 echo 的 recv SQE：
 *   send 的 buffer 地址只有 recv CQE 返回后才知道，链式提交时地址未知。
 *   替代方案：recv CQE 到达后再分别提交 send（已知 addr）和下一次 recv，不链式。
 *
 *   更高性能变体：io_uring_register_buf_ring()（Linux 5.19+）
 *   通过共享 buffer ring 避免每次 provide/return 都消耗 SQE。
 * ═══════════════════════════════════════════════════════════════════════════
 */
