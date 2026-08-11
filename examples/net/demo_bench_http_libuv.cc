// SPDX-License-Identifier: MIT

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <uv.h>

#include "bench_http_common.h"

namespace {

std::atomic_bool g_stop{false};
void OnSignal(int) noexcept { g_stop.store(true, std::memory_order_relaxed); }

#if defined(COROPACT_LIBUV_HAS_TCP_REUSEPORT)
constexpr int kLibuvBindFlags = UV_TCP_REUSEPORT;
#else
constexpr int kLibuvBindFlags = 0;
#endif

struct Worker;
struct Connection {
  uv_tcp_t tcp{};
  uv_write_t write{};
  Worker* worker{nullptr};
  std::array<char, coropact_bench::kRequestBufferSize> request{};
  std::size_t used{0};
  bool writing{false};
  bool closing{false};
};

struct Worker {
  uv_loop_t loop{};
  uv_tcp_t listener{};
  uv_async_t stop_async{};
  std::size_t index{0};
  std::uint16_t port{0};
  bool listener_initialized{false};
  bool stop_initialized{false};
};

void CloseConnection(Connection* connection) {
  if (connection->closing) return;
  connection->closing = true;
  uv_read_stop(reinterpret_cast<uv_stream_t*>(&connection->tcp));
  uv_close(reinterpret_cast<uv_handle_t*>(&connection->tcp),
           [](uv_handle_t* handle) { delete static_cast<Connection*>(handle->data); });
}

void AllocBuffer(uv_handle_t* handle, std::size_t, uv_buf_t* buffer) {
  auto* connection = static_cast<Connection*>(handle->data);
  buffer->base = connection->request.data() + connection->used;
  buffer->len = connection->request.size() - connection->used;
}

void StartReading(Connection* connection);

void OnWrite(uv_write_t* request, int status) {
  auto* connection = static_cast<Connection*>(request->data);
  if (status < 0) {
    CloseConnection(connection);
    return;
  }
  connection->used = 0;
  connection->writing = false;
  StartReading(connection);
}

void OnRead(uv_stream_t* stream, ssize_t nread, const uv_buf_t*) {
  auto* connection = static_cast<Connection*>(stream->data);
  if (nread < 0 || nread == 0) {
    if (nread < 0 && (nread == UV_EOF || nread == UV_ECONNRESET)) CloseConnection(connection);
    return;
  }
  connection->used += static_cast<std::size_t>(nread);
  if (!coropact_bench::HasHeaderTerminator(connection->request.data(), connection->used)) {
    if (connection->used == connection->request.size()) CloseConnection(connection);
    return;
  }

  connection->writing = true;
  uv_read_stop(stream);
  connection->write.data = connection;
  const auto& response = coropact_bench::Response();
  uv_buf_t buffer = uv_buf_init(const_cast<char*>(response.data()), response.size());
  if (uv_write(&connection->write, stream, &buffer, 1, OnWrite) < 0) {
    CloseConnection(connection);
  }
}

void StartReading(Connection* connection) {
  if (connection->closing || connection->writing) return;
  if (connection->used == connection->request.size()) {
    CloseConnection(connection);
    return;
  }
  if (uv_read_start(reinterpret_cast<uv_stream_t*>(&connection->tcp), AllocBuffer, OnRead) < 0) {
    CloseConnection(connection);
  }
}

void OnConnection(uv_stream_t* server, int status) {
  if (status < 0) return;
  auto* worker = static_cast<Worker*>(server->data);
  auto* connection = new Connection;
  connection->worker = worker;
  if (uv_tcp_init(&worker->loop, &connection->tcp) < 0) {
    delete connection;
    return;
  }
  connection->tcp.data = connection;
  if (uv_accept(server, reinterpret_cast<uv_stream_t*>(&connection->tcp)) < 0) {
    CloseConnection(connection);
    return;
  }
  int one = 1;
  uv_os_fd_t fd = -1;
  if (uv_fileno(reinterpret_cast<const uv_handle_t*>(&connection->tcp), &fd) == 0) {
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
  }
  StartReading(connection);
}

void OnStop(uv_async_t* async) {
  auto* worker = static_cast<Worker*>(async->data);
  uv_walk(&worker->loop,
          [](uv_handle_t* handle, void* arg) {
            auto* stop_async = static_cast<uv_async_t*>(arg);
            if (handle != reinterpret_cast<uv_handle_t*>(stop_async) &&
                !uv_is_closing(handle)) {
              uv_close(handle, nullptr);
            }
          },
          &worker->stop_async);
  uv_stop(&worker->loop);
}

void RunWorker(Worker* worker) {
  if (uv_loop_init(&worker->loop) < 0) return;
  if (uv_tcp_init(&worker->loop, &worker->listener) < 0) return;
  worker->listener_initialized = true;
  worker->listener.data = worker;
  sockaddr_in address{};
  uv_ip4_addr("127.0.0.1", worker->port, &address);
  if (uv_tcp_bind(&worker->listener, reinterpret_cast<const sockaddr*>(&address),
                  kLibuvBindFlags) < 0 ||
      uv_listen(reinterpret_cast<uv_stream_t*>(&worker->listener), 4096, OnConnection) < 0) {
    uv_close(reinterpret_cast<uv_handle_t*>(&worker->listener), nullptr);
    uv_run(&worker->loop, UV_RUN_DEFAULT);
    return;
  }
  if (uv_async_init(&worker->loop, &worker->stop_async, OnStop) < 0) return;
  worker->stop_initialized = true;
  worker->stop_async.data = worker;
  uv_run(&worker->loop, UV_RUN_DEFAULT);
  if (worker->stop_initialized && !uv_is_closing(reinterpret_cast<uv_handle_t*>(&worker->stop_async))) {
    uv_close(reinterpret_cast<uv_handle_t*>(&worker->stop_async), nullptr);
  }
  uv_run(&worker->loop, UV_RUN_DEFAULT);
  uv_loop_close(&worker->loop);
}

}  // namespace

int main() {
  std::signal(SIGPIPE, SIG_IGN);
  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);
  const auto port = static_cast<std::uint16_t>(coropact_bench::EnvInt("PORT", 19090));
  const std::size_t requested_workers = coropact_bench::EnvSize("LIBUV_WORKERS", 4);
  if (port == 0 || requested_workers == 0) return 2;
#if defined(COROPACT_LIBUV_HAS_TCP_REUSEPORT)
  const std::size_t workers = requested_workers;
#else
  const std::size_t workers = 1;
#endif

  std::vector<Worker> worker_state(workers);
  std::vector<std::thread> worker_threads;
  worker_threads.reserve(workers);
  for (std::size_t i = 0; i < workers; ++i) {
    worker_state[i].index = i;
    worker_state[i].port = port;
    worker_threads.emplace_back(RunWorker, &worker_state[i]);
  }
  std::printf("HttpLibuvBench bind=127.0.0.1 port=%u workers=%zu response_body=%zu\n", port,
              workers, coropact_bench::kResponseBodySize);
  std::fflush(stdout);
  while (!g_stop.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  for (auto& worker : worker_state) {
    if (worker.stop_initialized) uv_async_send(&worker.stop_async);
  }
  for (auto& thread : worker_threads) thread.join();
  return 0;
}
