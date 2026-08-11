// SPDX-License-Identifier: MIT

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include <event2/bufferevent.h>
#include <event2/buffer.h>
#include <event2/listener.h>
#include <event2/thread.h>

#include "bench_http_common.h"

namespace {

std::atomic_bool g_stop{false};
void OnSignal(int) noexcept { g_stop.store(true, std::memory_order_relaxed); }

struct Worker;
struct Connection {
  bufferevent* buffer{nullptr};
  std::string request;
  bool writing{false};
};

struct Worker {
  event_base* base{nullptr};
  evconnlistener* listener{nullptr};
  std::size_t index{0};
  std::uint16_t port{0};
};

void OnEvent(bufferevent* buffer, short events, void* arg) {
  auto* connection = static_cast<Connection*>(arg);
  if ((events & (BEV_EVENT_EOF | BEV_EVENT_ERROR | BEV_EVENT_TIMEOUT)) != 0) {
    delete connection;
    bufferevent_free(buffer);
  }
}

void OnWrite(bufferevent* buffer, void* arg) {
  auto* connection = static_cast<Connection*>(arg);
  if (connection->writing && evbuffer_get_length(bufferevent_get_output(buffer)) == 0) {
    connection->writing = false;
    connection->request.clear();
    bufferevent_enable(buffer, EV_READ);
  }
}

void OnRead(bufferevent* buffer, void* arg) {
  auto* connection = static_cast<Connection*>(arg);
  auto* input = bufferevent_get_input(buffer);
  char bytes[8192];
  while (evbuffer_get_length(input) != 0) {
    const int read = evbuffer_remove(input, bytes, sizeof(bytes));
    if (read <= 0) break;
    connection->request.append(bytes, static_cast<std::size_t>(read));
    if (connection->request.size() > coropact_bench::kRequestBufferSize) {
      bufferevent_free(buffer);
      delete connection;
      return;
    }
  }
  if (!connection->writing &&
      coropact_bench::HasHeaderTerminator(connection->request.data(), connection->request.size())) {
    connection->writing = true;
    bufferevent_disable(buffer, EV_READ);
    const auto& response = coropact_bench::Response();
    bufferevent_write(buffer, response.data(), response.size());
  }
}

void OnAccept(evconnlistener*, evutil_socket_t fd, sockaddr*, int, void* arg) {
  auto* worker = static_cast<Worker*>(arg);
  int one = 1;
  ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
  auto* connection = new Connection;
  connection->buffer = bufferevent_socket_new(worker->base, fd, BEV_OPT_CLOSE_ON_FREE);
  if (connection->buffer == nullptr) {
    ::close(fd);
    delete connection;
    return;
  }
  bufferevent_setcb(connection->buffer, OnRead, OnWrite, OnEvent, connection);
  bufferevent_enable(connection->buffer, EV_READ | EV_WRITE);
}

void RunWorker(Worker* worker) {
  worker->base = event_base_new();
  if (worker->base == nullptr) return;
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(worker->port);
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  worker->listener = evconnlistener_new_bind(
      worker->base, OnAccept, worker,
      LEV_OPT_REUSEABLE | LEV_OPT_REUSEABLE_PORT | LEV_OPT_CLOSE_ON_FREE, 4096,
      reinterpret_cast<sockaddr*>(&address), sizeof(address));
  if (worker->listener != nullptr) event_base_dispatch(worker->base);
  if (worker->listener != nullptr) evconnlistener_free(worker->listener);
  event_base_free(worker->base);
  worker->listener = nullptr;
  worker->base = nullptr;
}

}  // namespace

int main() {
  std::signal(SIGPIPE, SIG_IGN);
  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);
  evthread_use_pthreads();
  const auto port = static_cast<std::uint16_t>(coropact_bench::EnvInt("PORT", 19090));
  const std::size_t workers = coropact_bench::EnvSize("LIBEVENT_WORKERS", 4);
  if (port == 0 || workers == 0) return 2;

  std::vector<Worker> worker_state(workers);
  std::vector<std::thread> worker_threads;
  worker_threads.reserve(workers);
  for (std::size_t i = 0; i < workers; ++i) {
    worker_state[i].index = i;
    worker_state[i].port = port;
    worker_threads.emplace_back(RunWorker, &worker_state[i]);
  }
  std::printf("HttpLibeventBench bind=127.0.0.1 port=%u workers=%zu response_body=%zu\n", port,
              workers, coropact_bench::kResponseBodySize);
  std::fflush(stdout);
  while (!g_stop.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  for (auto& worker : worker_state) {
    if (worker.base != nullptr) event_base_loopbreak(worker.base);
  }
  for (auto& thread : worker_threads) thread.join();
  return 0;
}
