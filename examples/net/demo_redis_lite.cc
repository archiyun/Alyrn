// demo_redis_lite.cc — 用 vexo_net 实现的极简 Redis 服务器
// 环境变量：
//   PORT=6379  IO_THREADS=1
//
// 编译：
//   cmake --build build-tests --target demo_redis_lite -j$(nproc)
//
// 跑：
//   PORT=6379 ./build-tests/examples/demo_redis_lite
// 另起终端用官方客户端连（最直观）：
//   redis-cli -p 6379 set foo bar      # +OK
//   redis-cli -p 6379 get foo          # "bar"
//   redis-cli -p 6379 del foo          # (integer) 1
//   redis-cli -p 6379 get foo          # (nil)
// 没有 redis-cli 也可以手敲 RESP：
//   printf '*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n' | nc 127.0.0.1 6379

#include <cctype>
#include <charconv>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "vexo/log/logger.h"
#include "vexo/net/buffer.h"
#include "vexo/net/event_loop.h"
#include "vexo/net/inet_address.h"
#include "vexo/net/tcp_connection.h"
#include "vexo/net/tcp_server.h"
#include "vexo/time/timestamp.h"

using TcpConnectionPtr = std::shared_ptr<vexo::net::TcpConnection>;

static std::mutex g_db_mutex;
static std::unordered_map<std::string, std::string> g_db;

// RESP协议
static void SendSimpleString(const TcpConnectionPtr& conn, std::string_view s) {
  std::string resp;
  resp.reserve(s.size() + 3);
  resp += '+';
  resp += s;
  resp += "\r\n";
  conn->Send(resp);
}

static void SendError(const TcpConnectionPtr& conn, std::string_view msg) {
  std::string resp = "-ERR ";
  resp += msg;
  resp += "\r\n";
  conn->Send(resp);
}

static void SendInteger(const TcpConnectionPtr& conn, long long n) {
  conn->Send(":" + std::to_string(n) + "\r\n");
}

static void SendBulkString(const TcpConnectionPtr& conn, std::string_view s) {
  std::string resp = "$" + std::to_string(s.size()) + "\r\n";
  resp += s;
  resp += "\r\n";
  conn->Send(resp);
}

static void SendNullBulk(const TcpConnectionPtr& conn) { conn->Send(std::string_view{"$-1\r\n"}); }

// 命令解析
static void HandleCommand(const TcpConnectionPtr& conn, const std::vector<std::string>& args) {
  if (args.empty()) return;

  std::string cmd = args[0];
  for (char& c : cmd) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));

  if (cmd == "SET") {
    if (args.size() < 3) {
      SendError(conn, "wrong number of arguments for 'set' command");
      return;
    }
    {
      std::lock_guard<std::mutex> lk(g_db_mutex);
      g_db[args[1]] = args[2];
    }
    SendSimpleString(conn, "OK");
  } else if (cmd == "GET") {
    if (args.size() < 2) {
      SendError(conn, "wrong number of arguments for 'get' command");
      return;
    }
    std::optional<std::string> val;
    {
      std::lock_guard<std::mutex> lk(g_db_mutex);
      auto it = g_db.find(args[1]);
      if (it != g_db.end()) val = it->second;
    }
    if (val)
      SendBulkString(conn, *val);
    else
      SendNullBulk(conn);
  } else if (cmd == "DEL") {
    if (args.size() < 2) {
      SendError(conn, "wrong number of arguments for 'del' command");
      return;
    }
    long long removed = 0;
    {
      std::lock_guard<std::mutex> lk(g_db_mutex);
      for (std::size_t i = 1; i < args.size(); ++i)
        removed += static_cast<long long>(g_db.erase(args[i]));
    }
    SendInteger(conn, removed);
  } else if (cmd == "PING") {
    if (args.size() >= 2)
      SendBulkString(conn, args[1]);
    else
      SendSimpleString(conn, "PONG");
  } else if (cmd == "COMMAND") {
    conn->Send(std::string_view{"*0\r\n"});
  } else {
    SendError(conn, "unknown command '" + args[0] + "'");
  }
}

static bool TryParseCommand(const char* p, std::size_t n, std::vector<std::string>* out,
                            std::size_t* consumed, bool* protocol_error) {
  *protocol_error = false;
  std::size_t i = 0;

  auto read_line = [&](std::string_view* line) -> bool {
    const void* crlf = ::memmem(p + i, n - i, "\r\n", 2);
    if (!crlf) return false;
    const char* nl = static_cast<const char*>(crlf);
    *line = std::string_view(p + i, static_cast<std::size_t>(nl - (p + i)));
    i = static_cast<std::size_t>(nl - p) + 2;
    return true;
  };

  auto to_int = [](std::string_view sv, long long* v) -> bool {
    auto res = std::from_chars(sv.data(), sv.data() + sv.size(), *v);
    return res.ec == std::errc{} && res.ptr == sv.data() + sv.size();
  };

  if (n == 0) return false;
  if (p[0] != '*') {
    *protocol_error = true;
    return false;
  }

  std::string_view header;
  if (!read_line(&header)) return false;

  long long argc = 0;
  if (header.size() < 2 || !to_int(header.substr(1), &argc) || argc < 0) {
    *protocol_error = true;
    return false;
  }

  std::vector<std::string> args;
  args.reserve(static_cast<std::size_t>(argc));
  for (long long k = 0; k < argc; ++k) {
    std::string_view len_line;
    if (!read_line(&len_line)) return false;
    if (len_line.empty() || len_line[0] != '$') {
      *protocol_error = true;
      return false;
    }
    long long arg_len = 0;
    if (!to_int(len_line.substr(1), &arg_len) || arg_len < 0) {
      *protocol_error = true;
      return false;
    }

    if (n - i < static_cast<std::size_t>(arg_len) + 2) return false;
    args.emplace_back(p + i, static_cast<std::size_t>(arg_len));
    i += static_cast<std::size_t>(arg_len) + 2;
  }

  *out = std::move(args);
  *consumed = i;
  return true;
}

static void OnMessage(const TcpConnectionPtr& conn, vexo::net::Buffer& buf,
                      vexo::time::Timestamp) {
  while (buf.readable_bytes() > 0) {
    std::vector<std::string> args;
    std::size_t consumed = 0;
    bool protocol_error = false;

    bool ok = TryParseCommand(buf.Peek(), buf.readable_bytes(), &args, &consumed, &protocol_error);
    if (!ok) {
      if (protocol_error) {
        SendError(conn, "Protocol error");
        conn->Shutdown();
      }
      return;
    }

    buf.Retrieve(consumed);
    HandleCommand(conn, args);
  }
}

int main() {
  auto env_int = [](const char* k, int def) -> int {
    const char* v = std::getenv(k);
    return v ? std::atoi(v) : def;
  };
  const int io_threads = env_int("IO_THREADS", 1);
  const uint16_t port = static_cast<uint16_t>(env_int("PORT", 6379));

  std::signal(SIGPIPE, SIG_IGN);

  vexo::log::Logger::Instance().Init("redis_lite", vexo::log::LogLevel::INFO,
                                        /*flush_interval_ms=*/1000,
                                        /*roll_size=*/10 * 1024 * 1024);

  vexo::net::EventLoop main_loop;
  vexo::net::TcpServer server(&main_loop, vexo::net::InetAddress(port), "RedisLite");

  server.set_thread_num(io_threads);
  server.set_message_callback(OnMessage);

  server.Start();
  std::printf("RedisLite listening on port %u  io_threads=%d  (logs: tail -f redis_lite.log)\n",
              port, io_threads);
  std::fflush(stdout);
  main_loop.Loop();
  return 0;
}
