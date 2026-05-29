/**
 * // demo_logger.cpp
 * 展示 
 * stdout 快速验证
 * Level 过滤 
 * 文件输出 + 多类型流式写入
 * 运行时换级别
 */

#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

#include "runtime/log/logger.h"

// Helper: read a log file and print it to stdout so the demo is self-contained.
static void PrintFile(const std::string& path) {
    std::ifstream f(path);
    std::cout << f.rdbuf();
    std::filesystem::remove(path);
}

// ── Section 1: stdout output ─────────────────────────────────────────────────
// Pass an empty filename to write directly to stdout.
// Expected terminal output: DEBUG, INFO, WARN lines. ERROR and FATAL are absent
// because we set the threshold to WARN after the first two writes.
static void DemoStdout() {
    std::cout << "\n=== Section 1: stdout, threshold=DEBUG then raised to WARN ===\n";

    auto& logger = runtime::log::Logger::Instance();
    logger.Init("", runtime::log::LogLevel::DEBUG);

    LOG_DEBUG() << "startup probe, pid=" << ::getpid();
    LOG_INFO()  << "listening on port " << 8080;
    LOG_WARN()  << "connection pool at " << 80 << "% capacity";

    // Raise the threshold at runtime — INFO and DEBUG are now silently dropped.
    logger.SetLogLevel(runtime::log::LogLevel::ERROR);
    LOG_INFO()  << "this line is filtered and produces no output";
    LOG_ERROR() << "disk usage critical: " << 95 << "%";

    logger.Shutdown();
}

// ── Section 2: file output + multi-type streaming ────────────────────────────
// Init with a filename. Shutdown flushes the async buffer before returning.
// After Shutdown the file is guaranteed to contain every record written above.
static void DemoFileOutput() {
    std::cout << "\n=== Section 2: file output (runtime-demo.log) ===\n";

    const std::string path = "runtime-demo.log";
    auto& logger = runtime::log::Logger::Instance();
    logger.Init(path, runtime::log::LogLevel::INFO, /*flush_ms=*/200);

    LOG_INFO()  << "request id=" << 42 << " method=GET path=/api/users";
    LOG_WARN()  << "latency " << 312 << "ms exceeds threshold " << 200 << "ms";
    LOG_ERROR() << "upstream timeout after " << 3 << " retries";

    // DEBUG is below the INFO threshold — this line is dropped.
    LOG_DEBUG() << "internal state: queue_depth=7 (should not appear in file)";

    logger.Shutdown();  // blocks until async thread flushes

    std::cout << "--- file contents ---\n";
    PrintFile(path);
}

// ── Section 3: streaming operator accepts any <<-compatible type ──────────────
static void DemoStreamingTypes() {
    std::cout << "\n=== Section 3: streaming mixed types ===\n";

    auto& logger = runtime::log::Logger::Instance();
    logger.Init("", runtime::log::LogLevel::DEBUG);

    const double ratio = 0.9375;
    LOG_INFO() << "cache hit ratio=" << ratio
               << " hits=" << 15 << " total=" << 16;

    const bool is_primary = true;
    LOG_WARN() << "node is_primary=" << is_primary << " role=leader";

    logger.Shutdown();
}

int main() {
    DemoStdout();
    DemoFileOutput();
    DemoStreamingTypes();
    return 0;
}
