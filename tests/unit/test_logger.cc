#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include "vexo/log/async_logger.h"
#include "vexo/log/log_buffer.h"
#include "vexo/log/logger.h"

namespace vexo::log {
namespace {

std::string ReadFile(const std::filesystem::path& path) {
    std::ifstream input(path);
    return std::string((std::istreambuf_iterator<char>(input)),
                       std::istreambuf_iterator<char>());
}

std::filesystem::path UniqueLogPath(const std::string& test_name) {
    const auto timestamp =
        std::chrono::steady_clock::now().time_since_epoch().count();
    return std::filesystem::temp_directory_path() /
           ("runtime-" + test_name + "-" + std::to_string(timestamp) + ".log");
}

TEST(LogBufferTest, AppendAndReset) {
    LogBuffer<16> buffer;

    EXPECT_TRUE(buffer.Empty());
    EXPECT_EQ(buffer.size(), 0U);
    EXPECT_EQ(buffer.capacity(), 16U);

    EXPECT_TRUE(buffer.Append("hello", 5));
    EXPECT_EQ(buffer.size(), 5U);
    EXPECT_EQ(std::string(buffer.data(), buffer.size()), "hello");

    EXPECT_TRUE(buffer.Append("!", 1));
    EXPECT_EQ(buffer.Avail(), 10U);

    EXPECT_FALSE(buffer.Append("0123456789abc", 13));
    EXPECT_EQ(buffer.size(), 6U);

    buffer.Reset();
    EXPECT_TRUE(buffer.Empty());
    EXPECT_EQ(buffer.size(), 0U);
}

TEST(LogFormatterTest, IncludesExpectedMetadata) {
    const auto log_path = UniqueLogPath("formatter");
    auto& logger = Logger::Instance();
    logger.Init(log_path.string(), LogLevel::DEBUG, 10);
    logger.Log(LogLevel::WARN, "logger_test.cc", 42, "Run", "hello formatter");
    logger.Shutdown();

    const std::string output = ReadFile(log_path);
    EXPECT_NE(output.find("[WARN]"), std::string::npos);
    EXPECT_NE(output.find("[tid:"), std::string::npos);
    EXPECT_NE(output.find("[logger_test.cc:42]"), std::string::npos);
    EXPECT_NE(output.find("[Run]"), std::string::npos);
    EXPECT_NE(output.find("hello formatter"), std::string::npos);
    ASSERT_FALSE(output.empty());
    EXPECT_EQ(output.back(), '\n');

    std::filesystem::remove(log_path);
}

TEST(LoggerIntegrationTest, FiltersByLogLevelAndFlushesOnShutdown) {
    const auto log_path = UniqueLogPath("filter");
    auto& logger = Logger::Instance();

    logger.Init(log_path.string(), LogLevel::WARN, 10);
    logger.Log(LogLevel::INFO, "test_logger.cc", 100, "Case",
               "filtered message");
    logger.Log(LogLevel::ERROR, "test_logger.cc", 101, "Case",
               "persisted error");
    logger.Shutdown();

    const std::string output = ReadFile(log_path);
    EXPECT_EQ(output.find("filtered message"), std::string::npos);
    EXPECT_NE(output.find("[ERROR]"), std::string::npos);
    EXPECT_NE(output.find("persisted error"), std::string::npos);

    std::filesystem::remove(log_path);
}

TEST(LoggerIntegrationTest, StreamingMacroWritesExpectedContent) {
    const auto log_path = UniqueLogPath("stream");
    auto& logger = Logger::Instance();

    logger.Init(log_path.string(), LogLevel::DEBUG, 10);
    LOG_INFO() << "streamed value=" << 7 << " name=" << "runtime";
    logger.Shutdown();

    const std::string output = ReadFile(log_path);
    EXPECT_NE(output.find("[INFO]"), std::string::npos);
    EXPECT_NE(output.find("streamed value=7 name=runtime"), std::string::npos);

    std::filesystem::remove(log_path);
}

TEST(LoggerIntegrationTest, Cpp23FormatMacroWritesExpectedContent) {
    const auto log_path = UniqueLogPath("format-macro");
    auto& logger = Logger::Instance();

    logger.Init(log_path.string(), LogLevel::DEBUG, 10);
    LOG_INFOF("formatted fd={} peer={}", 7, "127.0.0.1");
    logger.Shutdown();

    const std::string output = ReadFile(log_path);
    EXPECT_NE(output.find("formatted fd=7 peer=127.0.0.1"), std::string::npos);

    std::filesystem::remove(log_path);
}

TEST(LoggerIntegrationTest, SupportsLargeMessageTruncationMarker) {
    const auto log_path = UniqueLogPath("truncate");
    auto& logger = Logger::Instance();
    const std::string large_message(AsyncLogger::kBufferSize + 256, 'x');

    logger.Init(log_path.string(), LogLevel::DEBUG, 10);
    logger.Log(LogLevel::ERROR, "test_logger.cc", 200, "Large",
               large_message);
    logger.Shutdown();

    const std::string output = ReadFile(log_path);
    EXPECT_NE(output.find("[ERROR]"), std::string::npos);
    EXPECT_NE(output.find("[truncated]"), std::string::npos);

    std::filesystem::remove(log_path);
}

TEST(LoggerIntegrationTest, ConcurrentProducersFlushOnShutdown) {
    const auto log_path = UniqueLogPath("concurrent");
    auto& logger = Logger::Instance();
    constexpr int kThreads = 8;
    constexpr int kMessagesPerThread = 250;

    logger.Init(log_path.string(), LogLevel::INFO, 10);
    std::vector<std::thread> producers;
    producers.reserve(kThreads);
    for (int thread_index = 0; thread_index < kThreads; ++thread_index) {
        producers.emplace_back([&, thread_index] {
            for (int message_index = 0; message_index < kMessagesPerThread;
                 ++message_index) {
                logger.Log(LogLevel::INFO, "concurrent_test.cc", 300,
                           "Producer", "concurrent message");
                LOG_INFOF("producer={} message={}", thread_index, message_index);
            }
        });
    }
    for (auto& producer : producers) {
        producer.join();
    }
    logger.Shutdown();

    const std::string output = ReadFile(log_path);
    EXPECT_NE(output.find("concurrent message"), std::string::npos);
    EXPECT_NE(output.find("producer=7 message=249"), std::string::npos);

    std::filesystem::remove(log_path);
}

}  // namespace
}  // namespace vexo::log
