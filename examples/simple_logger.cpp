#include "runtime/log/logger.h"

int main() {
    // Demonstrate asynchronous streaming log output.
    auto& logger = runtime::log::Logger::Instance();

    logger.Init("simple_logger.log", runtime::log::LogLevel::DEBUG);

    LOG_DEBUG() << "debug message, value=" << 7;
    LOG_INFO() << "server started on port " << 8080;
    LOG_ERROR() << "request failed: " << "timeout";

    logger.SetLogLevel(runtime::log::LogLevel::ERROR);
    LOG_INFO() << "this info message should be filtered";
    LOG_FATAL() << "fatal message for shutdown path";

    logger.Shutdown();

    return 0;
}
