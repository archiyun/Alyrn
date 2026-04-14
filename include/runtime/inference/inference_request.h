#pragma once

#include "runtime/net/tcp_connection.h"
#include "runtime/net/event_loop.h"
#include <functional>
#include <memory>
#include <string>
#include <atomic>
#include <string_view>

namespace runtime::inference {

struct InferenceRequest {
    // 请求参数
    std::string prompt_;
    float temperature_{0.8f};
    float top_p_{0.95f};
    int max_new_tokens_ = 512;
    bool stream_ = true;

    // 流式回调
    // token_cb : 每生成一次token调用一次
    // done_cb : 生成结束<void(std::string_view token));
    using TokenCallback = std::function<void(std::string_view token)>;
    using DoneCallback = std::function<void(std::string_view finish_reason)>;

    TokenCallback token_cb_;
    DoneCallback done_cb_;

    std::shared_ptr<std::atomic<bool>> cancelled = std::make_shared<std::atomic<bool>>(false);

    runtime::net::EventLoop *io_loop_{nullptr};
}; 

} // namespace runtime::inference