#pragma once

#include "runtime/inference/inference_request.h"
#include "runtime/task/blocking_queue.h"
#include <thread>
#include <string>

struct llama_model;
struct llama_context;
struct llama_sampler;

namespace runtime::inference {

static constexpr int DEFAULT_CONTEXT_LENGTH{4096};
static constexpr int DEFAULT_LLAMA_BATCH_LENGTH{512};

class LlamaEngine {
public:
    struct Config {
        std::string model_path;
        int n_ctx_{DEFAULT_CONTEXT_LENGTH};
        int n_gpu_layers_ = 99;
        int n_bacth_{DEFAULT_LLAMA_BATCH_LENGTH};
    };

    explicit LlamaEngine(Config cfg);
    ~LlamaEngine();

    void Submit(InferenceRequest req);

    void Start();
    void Stop();

private:
    void RunInferenceLoop();
    void ProcessOne(InferenceRequest &req);

    Config config_;
    llama_model *model{nullptr};
    llama_context *ctx_{nullptr};
    llama_sampler *sampler_{nullptr};

    runtime::task::BlockingQueue<InferenceRequest> queue_;
    std::jthread inference_thread_;
    std::atomic<bool> running_{false};
};
}  // namespace runtime::inference
