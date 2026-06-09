// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <string>

#include "runtime/task/executor_metrics.h"

namespace runtime::http {

// Serializes an ExecutorMetrics snapshot to a JSON object string.
// The returned string is a self-contained JSON value with no trailing newline.
std::string MakeMetricsJson(const runtime::task::ExecutorMetrics::Snapshot& snap);

}  // namespace runtime::http
