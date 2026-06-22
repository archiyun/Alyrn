// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <string>

#include "vexo/task/executor_metrics.h"

namespace vexo::http {

// Serializes an ExecutorMetrics snapshot to a JSON object string.
// The returned string is a self-contained JSON value with no trailing newline.
std::string MakeMetricsJson(const vexo::task::ExecutorMetrics::Snapshot& snap);

}  // namespace vexo::http
