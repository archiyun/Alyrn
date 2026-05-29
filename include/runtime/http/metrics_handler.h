// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <string>

#include "runtime/task/scheduler_metrics.h"

namespace runtime::http {

// Serializes a SchedulerMetrics snapshot to a JSON object string.
// The returned string is a self-contained JSON value with no trailing newline.
std::string MakeMetricsJson(const runtime::task::SchedulerMetrics::Snapshot& snap);

}  // namespace runtime::http
