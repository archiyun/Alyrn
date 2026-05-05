#pragma once

#include "runtime/task/scheduler_metrics.h"

#include <string>

namespace runtime::http {

// Serializes a SchedulerMetrics snapshot to a JSON object string.
// The returned string is a self-contained JSON value with no trailing newline.
std::string MakeMetricsJson(const runtime::task::SchedulerMetrics::Snapshot& snap);

}  // namespace runtime::http
