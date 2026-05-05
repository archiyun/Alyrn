#include "runtime/http/metrics_handler.h"

#include <cstdio>

namespace runtime::http {

std::string MakeMetricsJson(const runtime::task::SchedulerMetrics::Snapshot& snap) {
  char buf[256];
  std::snprintf(buf, sizeof(buf),
    "{"
      "\"submitted\":%llu,"
      "\"completed\":%llu,"
      "\"failed\":%llu,"
      "\"cancelled\":%llu,"
      "\"timeout\":%llu,"
      "\"queue_size\":%d,"
      "\"running_count\":%d"
    "}",
    static_cast<unsigned long long>(snap.submitted),
    static_cast<unsigned long long>(snap.completed),
    static_cast<unsigned long long>(snap.failed),
    static_cast<unsigned long long>(snap.cancelled),
    static_cast<unsigned long long>(snap.timeout),
    snap.queue_size,
    snap.running_count);
  return buf;
}

}  // namespace runtime::http
