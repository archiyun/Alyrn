#include "runtime/http/debug_handler.h"

#include "runtime/task/task_options.h"
#include "runtime/task/task_state.h"

#include <cstdio>

namespace runtime::http {

namespace {

const char* PriorityStr(runtime::task::TaskPriority p) {
  switch (p) {
    case runtime::task::TaskPriority::kLow:    return "low";
    case runtime::task::TaskPriority::kNormal: return "normal";
    case runtime::task::TaskPriority::kHigh:   return "high";
  }
  return "unknown";
}

const char* StateStr(runtime::task::TaskState s) {
  switch (s) {
    case runtime::task::TaskState::kPending:   return "pending";
    case runtime::task::TaskState::kRunning:   return "running";
    case runtime::task::TaskState::kCompleted: return "completed";
    case runtime::task::TaskState::kFailed:    return "failed";
    case runtime::task::TaskState::kCancelled: return "cancelled";
    case runtime::task::TaskState::kTimeout:   return "timeout";
  }
  return "unknown";
}

// Escapes backslash and double-quote so names are safe inside JSON strings.
std::string JsonEscape(const std::string& s) {
  std::string out;
  out.reserve(s.size());
  for (char c : s) {
    if (c == '\\' || c == '"') out += '\\';
    out += c;
  }
  return out;
}

std::string RecordToJson(const runtime::task::TaskRecord& r) {
  char buf[256];
  std::snprintf(buf, sizeof(buf),
      "{\"id\":%llu,\"priority\":\"%s\",\"state\":\"%s\","
      "\"created_at\":\"%s\",\"enqueued_at\":\"%s\","
      "\"started_at\":\"%s\",\"completed_at\":\"%s\"",
      static_cast<unsigned long long>(r.id),
      PriorityStr(r.priority),
      StateStr(r.state),
      r.created_at.ToString().c_str(),
      r.enqueued_at.ToString().c_str(),
      r.started_at.ToString().c_str(),
      r.completed_at.ToString().c_str());

  std::string result = buf;
  result += ",\"name\":\"";
  result += JsonEscape(r.name);
  result += "\"}";
  return result;
}

}  // namespace

std::string MakeDebugTasksJson(
    const std::vector<runtime::task::TaskRecord>& records,
    std::size_t capacity) {
  char header[128];
  std::snprintf(header, sizeof(header),
      "{\"capacity\":%zu,\"count\":%zu,\"tasks\":[",
      capacity, records.size());

  std::string out = header;
  for (std::size_t i = 0; i < records.size(); ++i) {
    if (i > 0) out += ',';
    out += RecordToJson(records[i]);
  }
  out += "]}";
  return out;
}

}  // namespace runtime::http
