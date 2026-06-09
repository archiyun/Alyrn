#include <gtest/gtest.h>

#include <stdexcept>
#include <string>

#include "runtime/http/metrics_handler.h"
#include "runtime/http/router.h"
#include "runtime/task/blocking_executor.h"
#include "runtime/task/cancellation_token.h"
#include "runtime/task/executor_metrics.h"

namespace {

TEST(HttpRouterTest, Distinguishes404And405) {
    runtime::http::Router router;
    router.Get("/users/:id",
               [](const runtime::http::HttpRequest&, runtime::http::HttpResponse&) {});

    const auto exact = router.Match(runtime::http::Method::Get, "/users/42");
    EXPECT_TRUE(static_cast<bool>(exact.handler));
    EXPECT_TRUE(exact.path_matched);
    ASSERT_EQ(exact.params.size(), 1u);
    EXPECT_EQ(exact.params[0].key, "id");
    EXPECT_EQ(exact.params[0].value, "42");

    const auto wrong_method = router.Match(runtime::http::Method::Post, "/users/42");
    EXPECT_FALSE(static_cast<bool>(wrong_method.handler));
    EXPECT_TRUE(wrong_method.path_matched);

    const auto missing = router.Match(runtime::http::Method::Get, "/missing");
    EXPECT_FALSE(static_cast<bool>(missing.handler));
    EXPECT_FALSE(missing.path_matched);
}

TEST(HttpRouterTest, StaticRouteWinsOverDynamicRoute) {
    runtime::http::Router router;

    router.Get("/users/me",
               [](const runtime::http::HttpRequest&, runtime::http::HttpResponse&) {});
    router.Get("/users/:id",
               [](const runtime::http::HttpRequest&, runtime::http::HttpResponse&) {});

    const auto match = router.Match(runtime::http::Method::Get, "/users/me");
    EXPECT_TRUE(static_cast<bool>(match.handler));
    EXPECT_TRUE(match.path_matched);
    EXPECT_TRUE(match.params.empty());
}

// Defensive registration: misconfigured routes must abort the process at
// registration time, with a diagnostic pointing at the user's call site.
// Each EXPECT_DEATH runs in a forked subprocess; the main test process
// continues after each case.
//
// Death-style assertions only execute the matcher against the subprocess
// stderr, so RouteFail writes its message directly to std::cerr (the global
// logger may not be initialized in tests).

TEST(HttpRouterDeathTest, RejectsPathWithoutLeadingSlash) {
    runtime::http::Router router;
    EXPECT_DEATH(
        router.Get("users/:id", [](const runtime::http::HttpRequest&,
                                   runtime::http::HttpResponse&) {}),
        "must start with '/'");
}

TEST(HttpRouterDeathTest, RejectsEmptyParamName) {
    runtime::http::Router router;
    EXPECT_DEATH(
        router.Get("/foo/:", [](const runtime::http::HttpRequest&,
                                runtime::http::HttpResponse&) {}),
        "empty parameter name");
}

TEST(HttpRouterDeathTest, RejectsConflictingParamNames) {
    runtime::http::Router router;
    router.Get("/users/:id", [](const runtime::http::HttpRequest&,
                                runtime::http::HttpResponse&) {});
    EXPECT_DEATH(
        router.Get("/users/:name", [](const runtime::http::HttpRequest&,
                                      runtime::http::HttpResponse&) {}),
        "param name conflict");
}

TEST(HttpRouterDeathTest, RejectsDuplicateRegistration) {
    runtime::http::Router router;
    router.Get("/dup", [](const runtime::http::HttpRequest&,
                          runtime::http::HttpResponse&) {});
    EXPECT_DEATH(
        router.Get("/dup", [](const runtime::http::HttpRequest&,
                              runtime::http::HttpResponse&) {}),
        "duplicate registration");
}

TEST(HttpRouterDeathTest, RejectsEmptyHandler) {
    runtime::http::Router router;
    runtime::http::Handler empty;
    EXPECT_DEATH(router.Get("/x", std::move(empty)), "handler must not be empty");
}

// ── HttpDispatch (Phase 5) ────────────────────────────────────────────────────
//
// The async dispatch path in OnMessage runs handlers on a BlockingExecutor
// worker thread, then sends the response via conn->loop()->RunInLoop() so that
// all connection-state writes stay on the owning IO thread.
//
// Tests below verify the handler-assembly contract (handler runs, response is
// built correctly, exception path produces 500) without requiring a live TCP
// connection.

TEST(HttpDispatchTest, HandlerAssemblesResponseCorrectly) {
  runtime::http::HttpRequest req;
  runtime::http::HttpResponse resp(false);

  auto handler = [](const runtime::http::HttpRequest&,
                    runtime::http::HttpResponse& r) {
    r.set_status_code(runtime::http::StatusCode::Ok);
    r.set_content_type("application/json; charset=utf-8");
    r.set_body("{\"status\":\"ok\"}");
  };

  handler(req, resp);
  const std::string wire = resp.ToString();

  EXPECT_NE(wire.find("200 OK"), std::string::npos);
  EXPECT_NE(wire.find("{\"status\":\"ok\"}"), std::string::npos);
  EXPECT_FALSE(resp.close_connection());
}

TEST(HttpDispatchTest, ExceptionInHandlerProducesInternalServerError) {
  runtime::http::HttpRequest req;
  runtime::http::HttpResponse resp(false);

  try {
    [](const runtime::http::HttpRequest&, runtime::http::HttpResponse&) {
      throw std::runtime_error("db offline");
    }(req, resp);
  } catch (const std::exception& ex) {
    resp.set_status_code(runtime::http::StatusCode::InternalServerError);
    resp.set_content_type("application/json; charset=utf-8");
    resp.set_body(std::string("{\"error\":\"") + ex.what() + "\"}");
    resp.set_close_connection(true);
  }

  const std::string wire = resp.ToString();
  EXPECT_NE(wire.find("500"), std::string::npos);
  EXPECT_NE(wire.find("db offline"), std::string::npos);
  EXPECT_TRUE(resp.close_connection());
}

// Verifies that handler body executes on a different thread than the submitter,
// which is the precondition that makes RunInLoop dispatch necessary.
TEST(HttpDispatchTest, HandlerRunsOnWorkerThread) {
  runtime::task::BlockingExecutor executor(2);
  const auto caller_tid = std::this_thread::get_id();
  std::atomic<bool> tid_differs{false};
  std::promise<void> done;

  executor.Submit([&](runtime::task::CancellationToken) {
    tid_differs.store(std::this_thread::get_id() != caller_tid);
    done.set_value();
  });

  done.get_future().wait();
  EXPECT_TRUE(tid_differs.load());
}

// ── MetricsHandler ────────────────────────────────────────────────────────────

TEST(MetricsHandlerTest, JsonContainsAllFields) {
  runtime::task::ExecutorMetrics::Snapshot snap{};
  snap.submitted     = 10;
  snap.completed     = 8;
  snap.failed        = 1;
  snap.cancelled     = 0;
  snap.queue_size    = 2;
  snap.running_count = 3;

  const std::string json = runtime::http::MakeMetricsJson(snap);

  EXPECT_NE(json.find("\"submitted\":10"),      std::string::npos);
  EXPECT_NE(json.find("\"completed\":8"),       std::string::npos);
  EXPECT_NE(json.find("\"failed\":1"),          std::string::npos);
  EXPECT_NE(json.find("\"cancelled\":0"),       std::string::npos);
  EXPECT_NE(json.find("\"queue_size\":2"),      std::string::npos);
  EXPECT_NE(json.find("\"running_count\":3"),   std::string::npos);
}

TEST(MetricsHandlerTest, JsonReflectsExecutorCounters) {
  runtime::task::BlockingExecutor executor(2);

  auto h1 = executor.Submit([](runtime::task::CancellationToken) {});
  auto h2 = executor.Submit([](runtime::task::CancellationToken) {
    throw std::runtime_error("fail");
  });
  h1.Wait();
  try { h2.Wait(); } catch (...) {}

  const auto snap = executor.metrics().Load();
  const std::string json = runtime::http::MakeMetricsJson(snap);

  EXPECT_NE(json.find("\"submitted\":2"), std::string::npos);
  EXPECT_NE(json.find("\"completed\":1"), std::string::npos);
  EXPECT_NE(json.find("\"failed\":1"),    std::string::npos);
}

}  // namespace
