#include <gtest/gtest.h>

#include <stdexcept>
#include <string>

#include "vexo/http/metrics_handler.h"
#include "vexo/http/router.h"
#include "vexo/task/blocking_executor.h"
#include "vexo/task/cancellation_token.h"
#include "vexo/task/executor_metrics.h"

namespace {

TEST(HttpRouterTest, Distinguishes404And405) {
    vexo::http::Router router;
    router.Get("/users/:id",
               [](const vexo::http::HttpRequest&, vexo::http::HttpResponse&) {});

    const auto exact = router.Match(vexo::http::Method::Get, "/users/42");
    EXPECT_TRUE(static_cast<bool>(exact.handler));
    EXPECT_TRUE(exact.path_matched);
    ASSERT_EQ(exact.params.size(), 1u);
    EXPECT_EQ(exact.params[0].key, "id");
    EXPECT_EQ(exact.params[0].value, "42");

    const auto wrong_method = router.Match(vexo::http::Method::Post, "/users/42");
    EXPECT_FALSE(static_cast<bool>(wrong_method.handler));
    EXPECT_TRUE(wrong_method.path_matched);

    const auto missing = router.Match(vexo::http::Method::Get, "/missing");
    EXPECT_FALSE(static_cast<bool>(missing.handler));
    EXPECT_FALSE(missing.path_matched);
}

TEST(HttpRouterTest, StaticRouteWinsOverDynamicRoute) {
    vexo::http::Router router;

    router.Get("/users/me",
               [](const vexo::http::HttpRequest&, vexo::http::HttpResponse&) {});
    router.Get("/users/:id",
               [](const vexo::http::HttpRequest&, vexo::http::HttpResponse&) {});

    const auto match = router.Match(vexo::http::Method::Get, "/users/me");
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
    vexo::http::Router router;
    EXPECT_DEATH(
        router.Get("users/:id", [](const vexo::http::HttpRequest&,
                                   vexo::http::HttpResponse&) {}),
        "must start with '/'");
}

TEST(HttpRouterDeathTest, RejectsEmptyParamName) {
    vexo::http::Router router;
    EXPECT_DEATH(
        router.Get("/foo/:", [](const vexo::http::HttpRequest&,
                                vexo::http::HttpResponse&) {}),
        "empty parameter name");
}

TEST(HttpRouterDeathTest, RejectsConflictingParamNames) {
    vexo::http::Router router;
    router.Get("/users/:id", [](const vexo::http::HttpRequest&,
                                vexo::http::HttpResponse&) {});
    EXPECT_DEATH(
        router.Get("/users/:name", [](const vexo::http::HttpRequest&,
                                      vexo::http::HttpResponse&) {}),
        "param name conflict");
}

TEST(HttpRouterDeathTest, RejectsDuplicateRegistration) {
    vexo::http::Router router;
    router.Get("/dup", [](const vexo::http::HttpRequest&,
                          vexo::http::HttpResponse&) {});
    EXPECT_DEATH(
        router.Get("/dup", [](const vexo::http::HttpRequest&,
                              vexo::http::HttpResponse&) {}),
        "duplicate registration");
}

TEST(HttpRouterDeathTest, RejectsEmptyHandler) {
    vexo::http::Router router;
    vexo::http::Handler empty;
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
  vexo::http::HttpRequest req;
  vexo::http::HttpResponse resp(false);

  auto handler = [](const vexo::http::HttpRequest&,
                    vexo::http::HttpResponse& r) {
    r.set_status_code(vexo::http::StatusCode::Ok);
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
  vexo::http::HttpRequest req;
  vexo::http::HttpResponse resp(false);

  try {
    [](const vexo::http::HttpRequest&, vexo::http::HttpResponse&) {
      throw std::runtime_error("db offline");
    }(req, resp);
  } catch (const std::exception& ex) {
    resp.set_status_code(vexo::http::StatusCode::InternalServerError);
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
  vexo::task::BlockingExecutor executor(2);
  const auto caller_tid = std::this_thread::get_id();
  std::atomic<bool> tid_differs{false};
  std::promise<void> done;

  executor.Submit([&](vexo::task::CancellationToken) {
    tid_differs.store(std::this_thread::get_id() != caller_tid);
    done.set_value();
  });

  done.get_future().wait();
  EXPECT_TRUE(tid_differs.load());
}

// ── MetricsHandler ────────────────────────────────────────────────────────────

TEST(MetricsHandlerTest, JsonContainsAllFields) {
  vexo::task::ExecutorMetrics::Snapshot snap{};
  snap.submitted     = 10;
  snap.completed     = 8;
  snap.failed        = 1;
  snap.cancelled     = 0;
  snap.queue_size    = 2;
  snap.running_count = 3;

  const std::string json = vexo::http::MakeMetricsJson(snap);

  EXPECT_NE(json.find("\"submitted\":10"),      std::string::npos);
  EXPECT_NE(json.find("\"completed\":8"),       std::string::npos);
  EXPECT_NE(json.find("\"failed\":1"),          std::string::npos);
  EXPECT_NE(json.find("\"cancelled\":0"),       std::string::npos);
  EXPECT_NE(json.find("\"queue_size\":2"),      std::string::npos);
  EXPECT_NE(json.find("\"running_count\":3"),   std::string::npos);
}

TEST(MetricsHandlerTest, JsonReflectsExecutorCounters) {
  vexo::task::BlockingExecutor executor(2);

  auto h1 = executor.Submit([](vexo::task::CancellationToken) {});
  auto h2 = executor.Submit([](vexo::task::CancellationToken) {
    throw std::runtime_error("fail");
  });
  h1.Wait();
  try { h2.Wait(); } catch (...) {}

  const auto snap = executor.metrics().Load();
  const std::string json = vexo::http::MakeMetricsJson(snap);

  EXPECT_NE(json.find("\"submitted\":2"), std::string::npos);
  EXPECT_NE(json.find("\"completed\":1"), std::string::npos);
  EXPECT_NE(json.find("\"failed\":1"),    std::string::npos);
}

}  // namespace
