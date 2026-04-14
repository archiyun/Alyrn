#include <gtest/gtest.h>

#include "runtime/http/router.h"

namespace {

TEST(HttpRouterTest, Distinguishes404And405) {
    runtime::http::Router router;
    router.Get("/users/:id",
               [](const runtime::http::HttpRequest&, runtime::http::HttpResponse&) {});

    const auto exact = router.Match(runtime::http::Method::Get, "/users/42");
    EXPECT_TRUE(static_cast<bool>(exact.handler));
    EXPECT_TRUE(exact.path_matched);
    ASSERT_EQ(exact.params.count("id"), 1u);
    EXPECT_EQ(exact.params.at("id"), "42");

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

}  // namespace
