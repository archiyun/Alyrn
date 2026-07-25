// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

// Public umbrella header for the gateway components.

#include "coropact/gateway/circuit_breaker.h" // IWYU pragma: export
#include "coropact/gateway/fallback_config.h" // IWYU pragma: export
#include "coropact/gateway/forwarded_header_context.h" // IWYU pragma: export
#include "coropact/gateway/gateway_config.h" // IWYU pragma: export
#include "coropact/gateway/gateway_core.h" // IWYU pragma: export
#include "coropact/gateway/gateway_health_service.h" // IWYU pragma: export
#include "coropact/gateway/gateway_session_service.h" // IWYU pragma: export
#include "coropact/gateway/health_check_config.h" // IWYU pragma: export
#include "coropact/gateway/load_balancer.h" // IWYU pragma: export
#include "coropact/gateway/proxy_pass.h" // IWYU pragma: export
#include "coropact/gateway/rate_limiter.h" // IWYU pragma: export
#include "coropact/gateway/upstream.h" // IWYU pragma: export
#include "coropact/io/async_connector.h" // IWYU pragma: export
#include "coropact/gateway/upstream_conn_pool.h" // IWYU pragma: export
#include "coropact/gateway/upstream_peer.h" // IWYU pragma: export
#include "coropact/gateway/upstream_registry.h" // IWYU pragma: export
