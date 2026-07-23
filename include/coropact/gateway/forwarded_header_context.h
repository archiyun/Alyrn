// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

#include <string_view>

namespace coropact::gateway {

struct ForwardedHeaderContext {
  std::string_view client_ip;
  std::string_view scheme;
  std::string_view gateway_name;
  std::string_view request_id;
};

}  // namespace coropact::gateway
