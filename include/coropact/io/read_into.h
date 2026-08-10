// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

// Public spelling for the backend-neutral owned-read outcome. Its definition
// remains in net because backend adapters use it below the io facade.
#include "coropact/net/read_into.h"

namespace coropact::io {

using ReadIntoOutcome = ::coropact::net::ReadIntoOutcome;

}  // namespace coropact::io
