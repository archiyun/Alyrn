// SPDX-License-Identifier: MIT
#pragma once

// Public spelling for the backend-neutral owned-read outcome. Its definition
// remains in net because backend adapters use it below the io facade.
#include "alyrn/net/read_into.h"

namespace alyrn::io {

using ReadIntoOutcome = ::alyrn::net::ReadIntoOutcome;

}  // namespace alyrn::io
