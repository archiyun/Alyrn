// SPDX-License-Identifier: MIT
#pragma once

// Public spelling for the backend-neutral owned-read outcome. Its definition
// remains in net because backend adapters use it below the io facade.
#include "alyrn/net/recv.h"

namespace alyrn::io {

using RecvOutcome = net::RecvOutcome;

}  // namespace alyrn::io
