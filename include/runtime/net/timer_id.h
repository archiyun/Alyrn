#pragma once

#include <cstdint>

namespace runtime::net {

class Timer;

struct TimerId {
    Timer *timer{nullptr};
    int64_t sequence{0};
};

} // namespace runtime::net