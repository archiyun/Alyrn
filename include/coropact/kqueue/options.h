// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>

namespace coropact::kqueue {

/*
 * Selects readiness delivery for a kqueue stream. The underlying poller is
 * deliberately not part of this public option.
 *
 * The three modes differ in who is responsible for the next notification:
 *
 * - kLevelTriggered reports readiness for as long as the condition holds, so
 *   the owner must disable interest once it has consumed what it wanted.
 * - kEdgeTriggered (EV_CLEAR) reports only transitions, so the owner must
 *   drain the descriptor until it would block or risk stalling.
 * - kOneShot (EV_ONESHOT) reports once and then the kernel drops the
 *   registration, so the owner must re-arm for every operation.
 *
 * kOneShot is the mode that matches an awaited operation one-to-one: arming
 * is what starts the wait and delivery is what ends it, with no interest left
 * behind for the owner to remember to withdraw.
 */
enum class TriggerMode : std::uint8_t {
  kLevelTriggered,
  kEdgeTriggered,
  kOneShot,
};

}  // namespace coropact::kqueue
