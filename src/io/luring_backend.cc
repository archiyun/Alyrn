// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#include "coropact/io/luring_backend.h"

#include "coropact/luring/capability.h"

namespace coropact::io {

namespace {

[[nodiscard]]
luring::RuntimeProfile ToLuringProfile(
    CapabilitySet requested_profile) noexcept {
  auto profile = luring::RuntimeProfile::Core();

  if (requested_profile.Has(IoRequirement::kRecvSource)) {
    profile = profile.Require(luring::NativeFeature::kProvidedBufferRing);
  }
  if (requested_profile.Has(IoRequirement::kIncrementalBufferLease)) {
    profile = profile.Require(
        luring::NativeFeature::kProvidedBufferRingIncremental);
  }
  if (requested_profile.Has(IoRequirement::kSendZeroCopy)) {
    profile = profile.Require(luring::NativeFeature::kSendZeroCopy);
  }

  return profile;
}

}  // namespace

base::Result<luring::RuntimeBinding> BindLuring(
    luring::LUringOptions& options,
    CapabilitySet requested_profile) noexcept {
  const auto native_profile = ToLuringProfile(requested_profile);
  options.active_profile = native_profile;
  return luring::BindLUring(options, native_profile);
}

}  // namespace coropact::io
