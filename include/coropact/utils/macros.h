// Copyright (c) 2026 Arsenova
// SPDX-License-Identifier: MIT
#pragma once

// NOLINTBEGIN(bugprone-macro-parentheses)

// Deletes copy construction and copy assignment for cls. Because the copy
// constructor is user-declared, no move constructor is generated implicitly;
// declare move operations explicitly when cls should be movable.
#define COROPACT_DELETE_COPY(cls) \
  cls(const cls&) = delete;   \
  cls& operator=(const cls&) = delete

// Deletes move construction and move assignment for cls. Declaring move
// operations also causes implicitly declared copy operations to be deleted;
// declare copy operations explicitly when cls should remain copyable.
#define COROPACT_DELETE_MOVE(cls) \
  cls(cls&&) = delete;        \
  cls& operator=(cls&&) = delete

// Deletes both copy and move operations for cls.
#define COROPACT_DELETE_COPY_MOVE(cls) \
  COROPACT_DELETE_COPY(cls);           \
  COROPACT_DELETE_MOVE(cls)

// Deletes copy operations and explicitly requests default move operations for
// cls. The move operations can still be implicitly deleted when a base class
// or data member is not movable.
#define COROPACT_DISABLE_COPY_ALLOW_MOVE(cls) \
  COROPACT_DELETE_COPY(cls);                  \
  cls(cls&&) = default;                   \
  cls& operator=(cls&&) = default

// NOLINTEND(bugprone-macro-parentheses)
