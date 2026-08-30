#!/usr/bin/env python3
"""Fail if header paths and C++ scopes disagree, or if a friend can bind the wrong detail.

Compilation can succeed when `detail::` inside an unnamed namespace, or a
mixed `detail::Fn(::alyrn::uring::detail::Op*)` friend, names a different
function than the one defined in the header. This check rejects those
spellings and checks that `*/detail/*.h` actually opens that module's detail
namespace.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INCLUDE = ROOT / "include" / "alyrn"
SRC = ROOT / "src"

# Public types that live in a detail/ header on purpose.
PATH_NAMESPACE_EXCEPTIONS = {
    # Public Task / Returnable live in alyrn::coro; the header is only stored
    # under coro/detail to keep task.h's implementation types together.
    INCLUDE / "coro" / "detail" / "task_fwd.h",
    # Macro-only header; no namespace.
    INCLUDE / "detail" / "macros.h",
}

# `friend void detail::Fn(::alyrn::...)` — not `::alyrn::uring::detail::Fn`.
MIXED_FRIEND = re.compile(
    r"friend\s+void\s+detail::\w+\s*\(\s*::alyrn::",
    re.MULTILINE,
)
UNNAMED_FRIEND_DETAIL = re.compile(
    r"friend\s+void\s+detail::",
)


def iter_sources() -> list[Path]:
    files: list[Path] = []
    for root in (INCLUDE, SRC, ROOT / "tests", ROOT / "examples"):
        if not root.exists():
            continue
        files.extend(root.rglob("*.h"))
        files.extend(root.rglob("*.cc"))
        files.extend(root.rglob("*.cpp"))
    return files


def check_mixed_friends(path: Path, text: str, errors: list[str]) -> None:
    for match in MIXED_FRIEND.finditer(text):
        line = text[: match.start()].count("\n") + 1
        errors.append(
            f"{path}:{line}: mixed friend scope: `detail::Fn(::alyrn::...)` "
            "can bind a different function than the definition. "
            "Use `detail::Fn(detail::Op*)` in `alyrn::<backend>`, or "
            "`::alyrn::<backend>::detail::Fn(::alyrn::<backend>::detail::Op*)` "
            "from an unnamed namespace."
        )


def check_unnamed_namespace_friends(path: Path, text: str, errors: list[str]) -> None:
    if "namespace {" not in text and "namespace{" not in text:
        return
    unnamed_at = text.find("namespace {")
    if unnamed_at < 0:
        unnamed_at = text.find("namespace{")
    close_unnamed = text.find("}  // namespace", unnamed_at)
    region = text[unnamed_at:close_unnamed] if close_unnamed > unnamed_at else text[unnamed_at:]
    for match in UNNAMED_FRIEND_DETAIL.finditer(region):
        line = text[: unnamed_at + match.start()].count("\n") + 1
        errors.append(
            f"{path}:{line}: friend `detail::` inside an unnamed namespace "
            "looks up `detail` from enclosing scopes and can silently bind "
            "the wrong function. Spell `::alyrn::<backend>::detail::...`."
        )


def expected_detail_namespace(header: Path) -> str | None:
    try:
        relative = header.relative_to(INCLUDE)
    except ValueError:
        return None
    parts = relative.parts
    if len(parts) >= 3 and parts[1] == "detail":
        return f"alyrn::{parts[0]}::detail"
    if len(parts) == 2 and parts[0] == "detail":
        return "alyrn::detail"
    return None


def header_opens_namespace(text: str, namespace: str) -> bool:
    if namespace == "alyrn::detail":
        return bool(re.search(r"namespace\s+alyrn::detail\b", text))
    module = namespace.removeprefix("alyrn::").removesuffix("::detail")
    compact = re.search(rf"namespace\s+alyrn::{module}::detail\b", text)
    nested = re.search(
        rf"namespace\s+alyrn::{module}\b[\s\S]*?namespace\s+detail\b",
        text,
    )
    return bool(compact or nested)


def check_header_namespaces(errors: list[str]) -> None:
    for header in sorted(INCLUDE.rglob("*.h")):
        expected = expected_detail_namespace(header)
        if expected is None:
            continue
        if header in PATH_NAMESPACE_EXCEPTIONS:
            continue
        text = header.read_text()
        if not header_opens_namespace(text, expected):
            errors.append(
                f"{header.relative_to(ROOT)}: path is under `{expected.replace('::', '/')}` "
                f"but the file never opens `namespace {expected}`."
            )


def main() -> int:
    errors: list[str] = []
    for path in iter_sources():
        text = path.read_text()
        rel = path
        check_mixed_friends(rel, text, errors)
        check_unnamed_namespace_friends(rel, text, errors)
    check_header_namespaces(errors)

    if errors:
        print("namespace / friend scope check failed:\n", file=sys.stderr)
        for error in errors:
            print(f"  {error}", file=sys.stderr)
        return 1
    print(f"ok: checked {len(iter_sources())} sources")
    return 0


if __name__ == "__main__":
    sys.exit(main())
