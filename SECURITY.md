# Security Policy

This is an academic capstone project — there are no tagged releases or
supported version branches. Only the current `main` branch is maintained.

## Reporting a Vulnerability

If you discover a security issue in the runtime (memory safety bug,
unbounded resource consumption, parser flaw, etc.):

1. **Do not open a public GitHub issue.**
2. Email the maintainer at **furina.see.fun@gmail.com** with:
   - A short description of the issue and its impact
   - Reproduction steps or a minimal test case
   - The commit hash you reproduced against
3. You can expect an acknowledgement within roughly one week. Because this
   is a solo academic project, fixes are best-effort and not bound to any
   SLA.

## Scope

In scope:
- `runtime_foundation`, `runtime_task`, `runtime_net`, `runtime_http`,
  `runtime_gateway` library code
- HTTP/1.1 parsing and routing

Out of scope:
- Example programs under `examples/`
- Local development scripts under `scripts/`
- Benchmarks and demo capture artifacts
