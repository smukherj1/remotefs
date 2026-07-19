---
name: coding-guidelines
description: Enforce code quality and a pre-handoff audit for implementation, refactoring, bug fixes, tests, code or PR reviews, and code-design feedback. Detect complex control flow, context-free errors, undocumented contracts, weak boundaries, wasteful data movement, and test gaps.
---

# Coding Guidelines

Write the simplest correct code. Expose control flow, ownership, invariants, and failures.

## Workflow

- Follow project and language rules when they conflict with this skill; report the tradeoff.
- Before editing, identify operation boundaries, invariants, edge cases, and error identifiers. Inspect affected adjacent code, not only added lines.
- Before handoff, audit the full diff, each changed source file's item order, and every changed function against all sections. Resolve violations or justify safer, clearer exceptions.
- State audit completion and exceptions in the final response.
- In reviews, report supported findings with location, impact, and remediation; omit unsupported style preferences.

## Public API

- Keep items private unless another module needs them.
- Expose domain concepts, not transport plumbing.
- Preserve compatibility unless the task includes migration.

## Structure

- Keep one contiguous public API section at the top of each source file, followed by private implementation items; do not resume public declarations after private ones.
- Give each function one operation or decision; use early returns to expose the main path.
- Extract branches that form distinct workflows or obscure control flow.
- Keep orchestration at one abstraction level: named phases, not phase internals.
- Review functions over about 40 non-blank lines, nesting beyond two control-flow levels, or long conditional/match chains. These trigger judgment, not automatic extraction; prefer helpers, early exits, or policy types when they improve verification.
- Dispatch heterogeneous loop items through a function returning a domain result; keep classification and side effects out of the loop body.
- Separate request construction, I/O, response validation, and transformation when their boundaries matter.
- Check an invariant once per function. Revalidate across boundaries only if data may change or the check prevents memory corruption, termination, or an invalid external operation.
- Keep refactors within the ownership boundary of the change. Separate unrelated cleanup.
- Prefer direct code over single-use abstractions that hide control flow.

## Types, Configuration, and Data

- Represent closed sets with enums or domain types, not strings.
- Name shared limits and defaults.
- Validate configuration at construction; make invalid states unrepresentable when feasible.
- Make lossy, truncating, overflowing, or semantic conversions explicit.
- Clone only for ownership, async execution, retries, or API boundaries.
- Borrow or stream large values; minimize retention.

## Errors

- Audit every propagation site (`?`, throw, rejection). Errors must identify the operation and entity.
- Do not propagate raw errors across operation boundaries. Permit raw `?` within the same operation when its structured error already contains both identifiers; do not add redundant context.
- Audit each layer of compound propagation such as Rust `??`; join and domain failures need separate context.
- Include stable identifiers: paths, digests, operations, instances, resources, entries, environment variables, and proto paths.
- Construct formatted or allocated context lazily.
- Use `anyhow::Context` only in functions returning `anyhow::Result`. Use RemoteFS context helpers for typed errors such as `TreeError`, `CasError`, `UploadError`, `DigestError`, and `ConfigError`.
- Preserve structured errors; use `map_err` to create identifier-bearing variants.
- Do not add context that only restates failure.

## Documentation

- Document methods, types, fields, proto methods, db tables and columns, and requests: inputs, outputs, errors, preconditions, and side effects.
- Introduce non-trivial modules and workflows with purpose, phase order, and guarantees.
- Document hidden contracts: invariants, rationale, protocol constraints, edge cases, and tradeoffs.
- Cover applicable filesystem and concurrency behavior: symlinks, unsupported nodes, mutation races, ordering, partial failure, cancellation, and retries.
- Document private contracts not evident from names, types, or control flow; do not narrate evident code.
- Resolve review notes and temporary reminders in code, tests, issues, or durable design comments.

## Tests

- Test observable behavior, boundary conditions, and failure modes.
- Use unit tests for parsing, validation, policy decisions, and pure transformations.
- Use integration tests across CAS, daemon, FUSE, process, network, or filesystem boundaries.
- Test private helpers only for policy impractical to exercise publicly.
- Control time, randomness, concurrency, and external state.
