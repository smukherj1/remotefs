---
name: coding-guidelines
description: |
  Use when writing code. Do not activate when writing documents or running other commands.
  Do not activate when writing code snippets within documents.
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
- Pass constructed resource owners across operational boundaries, not the paths,
  connection details, or configuration used to create them. Restrict those
  construction inputs to open/create/setup code.
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
- ALWAYS use the modern rust module layout, i.e., NEVER create <module name>/mod.rs. Instead create <module name>.rs and <mod ulename>/<sub module>.rs files.

## Types, Configuration, and Data

- Represent closed sets with enums or domain types, not strings.
- Reuse an existing boundary or persistence type when its fields and semantics
  match; introduce a second shape only for a distinct invariant or operation.
- Name shared limits and defaults as module level constants with comments documenting their purpose.
- Validate configuration at construction; make invalid states unrepresentable when feasible.
- Make lossy, truncating, overflowing, or semantic conversions explicit.
- Clone only for ownership, async execution, retries, or API boundaries.
- Borrow or stream large values; minimize retention.

## Errors

- Audit every propagation site (`?`, throw, rejection). Always add context on what was being attempted.
- NEVER use raw ? to return errors without additional context. Implement the Context error enum variant
  allowing usage of the error_context module in rfs-common to add context for errors.
- Include stable identifiers: paths, digests, operations, instances, resources, entries, environment variables, and proto paths.
- Construct formatted or allocated context lazily.
- Use `anyhow::Context` only in functions returning `anyhow::Result`. Use RemoteFS context helpers for typed errors such as `TreeError`, `CasError`, `UploadError`, `DigestError`, and `ConfigError`.
- Preserve structured errors; use `map_err` to create identifier-bearing variants.

## Documentation

- Comment _all_ methods, types, fields, proto methods, db tables and columns, and requests.
- For functions, comments should include inputs, outputs, errors, preconditions, and side effects.
- Comment all edge cases and non-trivial logic like early returns in functions or if/match conditions.
- Use plain language suitable for explaining to a junior engineer whose first
  language is not english.

## Tests

- Test behavior through the module's or type's public API, including boundary
  conditions and failure modes; do not couple tests to private call structure.
- Use integration tests across CAS, daemon, FUSE, process, network, or filesystem boundaries.
- Use comments in every test to describe:
  - The setup, i.e., what components are initialized with what date or state such as open
    files, db connections, etc.
  - Whether we expect to succeed or error and why.
  - Almost _always_ avoid probing internal state in the test. Probing internal
    state should be the exception and clearly document on why the condition
    being test is so critical for the overall system to warrant it.
