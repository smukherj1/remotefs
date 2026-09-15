---
name: coding-guidelines
description: |
  Use when writing code. Do not activate when writing documents or running other commands.
  Do not activate when writing code snippets within documents.
---

# Coding Guidelines

## Public API

- Keep methods, functions, structs, types, variables and constants private unless another module needs them.
- Minimize the public API surface of a module to simplify dependencies.
- Expose domain concepts, not transport plumbing.

## Structure

### File Structure

- Put all file level constants, variables in a contiguous block at the top. Sequence: Public constants, private constants, public variables, private variables.
- Keep one contiguous public API (types and functions) section at the top of each source file, followed by private implementation items; do not resume public declarations after private ones.
- Once an invariant has been verified in a public API, avoid re-verifying it in deeper helpers or later non-public methods unless these helpers are used in another public API code path
  that doesn't check the invariant.

### Functions

- Use comments to document functions as follows:
  - What the function does.
  - Each input argument, type and the expected value / constraints.
  - The returned result(s) and what they contain. Explain how it's related
    to the input if applicable.
  - Any side effects if any.
  - Possible errors.
- Aim for a function to be single responsibility. Delegate to helpers if a
  function does multiple things.
- Prefer returning early at the top of the function for edge conditions.
- Avoid nesting multiple levels of if/else or other control flow checks. Prefer
  returning or delegating the nested control flow to a function. Also avoid more than
  one level of nesting within a for loop wherever possible.
- Similarly, audit functions longer than 50 lines and prefer breaking them into helper
  functions.
- Extract branches that form distinct workflows or obscure control flow.
- Avoid abstractions (types or helper functions) that are only used in a single place unless
  it's to shorten a function body, loop or control flow (if/else/match) condition below 10s of
  lines.
- ALWAYS use the modern rust module layout, i.e., NEVER create <module name>/mod.rs. Instead create <module name>.rs and <mod ulename>/<sub module>.rs files.

## Types, Configuration, and Data

- Represent closed sets with enums or domain types, not strings.
- Minimize the number of public types, enums and constants.
- Document the purpose and members/variant of every type and enum.
- Document the purpose of every constant and module level variables.
- Name shared limits and defaults as module level constants with comments documenting their purpose.
- Validate configuration at construction; make invalid states unrepresentable when feasible.
- Prefer reusing types when introducing new ones. Consider if refactoring out
  common elements into a shared type is possible.

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
