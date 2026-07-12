# Coding Guidelines

Write the simplest code that correctly handles the problem. Make control flow, ownership, invariants, and failure modes easy to identify.

## Public API

- Keep items private unless another module needs them.
- Expose domain concepts, not protocol or transport plumbing.
- Document public inputs, outputs, errors, preconditions, and side effects.
- Preserve compatibility unless the change explicitly includes an API migration.

## Structure

- Keep each function focused on one operation or decision.
- Make the main path clear. Handle errors and exceptional cases with early returns.
- Avoid deep nesting. Extract a helper when a branch represents a distinct workflow or obscures the caller's control flow.
- Separate operation boundaries when doing so clarifies the code: construct a request, perform I/O, validate a response, and transform a result.
- Do not recheck an invariant within a function. Across module boundaries, validate again only when data may have changed or a check prevents memory corruption, process termination, or an invalid external operation.
- Keep refactors within the ownership boundary of the change. Separate unrelated cleanup.
- Prefer direct code over abstractions that have one use or hide control flow.

## Types and Configuration

- Represent closed sets with enums or domain types, not strings.
- Give shared limits and defaults named constants.
- Validate configuration at construction boundaries. Do not represent invalid states when the type system can prevent them.
- Make conversions explicit when they can truncate, overflow, lose information, or change semantics.

## Ownership and Data Movement

- Avoid cloning by default. Clone when ownership, asynchronous execution, retries, or an API boundary requires it.
- Stream or borrow large values when practical; do not retain data longer than needed.

## Errors

- Add context at propagation boundaries when it identifies the operation or affected value.
- Include stable identifiers such as paths, digests, operation names, instance names, resource names, entry names, environment variables, and proto paths.
- Use lazy context when constructing the message requires formatting or allocation.
- Use `anyhow::Context` only in functions returning `anyhow::Result`. Use RemoteFS context helpers for typed errors such as `TreeError`, `CasError`, `UploadError`, `DigestError`, and `ConfigError`.
- Preserve structured errors. Use `map_err` when creating a variant that carries the relevant operation and identifiers.
- Do not add context that only says an operation failed; the source error already conveys failure.

## Comments

- Explain invariants, non-obvious decisions, protocol constraints, edge cases, and tradeoffs.
- Do not narrate code that is clear from names and control flow. Improve the code instead.
- Document private items when their contract or behavior is not evident from the implementation.
- Do not leave review notes or temporary style reminders in production code. Resolve them in code, tests, issues, or durable design comments.

## Tests

- Test observable behavior, boundary conditions, and failure modes.
- Use unit tests for parsing, validation, policy decisions, and pure transformations.
- Use integration tests for behavior crossing CAS, daemon, FUSE, process, network, or filesystem boundaries.
- Test private helpers directly only when they encode policy that is impractical to exercise through a public interface.
- Keep tests deterministic. Control time, randomness, concurrency, and external state.
