# RemoteFS Coding Guidelines

These guidelines capture style and quality expectations remotefs code.

## Public API

- Keep items private by default. Make functions, types, and fields public only when another module needs that exact surface.
- Public functions and methods must document expected arguments, return values, and important error behavior.
- Do not expose protocol plumbing as public API unless callers need to reason about it directly. Resource-name construction, verification helpers, retry classifiers, packing helpers, and transport adapters should usually stay private.

## Structure

- Keep public methods small enough that the main control flow is obvious.
- Split large branches into private helpers when each branch performs a distinct workflow, such as batch download versus ByteStream download.
- Minimize the level of control flow (loops or if else nesting) within a function.
  - Nesting more than once is a MAJOR red flag. Should be avoided.
  - Avoid large bodies for if/else because it makes the overall function hard to follow. Prefer early returns at the top of a function to
    deal with error conditions. Otherwise, refactor into helpers.
  - Within a function, DO NOT check a condition if it's already guaranteed to never happen based on an above condition.
  - Within a package/crate, prefer to not check a condition if it's already been checked unless it prevents a fatal error that would
    trigger an immediate exit like out of bounds array access.
- Prefer one helper per operation boundary when it improves readability: build request, send RPC, validate response, transform result.
- Avoid unrelated refactors while implementing a plan step. If cleanup is necessary, keep it in the same ownership boundary as the change.

## Types And Constants

- Prefer typed enums or small domain types over hardcoded strings for closed sets such as operation names, modes, states, or protocol variants.
- Put shared limits and defaults in named constants. Validate configuration against those constants at construction time.
- Reject invalid configuration early. Do not allow impossible values such as zero retry attempts to reach runtime control flow.

## Data Movement

- Avoid unnecessary data cloning. Clone request data only where ownership, async retries, or tonic request construction requires it.

## Error Context

- Add context whenever an error is propagated, especially at `?` sites. The context should state what operation was in progress, not just that something failed.
- Prefer lazy context for dynamic messages so paths, digests, entry names, URLs, and counts are formatted only on error.
- Use `anyhow::Context` only in functions that return `anyhow::Result`.
- Use RemoteFS typed context helpers or traits in functions that return typed errors such as `TreeError`, `CasError`, `UploadError`, `DigestError`, or `ConfigError`.
- Keep `map_err` when it constructs a structured error variant that already includes the operation and relevant identifiers.
- Context messages should include stable debugging identifiers where available: paths, digests, CAS operation names, instance names, resource names, directory entry names, environment variable names, and proto paths.
- Do not add vague context such as `failed`, `error`, or `operation failed`; the source error already communicates failure.

## Comments

- _MUST_ comment non-obvious design choices, edge cases, protocol quirks, invariants, and tradeoffs.
- Strongly prefer briefly comment every struct, field and function with their purpose. For functions document
  the arguments and results and any assumptions / preconditions.
- Within the function body, explain the purpose of each condition branch with comments (unless extremely obvious from the condition itself).
- Do not leave review notes, temporary TODOs, or style reminders in production code. Convert them into code changes, tests, or durable design comments.
- Public APIs (methods and structures) _MUST_ have comments explaining the inputs, results, preconditions, side-effects and behaviors as applicable.
- Private APIs _SHOULD_ have comments (similar to public APIs) unless they're trivial wrappers.

## Tests

- Add unit tests for parsing, validation, policy decisions, and pure transformations.
- Add integration tests when behavior crosses a CAS, daemon, FUSE, process, or filesystem boundary.
- Test public behavior rather than private helper shape, unless the helper encodes an important policy that would be difficult to exercise through the public API.
