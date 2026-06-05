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
- Prefer one helper per operation boundary when it improves readability: build request, send RPC, validate response, transform result.
- Avoid unrelated refactors while implementing a plan step. If cleanup is necessary, keep it in the same ownership boundary as the change.

## Types And Constants

- Prefer typed enums or small domain types over hardcoded strings for closed sets such as operation names, modes, states, or protocol variants.
- Put shared limits and defaults in named constants. Validate configuration against those constants at construction time.
- Reject invalid configuration early. Do not allow impossible values such as zero retry attempts to reach runtime control flow.

## Data Movement

- Avoid unnecessary data cloning. Clone request data only where ownership, async retries, or tonic request construction requires it.

## Comments

- Use comments to explain non-obvious design choices, edge cases, protocol quirks, invariants, and tradeoffs.
- Do not leave review notes, temporary TODOs, or style reminders in production code. Convert them into code changes, tests, or durable design comments.

## Tests

- Add unit tests for parsing, validation, policy decisions, and pure transformations.
- Add integration tests when behavior crosses a CAS, daemon, FUSE, process, or filesystem boundary.
- Test public behavior rather than private helper shape, unless the helper encodes an important policy that would be difficult to exercise through the public API.
