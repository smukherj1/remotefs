---
name: program-design
description: Use when asked to write a program-design document describing a program's structure — how code is organized into modules, their interfaces, and inter-dependencies.
Do not trigger when writing code or other technical documents. Do not trigger unless progress-design document is explicitly mentioned.
---

# Program Design Documents

A program-design document describes a program's structure: how code is organized
into modules, each module's interface, and how modules depend on each other.
Write one when a change spans multiple components and needs a written design
before implementation.

## Relationship to the technical design

- `docs/technical-design.md` records architecture decisions, protocols, and
  permanent invariants. A program-design document records the shape of one
  change: which components change, the new struct and API shapes, schema, and
  tests.
- Never restate what the technical design already covers. Replace it with a
  reference ("per `docs/technical-design.md`"). Cut any sentence the reader
  could find in the technical design.

## Format

A `## Purpose` section, a `## Highlights` section, one section per changing
component, then a section for removed and unchanged components. Work top-down
from coarse to fine detail.

A rust module is a component. The component may wrap around a database in which
case the SQL schema is part of the component.

### `## Purpose`

One or two sentences in precise terminology stating what the change accomplishes
and, when relevant, what callers stop seeing. Minimum needed to orient the
reader; anything more repeats the highlights or the technical design.

### `## Highlights`

One bullet per changing component. Format: `**`Component`** — one or two lines`
describing what is changing. Keep each line at the component level (responsibilities
gained or lost, APIs removed), not implementation detail.

## Per-component sections

Each changing component gets its own `## <Component>` section with these
subsections in this order.

### `### Changed behavior and dependencies`

A bulleted list. Each bullet states one changed behavior (a responsibility
gained or lost, a guarantee added, an API removed) or one dependency (the
component's direct dependencies). List what the component stops depending on
explicitly. Do not re-derive invariants the technical design already establishes.

### `### Target shape`

The new version of the component's publicly visible structs as a code snippet:

- One doc comment on the struct explaining its new purpose.
- Every member with its own doc comment, using the real struct names from the codebase.
- Supporting structs the component is built from, each with their own comments.
- Real language and real type names; no pseudocode.

### `### Public API`

The complete callable surface, code-snippet form. Each method is documented so
an implementer can code to it without guessing:

- What the method does.
- What each argument represents.
- What is returned and the error conditions.
- Side effects: I/O, cache admission or admission removal, counter updates,
  durable writes, locking, lifecycle transitions.
- When the refactor removes public names, list them explicitly (e.g.,
  "Removed from the public boundary: `X`, `Y`").

A component private to a module still documents its complete callable surface in
this subsection, using the module-private visibility as written.

### `### Schema`

Only for components that own durable state (repositories, stores, daemons).
Document:

- The exact SQL for the new or changed tables and indexes.
- The bulleted validation and enforcement rules applied by the private row
  decoder or write validator.
- What is dropped or renamed, and whether existing data is migrated.

### `### Unit tests`

Bulleted test coverage for the component. Every test calls only the component's
public or component-level API — no internal helpers, raw connections, direct
SQL, or private decoders. If a behavior is only reachable through internals,
exercise it through the public API instead. Test observable results, not lock
maps or internal state.

## Removed and unchanged components

Short subsections for components that are removed (name what is removed and
where its responsibilities moved) and for components that keep their behavior
(one line confirming no API change and where new behavior is instead tested).

## Acceptance

Before the document is complete:

- Nothing restates the technical design; anything already decided there is a
  reference, not content.
- Terminology matches the technical design exactly.
- Every changing component has a section with the five subsections.
- Every struct member and every public method is documented: purpose, arguments,
  returns, error conditions, and side effects.
- Every durable-state component documents its schema.
- Unit tests exercise each component's public API only.
