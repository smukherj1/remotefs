# `SessionStore` Refactor Findings

Review of `crates/rfs-common/src/session/store.rs` against the coding guidelines
(`.agents/skills/coding-guidelines/SKILL.md`) and the technical design
(`docs/technical-design.md`). Findings are ranked by importance: correctness and
honesty of the error model first, then structure and reuse, then documentation
and style.

All line numbers refer to the current revision of `store.rs` unless another
file is named.

---

## P4 — `create_directory_children` mixes five responsibilities

**Location:** `store.rs:131-222`.

**Problem.** One method performs: input validation, transaction setup, parent
existence/kind checks, idempotent already-loaded short-circuit, per-child
reconciliation (the classification match at 178-203 with side effects in the
loop body), marking loaded, and read-back + commit. At ~91 lines with a
three-level nest (method → loop → match arms), it exceeds the guideline
thresholds ("functions over about 40 non-blank lines, nesting beyond two
control-flow levels"). Additionally, the read-back-and-commit tail appears
twice (164-176 for the repeated-load branch and 211-221 for the normal path),
differing only in the commit label. A code comment (`// TODO: Simplify logic.`)
acknowledges this.

**Impact.** The reconciliation policy — the actual domain rule this method
exists to enforce — is buried under plumbing; the duplicated tail invites
edits to one copy only.

**Proposal.**

1. Extract `reconcile_remote_children(tx, path, parent, children) -> Result<(), _>`
   containing the per-child loop; keep classification in the match arms but route
   insertion through `insert_inode` (loop body then dispatches via domain-result
   functions, per guidelines).
2. Restructure so there is one common tail: reconcile conditionally, mark
   loaded only when rows were inserted, then a single
   read-children/commit/return sequence.
3. Optionally fold `Some(existing) if authoritative_overlay(..)` and
   `Some(existing) if remote_inode_matches(..)` into one
   `stored_entry_accepts(stored, proposed) -> bool` arm with a comment naming
   the rule (see P8).

---

## P5 — Transaction begin/commit boilerplate repeated at every write site

**Location:** `create_directory_children` (150-153, 169-175, 218-220),
`close` (231-234, 239-241), `initialize_database` (416-418, 437-439).

**Problem.** Every write repeats the same four-step dance: lock connection,
`transaction()` with a mapped begin error, work, `commit()` with a mapped
commit error. Only the operation label varies.

**Impact.** Noise that buries each operation's actual decisions; a future
change (e.g., retry-on-busy) must be applied in three places.

**Proposal.** Add a private helper on `SessionStore`, e.g.
`fn transaction(&self, operation: &'static str) -> Result<Transaction<'_>, SessionError>`
for the begin half, and/or a `with_transaction(&self, operation, body)` runner
that owns commit mapping. `initialize_database` can take an opened transaction
instead of building its own.

---

## P6 — `validate_inode`: boolean flag parameter and opaque field grouping

**Location:** `store.rs:636-713`; callers pass literal `true` at `store.rs:571`
(`validate_inode_row`) and `false` at `store.rs:487`
(`validate_child_inodes_for_creation`) and `store.rs:768` (`insert_inode`).

**Problem.**

- The `is_stored: bool` parameter hides control flow: callers pass opposite
  values and readers must trace which branches depend on it.
- The `file_values` tuple (`648-652`) replaces readable field names with
  `.0/.1/.2` indexing purely to reuse the tuple across three match arms; direct
  field access would be clearer and no longer.
- The three kind branches repeat exclusion checks (e.g., every non-symlink kind
  rejects `symlink_target.is_some()`), producing long `||` chains that are hard
  to verify against the schema's nullability rules.

At ~79 lines this also trips the length guideline.

**Impact.** The kind/field matrix is the store's central invariant; its current
shape resists verification and makes adding a node kind error-prone.

**Proposal.** Split into `validate_stored_inode` and `validate_proposed_inode`,
each delegating to small per-kind predicates (`validate_file_fields`,
`validate_symlink_fields`, `validate_directory_fields`) that take `&Inode` and
check exactly their kind's nullability/exclusivity rules. Drop the
`file_values` tuple.

---

## P7 — Positional inode row decoding is now localized

**Location:** `InodeRow` (`store.rs:334-398`), `validate_inode_row`
(`store.rs:507-573`), and `SessionMetadataRow` (`store.rs:839-901`).

**Status & Problem.**

- **Resolved structure:** A private `InodeRow` now owns `column_list()` and
  `from_rusqlite_row()`. SQLite callbacks only decode SQLite-shaped values.
  `validate_inode_row` separately converts IDs, kinds, modes, timestamps,
  booleans, paths, and digests before applying the shared inode invariants.
- Single-row lookups validate after `optional()`, while directory reads collect
  raw rows before validating them. Domain failures therefore remain
  `SessionError`s instead of being flattened into `FromSqlConversionFailure`.
- `SessionMetadataRow` now follows the same `column_list()` naming convention.
- Corruption-oriented tests at `store.rs:1209-1247` cover invalid kinds,
  booleans, partial timestamps, digests, modes, identity shape, and node-kind
  field combinations.
- **Remaining limitation:** both row types still use positional `row.get(...)`
  calls. The column-order contract is centralized and easier to audit, but it
  is not compile-time safe; reordering compatible columns can still misdecode a
  row.

**Impact.** The original mixed decoding/validation problem is resolved. Only
the localized positional column-order risk remains during schema evolution.

**Possible follow-up.** If compile-time-independent column ordering becomes
important, decode by column name or add a projection-order regression test.

---

## P8 — The "remote entry" rule is encoded three times under unrelated names

**Location:** `authoritative_overlay` (`store.rs:448-452`),
`remote_inode_matches` (`store.rs:453-461`), and the compound boolean in
`validate_child_inodes_for_creation` (`store.rs:488-502`).

**Status & Problem.**

- **Progress made:** The `directory_materializations` table has been removed;
  directory state is now held directly on `Inode` (`directory_remote_digest` and
  `directory_loaded`). `validate_directory_children` was renamed to
  `validate_child_inodes_for_creation`.
- **Remaining issue:** All three locations still encode aspects of one domain
  rule — _which entries are pure remote-backed rows and when a stored row wins
  over a proposed remote row_:
  - `authoritative_overlay` returns true for tombstones, overlay files, and
    unloaded local directories, but has no doc comment explaining the rule.
  - `validate_child_inodes_for_creation` (488-502) mixes `||`/`&&`/`matches!`
    across several lines; a `TODO` at line 488 notes that these combined checks
    need clearer separation.
  - `remote_inode_matches` compares stored vs. proposed but omits
    `file_content_dirty` and parent/name — correct, but undocumented.

**Impact.** A change to what counts as a remote entry must be mirrored in three
places.

**Proposal.** Define one documented predicate, e.g. `fn is_remote_entry(inode:
&Inode) -> bool` (tombstone-free, no overlay, clean content, files carry a
digest, directories carry digest + `loaded == Some(false)`) and use it in both
validation and reconciliation; rename `authoritative_overlay` to something
accurate like `stored_entry_is_authoritative` with a doc comment stating the
rule; add a comment to `remote_inode_matches` listing why dirty-state and
identity fields are intentionally not compared.

---

## P9 — `InodeId::INVALID` sentinel usage in proposed child rows

**Location:** `child_for_parent` (`store.rs:442-447`),
`validate_child_inodes_for_creation` (`store.rs:463-487`), `insert_inode`
(`store.rs:754-789`), test builders (`store.rs:1106-1161`).

**Status & Problem.**

- **Progress made:** The sentinel constant was promoted to an associated constant
  `InodeId::INVALID` on the `InodeId` type. `insert_inode` no longer infers
  `is_stored` mode from the sentinel value.
- **Remaining issue:** `Inode` is used both for fully numbered stored rows and
  unallocated proposed children. Proposed children are required to supply
  `id: InodeId::INVALID` and `parent: None`, which `child_for_parent` patches up
  before insertion.

**Impact.** Forces callers to construct `Inode` instances with dummy IDs and
sentinel checks.

**Proposal.** Introduce a `ProposedInode` input type (same fields minus `id`,
with `parent` set by the store helper) used by `create_directory_children` and
`insert_inode`; keep `Inode` strictly for stored, numbered rows.

---

## P10 — Documentation sweep required by guidelines

**Location:** file-wide.

**Status & Problem.**

- **Progress made:** Public `SessionStore` methods (`create`, `inspect`, `inode`,
  `child`, `get_directory_children`, `create_directory_children`, `close`) and
  key private helpers (`InodeRow`, `validate_inode_row`, its scalar validation
  helpers, `SessionMetadataRow`, `read_stored_session`, `validate_session`,
  `get_inode_by_id`, `lookup_child_in_dir_inode`, `ensure_inode_is_dir`, and
  `ensure_active`) now have guideline-compliant doc comments.
- **Remaining undocumented items:**
  - Constants: `SCHEMA_VERSION` (19), `SCHEMA_SQL` (20)
  - Private helpers: `connection` (244), `initialize_database` (400),
    `child_for_parent` (442), `authoritative_overlay` (448),
    `remote_inode_matches` (453), `validate_child_inodes_for_creation` (463),
    `validate_inode` (636), `validate_stored_identity` (715),
    `validate_child_name` (746), `insert_inode` (754), `read_children` (816),
    `open_database` (1008), `prepare_schema` (1029), `schema_version` (1044),
    `validate_schema_version` (1049), `kind_text` (1059), `db_error` (1066).
- Specific wording fix: `now_seconds` doc comment still says "for session
  metadata" even though it is also used by `close()`.

**Impact.** Guideline deviation for remaining private functions and module
constants.

---

## P11 — Minor style and structure nits

1. **Missing blank lines between items** at 447/448 (`child_for_parent` /
   `authoritative_overlay`), 452/453 (`authoritative_overlay` / `remote_inode_matches`),
   1028/1029 (`open_database` / `prepare_schema`), 1043/1044 (`prepare_schema` /
   `schema_version`), 1048/1049 (`schema_version` / `validate_schema_version`),
   1058/1059 (`validate_schema_version` / `kind_text`), 1065/1066 (`kind_text` /
   `db_error`).
2. **`SessionStore::create` takes five loose scalars** (78-84): two `PathBuf`s,
   a `String`, a `u32`, and a `Digest` invite transposition at the call site.
   Group identity inputs into a seed struct.
3. **`close()` reads full metadata to compare one field** (235, 321-330):
   `ensure_active` calls `read_stored_session`, running digest and mountpoint
   validations on a path that only needs the state byte. Consider a narrow
   `read_lifecycle(connection, path)`.
4. **Root inode literal in `initialize_database`** (421-435): extract an
   `Inode::root(root_digest)` constructor.
5. **Test builders triplicated** (1106-1161): `remote_file`, `remote_directory`,
   `remote_symlink` share ten identical fields; a `base_proposed(name, kind)`
   helper would shrink them.
6. **Per-call `format!` SQL** in `get_inode_by_id` (265),
   `lookup_child_in_dir_inode` (293), `insert_inode` (778), `read_children`
   (822), and `read_stored_session` (915): hoist to constants or statement
   builders.

---

## Reviewed and found compliant (no action)

- **Validation on both write and read paths** (`insert_inode` pre-write,
  `validate_inode_row` post-read): matches the design mandate that Rust owns
  domain validation on reads and writes; persistence across processes justifies
  the double check. Raw SQLite extraction remains isolated in
  `InodeRow::from_rusqlite_row`.
- **Mutex-serialized single connection, rollback-journal mode, foreign keys,
  separate read-only inspection connection** (23-28, 74-106, 1008-1028): conforms
  to the process-model section of the design.
- **Error propagation discipline**: `map_err` attaches operation labels and
  stable identifiers (`db_error`, path-bearing contexts); lazy formatting via
  `with_context` closures is respected.
- **Tests exercise behavior through repository APIs**, cover concurrency,
  atomicity of rejected batches, and idempotent reloads. Internal-state
  assertions (`directory_loaded`) go through the store API.
- **rusqlite/SQLite row types do not escape the session module**: `Inode`,
  `SessionMetadata` are plain domain structs.

---

## Suggested sequencing

1. Quick wins: P10 sweep for remaining helpers, P11.1 / P11.6.
2. Structural: P5 → P4 → P6 → P8.
3. Follow-up: P9 (`ProposedInode`).
4. Optional hardening: P7's remaining positional column-order risk.
