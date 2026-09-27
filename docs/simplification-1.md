# Simplification 1: Detach resources on shutdown

## Purpose

Address [simplification #1](simplifications.md#1-give-daemon-teardown-one-owner-and-one-completion-condition)
with the smallest correct model:

- The control service owns every daemon resource in one `Option`.
- A `Shutdown` RPC (renamed from `Unmount`) detaches the resources: it takes
  them out of the option and hands them to `serve`. It returns as soon as the
  handoff is done. From then on, every request gets `unavailable`.
- `serve` releases the detached resources inline, then gracefully stops tonic.
  Signals and server exit detach and release the same way.
- If release fails, the daemon's state is undefined. `rfsd` exits with an
  error instead of retrying or recovering.

`rfs unmount` keeps its user-facing guarantee. After the RPC returns, the CLI
waits for the daemon process to exit. It reports success only if the retained
session is `closed`. The daemon no longer promises that the RPC response
means release has finished.

The technical design already describes `Session::close` as one-shot. Taking
the resources out of the option is what prevents a second close attempt.

### What this replaces

The first implementation of this plan built release tasks, a task-registration
channel, a drain guard, an external-trigger abstraction, and test hooks inside
production types. It also cleaned up partial startup through the same resource
owner. This model removes all of that:

| Removed                                                                                      | Why it is no longer needed                                                                             |
| -------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| Spawned release task and supervising it                                                      | `serve` is the daemon's main task and cannot be cancelled by a client, so it releases inline.          |
| RPC waiting for the release result                                                           | The RPC only detaches the resources. The CLI checks the outcome after the daemon exits.                |
| `Option` fields and partial-startup tracking in `DaemonResources`                            | The value exists only once every resource has been acquired. Startup failure relies on `Drop` (below). |
| First-error bookkeeping and secondary-error logging in release                               | Release stops at the first failure, and the daemon exits.                                              |
| `serve_with_external_trigger`, `ExternalTrigger`, `TestTransport`, `ReleaseSupervisorClosed` | `serve` waits on the handoff channel, signals, and the server directly.                                |
| Session drain guard                                                                          | Nothing uses the session after release.                                                                |
| `test_*` fields, `panic_release_task`, `wait_for_test_drain`, injected `configure` closure   | No fault injection in production types. See [Verification](#verification).                             |
| `complete_construction_stage`, `release_after_startup_failure`                               | Startup failure drops what it acquired and returns the error.                                          |
| The `rusqlite` lifecycle probe in the e2e test                                               | Retained inspection reports whether the session is closed.                                             |

## Construction in `rfsd` (`lib.rs`)

```rust
/// Constructs all daemon dependencies. Returns the complete resources and the
/// bound control listener. On failure, drops anything acquired and returns the
/// startup error.
async fn construct_daemon(cli: Cli) -> anyhow::Result<(DaemonResources, UnixListener)>;

/// Installs signal handlers, constructs the daemon, and serves it until release.
pub async fn run(cli: Cli) -> anyhow::Result<()>;
```

`run` installs SIGINT and SIGTERM handlers before constructing anything, so a
failed installation has nothing to clean up. It then calls `construct_daemon`
and passes the result and the signals to `serve`.

`construct_daemon` works through these steps in order:

1. Resolve configuration and connect to CAS.
2. Open the session and initialize logging.
3. Construct the `FilesystemService` and validate its root.
4. Mount FUSE.
5. Bind the control socket, then restrict its permissions.

It builds `DaemonResources` only after every step succeeds.

A startup failure returns its error using `?` with context:

- `FuseMount` unmounts and joins when dropped (`BackgroundSession` drop).
- Dropping the `Session` releases the home lock.
- If setting permissions fails after a successful bind, remove the socket
  that was just created before returning. A failed bind never removes an
  existing path.

A failed startup does not mark the session `closed`. Its retained state looks
like an unclean exit. The design already handles that: the next `Session::open`
replaces the session tree unconditionally. This departs from the original
recommendation that startup failure should share release logic. It follows
the same rule as a failed runtime release: a daemon that cannot finish cleanly
exits and leaves retained state for inspection.

## `DaemonResources`

```rust
/// Resources owned by one fully constructed daemon.
pub(crate) struct DaemonResources {
    /// Session state and the home ownership lock.
    session: Arc<Session>,
    /// Live FUSE mount.
    mount: FuseMount,
    /// Control socket path created by this daemon's bind.
    socket: PathBuf,
}

impl DaemonResources {
    /// Unmounts FUSE, durably closes the session, and removes the socket, in
    /// that order. Stops at the first failure; the remaining resources are
    /// dropped and the caller must treat daemon state as undefined.
    pub(crate) async fn release(self) -> Result<(), ControlError>;
}
```

`release` consumes `self`, so it can run only once. Its steps are:

1. Join FUSE in `spawn_blocking` (`mount.unmount()`).
2. Close the session in `spawn_blocking` (`session.close()`).
3. Remove the socket, treating `NotFound` as success.

Each step uses `?`. A `JoinError` from a blocking step becomes
`ControlError::Task`. A panic in FUSE teardown or session close is therefore
an ordinary release error. Log `daemon resource release completed` on success.

## Control service

```rust
#[derive(Clone)]
struct ControlService {
    /// `Some` until shutdown detaches it; `None` makes every endpoint unavailable.
    resources: Arc<std::sync::Mutex<Option<DaemonResources>>>,
    /// Hands detached resources to `serve`; capacity 1.
    detached: mpsc::Sender<DaemonResources>,
}

impl ControlService {
    /// Takes the resources and returns them, or `None` if already detached.
    fn detach(&self) -> Option<DaemonResources>;
}
```

The mutex is never held across an `.await`, so a `std::sync::Mutex` is enough.
Recover from poisoning with `PoisonError::into_inner`, since no code under the
lock can leave the option half-updated.

Endpoints:

- `check_protocol`, `status`, `snapshot`: lock and return `Status::unavailable`
  if the option is `None`. Then validate the protocol version. With resources
  present, behavior is unchanged: `status` reads `session.info()`, and
  `snapshot` returns `unimplemented`.
- `shutdown`: runs the following steps under one lock, with no `.await`:
  1. Return unavailable if the option is `None`.
  2. Validate the version. An incompatible request never detaches anything.
  3. `take()` the resources.
  4. `try_send` them on `detached`.

  Then return `ShutdownResponse`. The send cannot fail: only one caller can
  take the resources, the channel has capacity 1, and `serve` holds the
  receiver.

Taking and sending under one lock matters. When `serve` finds the option
`None`, the resources are guaranteed to be either released already or waiting
in the channel. The handler has no `.await` between take and send, so client
cancellation cannot lose the resources.

## `serve`

```rust
/// Serves control RPCs until resources are detached, releases them, then
/// drains tonic. Returns a transport failure if the server failed first;
/// otherwise returns the release result.
pub(crate) async fn serve(
    resources: DaemonResources,
    listener: UnixListener,
    signals: TerminationSignals,
) -> Result<(), ControlError>;
```

`serve` builds the service and starts tonic with
`serve_with_incoming_shutdown`, using a `oneshot` as the shutdown future. It
waits for the first of three events and ends up holding the detached
resources:

```rust
let (resources, transport_error) = tokio::select! {
    Some(resources) = detached_rx.recv() => (resources, None),     // Shutdown RPC
    _ = signals.recv() => (detach_or_receive(..).await, None),
    result = &mut server => (detach_or_receive(..).await, result.err()),
};
let release_result = resources.release().await;
```

`detach_or_receive` calls `service.detach()`. If a `Shutdown` RPC got there
first, it receives the resources from `detached_rx` instead. Signal and
server-exit shutdown skip protocol validation.

After release, `serve` sends the shutdown `oneshot`. If the server has not
already finished, `serve` awaits it. Tonic's graceful shutdown finishes
in-flight requests, including the `Shutdown` response, before it returns.
Unlinking the socket does not close established connections. While release
runs, new requests on open connections see `None` and return unavailable.

Result: if the server failed, return the transport error and log any release
error with its source. Otherwise return the release result. `run` wraps it
with context, so a failed release gives `rfsd` a non-zero exit.

## Control protocol

Rename `rpc Unmount(UnmountRequest) returns (UnmountResponse)` to
`rpc Shutdown(ShutdownRequest) returns (ShutdownResponse)` in
`proto/remotefs/control/v1/control.proto`. The response confirms that the
daemon accepted the shutdown request, not that release finished. Keep
`PROTOCOL_VERSION` as 1 because we're still implementing remotefs.

## CLI: `rfs unmount`

The command and its output stay the same. `DaemonClient::unmount` becomes
`shutdown`. `run_unmount` does the following:

1. Call `status` for the mountpoint and `daemon_pid`, as it does today.
2. Call `shutdown`.
3. Wait for `daemon_pid` to exit. Poll `kill(pid, 0)` until it reports
   `ESRCH`, with a timeout of 30 seconds.
4. Call `Session::inspect`. Print `unmounted <mountpoint>` only if the
   retained session is closed.
5. Otherwise fail. On timeout, say the daemon is still shutting down.
   Otherwise, say it exited without closing its session. In both cases, name
   the daemon log path from `SessionInfo::log_path`.

This preserves the design rule "graceful session close before a successful
unmount response" at the CLI instead of the RPC.

`SessionInfo` gains `closed: bool`. `Session::inspect` fills it from the stored
lifecycle. The active daemon's `Session::info` sets it to `false`. This is the
only session facade change. It replaces the direct SQLite probe in the e2e
test.

## Errors

`ControlError` keeps `Socket { path, source }`, `Transport`, `Signal`,
`State(SessionError)`, and `Task(JoinError)`, and adds
`Context { operation, source }` so release steps can use the `error_context`
helpers instead of a raw `?`. Remove `ResourceLockPoisoned`,
`ReleaseSupervisorClosed`, and `TestTransport`.

Do not repeat the source in `#[error]` strings (for example, use
`#[error("close daemon state")]` with `#[source]`). This keeps `anyhow`'s
`{:#}` output free of duplicated messages. Log errors with a chain-aware
formatter, such as `error = ?anyhow::Error::from(error)`.

## Documentation updates

In `technical-design.md` (Process Model), make these changes:

- Replace "Graceful session close before a successful unmount response" with
  a daemon-side rule: the daemon closes the session before exiting
  successfully.
- Add the CLI-side rule: `rfs unmount` succeeds only after the daemon exits
  and retained state is `closed`.
- Mention the `Shutdown` control RPC wherever the control RPCs are listed.

## Verification

Shutdown is covered by one end-to-end test, with no hooks, fakes, or
fault injection. It reuses the existing mount workflow in
`tests/readonly_e2e.rs`, which already mounts FUSE against a local CAS and
ends with `rfs unmount`. After `rfs unmount` returns success, the test checks
that:

- the daemon process has exited,
- `Session::inspect` reports the retained session as closed,
- the socket path is absent.

Remove every unit test added for the earlier scaffolding in `control_service.rs`
and `lib.rs`, along with the unused `tower` dependency and the `rusqlite` dev-dependency. Signal
handling, release failure, startup cleanup, and the CLI failure messages are
short straight-line code, verified by inspection.

No schema or FUSE API changes are required. Writable operations and snapshot
coordination remain separate work.

## Open issue: daemon stderr after `rfs mount` returns

This is an existing issue. The simplification did not cause it and does not fix it.

`rfs mount` starts `rfsd` with stderr as a pipe. It reads that pipe only if
the daemon exits before it becomes ready. Once the mount is ready, `rfs mount`
exits and the read end of the pipe closes. The daemon keeps running with a
stderr that no one reads.

If `rfsd` later exits with an error, for example because release failed, `main`
prints the error with `eprintln!`. Rust ignores `SIGPIPE`, so the write fails
with `EPIPE` and `eprintln!` panics. The process then exits with status 101
instead of 1. The error message is lost, and so is the panic message, since
it also goes to stderr.

Current mitigation: `run` also logs a shutdown failure to the session log with
its full error chain before returning. `rfs unmount` names that log when it
fails. Startup failures are not affected, because `rfs mount` is still reading
stderr at that point.

Options to explore:

- Once ready, have `rfs mount` point the daemon's stderr at the session log or
  `/dev/null`. This needs the daemon to reopen stderr itself, because the CLI
  cannot change the child's descriptors after spawn.
- Have `rfsd` redirect its own stderr to the session log (`dup2`) once logging
  is initialized.
- In `main`, write the final error with `writeln!(io::stderr(), ..)` and ignore
  the write error instead of calling `eprintln!`.
