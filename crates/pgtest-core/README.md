# pgtest-core

Database lifecycle management is implemented by three actors. The Manager owns
leases and inventory, Creation processes requested counts, and Cleanup deletes
specified resources. Their state transitions are synchronous and shared by two
runtimes. Mailboxes are unbounded; active provider operations are limited by the
configured creation and cleanup concurrency.

`pgtest-engine-backend` defines resource identities, PostgreSQL connection targets,
and `AsyncDatabaseBackend`. Core has no dependency on the PostgreSQL implementation.
The CLI and server compose `PreparedPostgres` from `pgtest-database-operations` with
`TokioRuntime`. The wire receives a `ManagerHandle` and connects using each returned
`LeaseSession.target`.

## Deterministic runtime

Enable `test-support` and disable default features to use `simulation::SimRuntime`
without Tokio, an executor, futures, threads, or a wall clock:

```sh
cargo test -p pgtest-core --no-default-features --features test-support
cargo tree -p pgtest-core --no-default-features --features test-support
```

The driver has FIFO actor mailboxes. `step(ActorId)` delivers one message;
`run_until_idle(max_steps)` uses fixed round-robin scheduling and returns a trace
on step-limit exhaustion. Neither operation advances time or completes provider
calls. `advance_by` advances virtual time, expires caller deadlines, and enqueues
lease timers ordered by deadline and insertion sequence.

```rust,ignore
let mut runtime = SimRuntime::new(config);
runtime.run_until_idle(100)?;
for id in runtime.active_creations() {
    runtime.complete_creation(id, Ok(database_fixture(id)));
}
runtime.run_until_idle(100)?;
let request = runtime.attach("template", "test-a");
runtime.run_until_idle(100)?;
let reply = runtime.reply(request);
```

Tests explicitly complete active creations and deletions, discard requests or
sessions, inject stale messages, fail submissions, close actor mailboxes, and
inspect inventory. Quiescence does not imply startup or pending operations have
completed. The same shared worker transitions enforce execution windows in both
runtimes. Cancellation and restart scenarios can supply discovered stale resources
without assuming that interrupted remote operations rolled back.

## Tokio runtime

The default `tokio-runtime` feature enables production tasks and handles.
`TokioRuntime::start(Arc<B>, RuntimeConfig)` accepts any `AsyncDatabaseBackend`.
`runtime.handle()` returns a cloneable handle; `runtime.shutdown().await` cancels
and joins the actors and reports task failures. Dropping the runtime also signals
cancellation. Keep the runtime owner alive for the lifetime of the listeners.

Worker handles use unbounded Tokio channels; request replies use oneshots. Actor
logic has no Tokio types. Runtime ports translate logical session cancellation
into cancellation tokens and logical deadlines into monotonic Tokio timers.
Creation processes batches sequentially with concurrent operations inside each
batch; Cleanup uses a separate execution window. Provider calls are attempted
once, with each result reported separately. Failed cleanup retains retirement
identity and never returns a resource to available supply.

```sh
cargo test -p pgtest-core --features runtime-tests
```

These tests use a controlled async backend to exercise actual tasks, cancellation,
timers, worker failure, and reply ownership. PostgreSQL integration tests live in
`pgtest-wire` and `pgtest-database-operations` and require Docker.

## Lifecycle guarantees

Startup delegates discovered stale-resource deletion and initial creation to the
workers. Cleanup failures warn and permit startup to continue. Initial creation
waits for every result; a partial failure fails startup and leaves successful
resources for the next startup reconciliation.

Connections sharing an open lease share its database. Last-session detach keeps
the assignment. Explicit release closes the identifier, cancels its sessions, and
acknowledges logical closure before deletion finishes. Expiry preserves the prior
behavior of permitting a fresh assignment. The claim-timeout setting still also
controls lease lifetime; zero disables both deadlines.

Shutdown discards queued work and drops active operation futures. Already submitted
provider commands may still finish remotely. Actor failure stops the runtime;
callers receive unavailability instead of waiting indefinitely.

The previous `WorkerEngineManager::start(PostgresConfig, ...)` interface and public
`pg_client` field have been replaced by explicit application composition,
`TokioRuntime`, and `ManagerHandle`. Per-database upstream connection pools,
profiling runs, Xata authentication, and TLS remain separate follow-up work.
