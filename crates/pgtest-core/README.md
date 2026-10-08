# pgtest-core

The Manager actor owns leases and inventory in this crate. Creation and Cleanup
actors live in `pgtest-database-operations`; their synchronous state transitions
live in `pgtest-engine-backend` and are shared by the deterministic simulator and
Tokio workers. Mailboxes are unbounded; active provider operations are limited by
the configured creation and cleanup concurrency.

`pgtest-engine-backend` defines resource identities, connection targets, worker
messages, and synchronous scheduling states. It has no Tokio dependency.
The `tokio-runtime` feature enables PostgreSQL operations and concrete worker
handles. The CLI and server call `TokioRuntime::start(postgres_config, engine_config)`;
the wire receives a `ManagerHandle` and uses each returned `LeaseSession.target`.

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
runtimes. PostgreSQL bootstrap and stale-database discovery are tested in
`pgtest-database-operations`, outside the Manager state machine.

## Tokio runtime

`TokioRuntime::start(PostgresConfig, WorkerEngineConfig)` bootstraps PostgreSQL,
initializes Creation and Cleanup through their concrete handle constructors,
and spawns Manager. Each actor has its own unbounded inbox. Worker results,
wire requests, session detaches, and timer messages go directly to Manager's
inbox. There is no additional completion channel or forwarding task.

Manager stores concrete `CreationHandle` and `CleanupHandle` values. The three
actors are independently spawned with `tokio::spawn`; `TaskTracker` is used only
for explicit shutdown. There is no supervisor, task-error aggregation, or automatic
sibling cancellation when a worker exits.

`runtime.handle()` returns a cloneable handle. Keep the runtime alive while
listeners serve connections. `runtime.shutdown().await` cancels the shared token
and waits for actors and timers; it returns `()`. Dropping the runtime signals
cancellation. `ManagerHandle::stopped()` waits for its own inbox to close.

Creation processes batches in order with concurrent operations inside each
batch; Cleanup uses its own execution window. Each operation is attempted once,
and each result is reported separately. Failed cleanup retains retirement
identity and never returns a resource to available supply.

```sh
cargo test -p pgtest-core --features runtime-tests
```

These tests cover all three actors with controlled clients, including reply
ownership, concurrency, timers, cancellation, and simulator parity. Helpers for
injecting clients are compiled only for tests or the explicit `test-support`
feature. PostgreSQL integration tests in `pgtest-wire` and
`pgtest-database-operations` require Docker.

## Lifecycle guarantees

Bootstrap validates PostgreSQL and removes stale databases on one temporary
connection, closes it, and returns configuration and metadata. Bootstrap cleanup
failures warn and permit startup to continue; validation failures stop startup.
Each handle initializes its independent pool before spawning its actor. A
constructor failure cancels and waits for any worker already started. Dropping
a pending startup future signals cancellation through the runtime owner.

Manager requests initial creation through Creation's inbox and waits for every
result. A partial creation failure fails startup and leaves successful resources
for the next bootstrap.

Connections sharing an open lease share its database. Last-session detach keeps
the assignment. Explicit release closes the identifier, cancels its sessions, and
acknowledges logical closure before deletion finishes. Expiry preserves the prior
behavior of permitting a fresh assignment. The claim-timeout setting still also
controls lease lifetime; zero disables both deadlines.

Shutdown discards queued work and drops active operation futures. Already submitted
provider commands may still finish remotely. Closed mailboxes return submission
errors; worker panics are not monitored or propagated by a supervisor.

The previous `WorkerEngineManager::start(PostgresConfig, ...)` interface and public
`pg_client` field have been replaced by explicit application composition,
`TokioRuntime`, and `ManagerHandle`. Per-database upstream connection pools,
profiling runs, Xata authentication, and TLS remain separate follow-up work.
