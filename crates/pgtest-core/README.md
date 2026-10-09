# pgtest-core

The Manager actor owns leases and inventory in this crate. Creation and Cleanup
actors live in `pgtest-database-operations`; their synchronous state transitions
live in `pgtest-engine-backend` and are shared by the deterministic simulator and
Tokio workers. Mailboxes are unbounded; active provider operations are limited by
the configured creation and cleanup concurrency.

`pgtest-engine-backend` defines resource identities, connection targets, worker
messages, and synchronous scheduling states. It has no Tokio dependency.
The `tokio-runtime` feature enables PostgreSQL operations and concrete worker
handles. The CLI and server call `TokioRuntime::start(postgres_config, manager_config)`;
the wire receives a `ManagerHandle` and the configured template name. It validates
the startup database before sending an attach request, then uses the returned
`LeaseSession.target`. Manager receives lease IDs without template names.

Requests carry their reply handle directly into the pending waiter list. Tokio
uses a `oneshot::Sender`, with no request-ID counter or separate reply map. The
deterministic simulator uses test-only request IDs to inspect replies; `ManagerIO`
selects the reply handle type without adding Tokio to the synchronous message handler.

## Layout

```text
src/
├── lib.rs
├── config.rs
├── runtime.rs
├── manager_handle.rs
├── manager_handle/
│   ├── manager_worker.rs
│   ├── messages.rs
│   ├── database_inventory.rs
│   ├── lease.rs
│   ├── errors.rs
│   └── tests.rs
└── simulation.rs
```

Manager treats Creation and Cleanup as message endpoints. Their database-operation
implementations and connections stay in `pgtest-database-operations`; the runtime
constructs and connects them. `ManagerIO` is a statically dispatched testing seam
beside Manager, and `TokioPorts` is its crate-private production implementation.
There is no runtime selection or dynamic dispatch. `simulation.rs` is private
and compiled only for this crate's unit tests.

## Deterministic runtime

Disable default features to run the private deterministic simulator tests
without Tokio, an executor, futures, threads, or a wall clock:

```sh
cargo nextest run -p pgtest-core --no-default-features --features test-support
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
let request = runtime.attach("test-a");
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

`runtime.rs` owns initialization, worker wiring, and shutdown.
`manager_handle.rs` contains the caller-facing Tokio API.
`manager_handle/manager_worker.rs` contains the shared synchronous message handler,
its `ManagerIO` testing interface, and the private Tokio adapter and receive loop.
Lease types live in `manager_handle/lease.rs`; configuration lives in `config.rs`.
Runtime tests live in `runtime.rs` and deterministic behavior tests in
`manager_handle/tests.rs`.

`TokioRuntime::start(PostgresConfig, ManagerConfig)` bootstraps PostgreSQL,
initializes Creation and Cleanup concurrently through their concrete handle
constructors, awaits initial database creation, and then spawns Manager. Each
actor has its own unbounded inbox. Worker results, wire requests and timer messages go directly to Manager's
inbox. There is no additional completion channel or forwarding task.

Manager stores concrete `CreationHandle` and `CleanupHandle` values. The three
actors are independently spawned with `tokio::spawn`; `TaskTracker` is used only
for explicit shutdown. There is no supervisor, task-error aggregation, or automatic
sibling cancellation when a worker exits.

`runtime.handle()` returns a cloneable handle. Keep the runtime alive while
listeners serve connections. `runtime.shutdown().await` sends `Shutdown` to the
Manager inbox, cancels the shared token, and waits for actors and timers; it
returns `()`. Dropping the runtime also sends `Shutdown` and signals cancellation.
Manager processes its inbox with a receive loop. `ManagerHandle::stopped()` waits
for that inbox to close.

Creation processes batches in order with concurrent operations inside each
batch; Cleanup uses its own execution window. Each operation is attempted once,
and each result is reported separately. Failed cleanup retains retirement
identity and never returns a resource to available supply.

```sh
cargo nextest run -p pgtest-core --features runtime-tests
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

The runtime requests an initial batch through Creation's inbox and awaits its
results through the existing Manager inbox before spawning Manager with the
populated inventory. Manager has no startup tracking or readiness notification.
Startup returns immediately on the first creation error and cancels remaining
work. Successful resources remain for the next bootstrap. The deterministic
runtime simulates this startup phase before dispatching messages to Manager and
exposes its startup result directly.

Connections sharing an open lease share its database. Disconnecting keeps
the assignment; leases do not track connection counts. Explicit release removes
the current assignment, cancels its sessions, and acknowledges removal before
deletion finishes. Release is a no-op when no assignment exists, including while
an attach request is waiting for a database. Release and expiry both permit a fresh
assignment with the same ID. The claim-timeout setting still also
controls lease lifetime; zero disables both deadlines.

Creation and Cleanup discard queued work and drop active operation futures on
cancellation. Manager stops when it receives `Shutdown`, discarding later messages.
Already submitted provider commands may still finish remotely. Closed mailboxes return submission
errors; worker panics are not monitored or propagated by a supervisor.

Applications compose `runtime::TokioRuntime`, `config::ManagerConfig`, and
`manager_handle::ManagerHandle`. Per-database upstream connection pools,
profiling runs, Xata authentication, and TLS remain separate follow-up work.
