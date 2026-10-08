# pgtest-database-operations

Contains PostgreSQL configuration, database creation and deletion operations,
Creation/Cleanup actors, database naming helpers, and PostgreSQL test support.

Shared PostgreSQL modules (`config`, `connection`, `database_name`, `errors`, and
`sql_profile`) live at the crate root. `backend` owns startup validation and
cleanup. The Manager actor lives in `pgtest-core`.

## Worker modules

Each worker keeps its handle, PostgreSQL client, and actor loop together:

```text
src/
├── creation_worker_handle.rs          # CreationHandle
├── creation_worker_handle/
│   ├── creation_pg_client.rs          # CreationClient
│   └── creation_worker.rs             # CreationActor loop
├── cleanup_worker_handle.rs           # CleanupHandle
└── cleanup_worker_handle/
    ├── cleanup_pg_client.rs           # CleanupClient
    └── cleanup_worker.rs              # CleanupActor loop
```

Rust imports use `creation_worker_handle::{CreationHandle, CreationClient}` and
`cleanup_worker_handle::{CleanupHandle, CleanupClient}`.

## Startup and ownership

The Tokio runtime owns initialization. It calls `PreparedPostgres::prepare` to
validate the server version and template and clean stale databases on one
temporary connection. Bootstrap closes that connection before returning
`PostgresConfig` and `PostgresMetadata`; it starts no workers. Cleanup failures
warn and continue; validation failures stop startup.

The runtime then concurrently calls `CreationHandle::new` and `CleanupHandle::new`.
Each constructor initializes its own PostgreSQL client, creates its inbox, spawns
`actor.run()` with `tokio::spawn`, and returns the concrete handle. Creation uses
`pgtest_pg_creation_pool_connection`; Cleanup uses
`pgtest_pg_cleanup_pool_connection`. These settings retain their pool-capacity and
operation-concurrency limits.

There are three actor inboxes:

| Inbox | Messages |
|---|---|
| Manager | Wire requests, session/timer messages, and worker results |
| Creation | Creation requests from Manager |
| Cleanup | Deletion requests from Manager |

Each worker receives a clone of Manager's inbox sender. Constructors accept
`UnboundedSender<ManagerMessage>` where
`ManagerMessage: From<DatabaseWorkerMessages> + Send + 'static`, so
workers can send results directly without depending on core's message envelope.
Actors declare `ManagerMessage` and `DatabaseClient` at `impl` scope. Handles
are nongeneric sender structs; their constructors declare the generic types
needed to initialize the actors. This uses static dispatch.

The handle/task split follows [Actors with Tokio](https://ryhl.io/blog/actors-with-tokio/).
Mailboxes remain unbounded. There is no supervisor, task wrapper, worker bundle,
factory layer, or boxed worker port. A shared cancellation token and `TaskTracker`
retain the original explicit shutdown behavior. A failed constructor cancels and
waits for already-started work; worker panics do not cancel sibling actors.

```rust,ignore
use pgtest::runtime::TokioRuntime;

let runtime = TokioRuntime::start(postgres_config, manager_config).await?;
let manager = runtime.handle(); // Pass this to the wire listener.
// Keep runtime alive while listeners serve connections.
runtime.shutdown().await;
```

Creation and Cleanup use synchronous scheduling states from
`pgtest-engine-backend`, shared with the deterministic runtime. The simulator
constructs these states directly and does not use Tokio or PostgreSQL clients.

## Tests

Bootstrap tests live in `backend.rs`. Operation tests live beside each PostgreSQL
client, and creation-handle lifecycle tests live in `creation_worker_handle.rs`.
`worker_tests.rs` contains the shared checks for independent pools, creation and
deletion together, and worker initialization failures. Shared test-client setup
is gated with `cfg(test)` in `testcontainer.rs`.

```sh
# Three Tokio actors with controlled clients; no Docker.
cargo nextest run -p pgtest-core --features runtime-tests runtime::tests
# Synchronous simulation without Tokio.
cargo nextest run -p pgtest-core --no-default-features --features test-support
# PostgreSQL operations and runtime initialization; requires Docker.
cargo nextest run -p pgtest-database-operations -p pgtest-wire
```

Mock-client constructors and bulk-operation helpers used exclusively by tests
are gated with `cfg(test)` or the explicit `test-support` feature. Normal
production builds exclude them. Runtime tests cover all three inboxes, direct
result delivery, reply ownership, timers, concurrency, explicit shutdown, and
simulator parity. Integration tests cover bootstrap connection closure,
independent pools, constructor failure, and cancelled initialization.

## SQL profiling

With `hotpath` enabled, `tokio-postgres` calls appear in Hotpath's SQL report.
This covers database listing, server-version queries, creation, and individual
drops, including pipelined cleanup and failed executions. Timings measure the
driver future after pool checkout, so they include network and PostgreSQL queue
time but exclude pool acquisition. Cancelled futures do not
emit a completion event.

DDL labels replace database and template identifiers with placeholders to group
leases into stable query buckets. Bound parameter values are not recorded.
The crate exposes `sql_tracing_layer` for the CLI, server, and profiling tests.
Hotpath 0.25 requires the `sqlx` feature and `sqlx::query` event target for this
collector. These compatibility names do not pull in the SQLx driver; operations
use tokio-postgres, and events identify it with `db.driver=tokio-postgres`.
Console filtering remains separate from the collector. With profiling disabled, the adapter does not read the clock or emit
events.
Profiling builds box each driver future to keep the startup stack bounded; that
allocation is visible in allocation profiles. Builds without profiling return
the original driver future directly.
