# pgtest-database-operations

PostgreSQL configuration, bootstrap, creation/deletion clients, database naming,
and integration-test support live here. Lifecycle workers and lease coordination
live in `pgtest-core`; this crate does not spawn creation or cleanup actors.

`creation_client::CreationClient` and `cleanup_client::CleanupClient` each
own a separate administrative connection pool. The Pingora background services
construct each client on its service runtime and poll its operations there.
Creation and cleanup pool settings apply once for the whole service.

`PreparedPostgres::prepare` validates the server version and template and cleans
stale databases on one temporary connection. It closes that connection before
returning configuration and metadata. Validation failures stop startup; bootstrap
cleanup failures warn and continue. Preparation runs once before lifecycle and
connection workers start accepting work.

The application then calls `prepared.create_initial_databases(initial_slots)`
before starting Pingora. This creates the initial supply with bounded concurrency
and waits for the whole batch, including failures. Its temporary client is
dropped before returning database descriptors for core's shared inventory.
Workers do not repeat initial creation. Later growth uses the creation worker's
own client on its Pingora runtime.

The clients implement the provider-neutral `DatabaseCreator` and
`DatabaseCleaner` traits from `pgtest-engine-backend`. Creation batch scheduling,
ready inventory, and publication belong to core's creation worker. Logical lease
removal, expiry, and physical retirement belong to core's cleanup worker. Backend
results are consumed directly by those workers, without a manager mailbox.

## Tests

PostgreSQL operation and bootstrap integration tests use isolated containers.
`backend::initial_tests` checks bootstrap batching and cancellation with a fake
provider and does not require Docker. Shared checks
in `client_tests.rs` verify independent administrative pool capacity and client
initialization failure. Controlled lifecycle tests live in
`pgtest-core` and do not require Docker.

```sh
cargo test -p pgtest-core --lib
# Requires Docker:
cargo test -p pgtest-database-operations -p pgtest-wire-proxy
```

## SQL profiling

With `hotpath` enabled, `tokio-postgres` calls appear in Hotpath's SQL report.
This covers database listing, server-version queries, creation, and individual
drops, including pipelined cleanup and failed executions. Timings measure the
driver future after pool checkout, so they include network and PostgreSQL queue
time but exclude pool acquisition. Cancelled futures do not
emit a completion event.

DDL labels replace database and template identifiers with placeholders to group
leases into stable query buckets. Bound parameter values are not recorded.
The crate exposes `sql_tracing_layer` for the server and profiling tests.
Hotpath 0.25 requires the `sqlx` feature and `sqlx::query` event target for this
collector. These compatibility names do not pull in the SQLx driver; operations
use tokio-postgres, and events identify it with `db.driver=tokio-postgres`.
Console filtering remains separate from the collector. With profiling disabled, the adapter does not read the clock or emit
events.
Profiling builds box each driver future to keep the startup stack bounded; that
allocation is visible in allocation profiles. Builds without profiling return
the original driver future directly.
