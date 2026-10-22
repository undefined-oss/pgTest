# pgtest-core

One creation worker and one cleanup worker serve every connection worker.
Each lifecycle worker owns its administrative PostgreSQL connection pool. The
Pingora host runs creation, cleanup, and connections on one shared runtime.
There is no
manager actor, lease-owner routing, or manager completion queue.

`LifecycleWorkers` supplies the creation and cleanup futures and a cloneable
`LeaseService` with `attach`, `release`, and `stopped`. This crate does not own
production threads or runtimes. PostgreSQL preparation and service hosting live
at the application boundary.

`PoolConfig` configures the shared database inventory. Pingora connection
workers use the `LeaseService` API for application and control requests.

## Hosting the workers

`LifecycleWorkers::new(pool_config, initial_databases, stop)` separates the lifecycle from its
thread launcher. It returns a shared `service` handle and two owned workers,
`creation` and `cleanup`, without starting threads or connecting to PostgreSQL.
It seeds the shared inventory with databases already created during application
bootstrap. The lease registry starts empty; clients supply lease IDs on attach.

Each worker's `run(initialize, concurrency_limit, ready_sender)` consumes that
worker. The initialization future builds its database client on the runtime polling the
worker. Both workers send readiness after their clients initialize and report startup errors through the
readiness channel. The host must wait for both successful readiness results
before admitting connections.

The host cancels the shared token to shut down. Once running, a worker exiting,
panicking, or having its future dropped also cancels that token to stop its
sibling and invalidate lease sessions. The host remains responsible for awaiting
both worker futures during shutdown.

The server application uses `ProxyServer`, which hosts creation and cleanup
through `ServiceBuilder` in
[`pgtest-wire-proxy/src/services.rs`](../pgtest-wire-proxy/src/services.rs).
Pingora runs creation, cleanup, and connection tasks on one work-stealing runtime.
Clients initialize there before connections are accepted. `PGTEST_CONNECTION_WORKERS`
sets the shared worker count. The host checks both initialization results before
reporting readiness and preserves the underlying error if startup fails.

Application bootstrap calls `prepared.create_initial_databases(initial_slots)`
before starting Pingora. It waits for the complete batch and transfers only
database descriptors; the temporary bootstrap client is dropped before worker
clients open on the shared runtime.

The synchronous server calls `ProxyServer::run_until` with a shutdown future.
Shutdown during startup cancels both lifecycle workers. Core tests run creation
and cleanup futures directly on the test runtime and explicitly await their
shutdown.

## Responsibilities

| Component | Owns |
|---|---|
| Application bootstrap | Initial database creation before Pingora starts |
| Creation worker | Ready inventory, pending requests and waiters, creation reservations, assignment, registry publication, replenishment |
| Cleanup worker | Release, expiry, registry removal, session cancellation, physical deletion, failed retirement records |
| Connection worker | Client/upstream sockets, startup parsing, waiting for assignment, byte relay |
| Papaya registry | Lease ID to completed, immutable assignment; no pending state or core-owner metadata |

Ready attaches read a published session without messaging a lifecycle worker.
Missing attaches send requests to creation, which groups waiters by lease ID in
its local state. Each caller awaits its own reply with an independent timeout.
Disconnecting or timing out closes that reply and wakes creation to prune
cancelled waiters. Other callers for the same lease remain pending.

Creation records a batch only after every operation finishes. It assigns ready
inventory by sending replies first. If at least one send succeeds, creation
publishes the complete immutable entry into Papaya, then removes pending tracking.
If every send fails, the database returns to available inventory without publication.
A successful send commits the assignment even if the receiver disconnects before
consuming it. Requests that miss publication queue behind dispatch and recheck the
registry. No pending entry, waiter count, or per-entry mutex exists in the registry.
Only creation can publish; only cleanup can remove. Cleanup conditionally
removes the exact assignment, so a delayed release or timer cannot affect a
replacement using the same lease ID. No map guard crosses an await.

## Release, expiry, and shutdown

Disconnecting does not release a published assignment. Explicit release closes
both pending and ready leases, cancels sessions, and acknowledges logical removal
before physical deletion finishes. Pending waiters receive `LeaseClosed`. Late
provisioning results remain available inventory rather than reviving the old lease.
Release of a published lease goes directly to cleanup. If the registry lookup
misses, release is ordered through creation alongside pending attaches. Creation
either cancels its pending waiters or forwards an assignment published in the
meantime to cleanup. An unknown lease is a no-op and does not trigger growth.
Reusing a released ID creates a new assignment.

`PGTEST_LEASE_CLAIM_TIMEOUT_MS` retains its existing dual meaning: each attach's
wait deadline and the lifetime starting at publication. Zero disables both.
All inventory and administrative pool settings apply once to the service,
not once per connection worker. Growth preserves batch sizing and the inclusive
starvation threshold. A creation batch becomes available after all its outcomes
arrive. Startup failure likewise waits for its full batch.

Cleanup has independent concurrency. Failed deletions retain their resource
identity and are not returned to ready inventory or automatically retried.
Unexpected worker exit cancels the service. Shutdown cancels pending operations
and sessions. The synchronous host returns after Pingora shuts down its runtime.
Cancelling a provider future does not guarantee that PostgreSQL cancelled a
command already submitted. The next bootstrap reconciles stale databases.

## Tests

Pingora owns the shared service runtime; the operating system schedules its
threads onto CPUs. Pool clients and connection sessions run on that runtime.

```sh
cargo test -p pgtest-core --lib
```

Controlled providers exercise worker tasks, concurrent connection
runtimes, readiness, cancellation, expiry, assignment races, and shutdown without
PostgreSQL. They also cover creation concurrency and whole-batch publication.
PostgreSQL integration tests require Docker.

With `hotpath`, the `creation` and `cleanup` request queues are profiled. There is
no manager queue. Ready joins produce no creation request.
