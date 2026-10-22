# pgtest-wire-proxy

Handles PostgreSQL client connections over TCP and Unix sockets and supports
pgtest control commands. Application connections use `template/lease-id` as the
database name; the proxy routes each connection to the PostgreSQL database
assigned to that lease and forwards its SQL there.

One Pingora-owned work-stealing runtime hosts creation, cleanup, and all
application/control sessions. `PGTEST_CONNECTION_WORKERS` sets its worker count.
Database clients initialize on this runtime before the listener starts accepting
connections. Failed or cancelled initialization stops the host and closes listeners.

`ProxyServer::run_until` runs Pingora on a synchronous caller's thread and polls
its shutdown future on Pingora's main-loop runtime. The executable uses this
path after dropping its temporary PostgreSQL bootstrap runtime. This is the sole
production hosting entry point; it returns after Pingora shuts down its runtime.
Async hosting helpers are private to the test harness.

For application connections, the proxy rewrites the startup database name and
preserves the client's protocol version and other parameters. It then relays
bytes immediately, including authentication, protocol negotiation, startup errors,
and session parameters. PostgreSQL authenticates the application user directly;
the proxy does not synthesize authentication success or wait for `ReadyForQuery`.
Releasing a lease also closes connections that are still authenticating.

With profiling enabled, `upstream::connect` covers socket connection and
startup submission. Waiting for PostgreSQL's startup response is part of
`relay::run`, so connect timing is not the complete client startup latency.

## Pingora structure

This diagram shows pgtest's Pingora integration. `SharedService` is the single
registered Pingora service. Creation and cleanup are the two async futures stored
in `lifecycle_tasks`; connection sessions are spawned as additional async tasks.
All of them execute on the same service runtime's worker threads.

```mermaid
flowchart TB
    main["Synchronous main"] --> bootstrap["Temporary Tokio runtime<br/>Validate PostgreSQL and create initial inventory"]
    bootstrap -->|"Drop bootstrap runtime"| server["ProxyServer::run_until<br/>Pingora Server::run"]

    subgraph pingora["Pingora-owned runtimes"]
        subgraph supervisor["Main-loop runtime"]
            signal["RunSignal<br/>Await startup, shutdown request, or task failure"]
        end
        subgraph shared["Service runtime: pgtest-shared<br/>PGTEST_CONNECTION_WORKERS threads, work stealing"]
            service["SharedService::start_service"]
            creation["Creation task<br/>Database creation and lease assignment"]
            cleanup["Cleanup task<br/>Lease expiry, removal, and database deletion"]
            accept["ConnectionService::run<br/>Accept TCP / Unix connections"]
            sessions["Session tasks<br/>Application relay or control commands"]
            leases["LeaseService<br/>Registry reads and lifecycle requests"]
            service -->|"Spawn lifecycle_tasks"| creation
            service -->|"Spawn lifecycle_tasks"| cleanup
            service -->|"After both clients initialize"| accept
            accept -->|"Spawn per connection"| sessions
            sessions --> leases
            leases -->|"New assignment or pending release"| creation
            leases -->|"Assigned lease release"| cleanup
            creation -->|"Schedule expiry / retirement"| cleanup
        end
    end

    server -->|"Start service runtime"| service
    server -->|"Await signal"| signal
    accept -.->|"Startup result"| signal
    service -.->|"Initialization failure / cancellation"| signal
    os["Ctrl-C / SIGTERM"] --> signal
    signal -.->|"Graceful shutdown via Pingora"| service
    clients["PostgreSQL clients"] <-->|"TCP / Unix"| accept
    sessions <-->|"Application protocol bytes"| postgres["PostgreSQL"]
    creation -->|"Administrative SQL"| postgres
    cleanup -->|"Administrative SQL"| postgres
```

The accept loop hands each accepted stream to a session task. Application sessions
use a 64 KiB copy buffer per direction; accepted TCP sockets have `TCP_NODELAY` enabled.
Control sessions invoke lease operations rather than relaying SQL to PostgreSQL.

On shutdown, Pingora broadcasts its shutdown watch, sessions stop, and the
lifecycle cancellation token stops creation and cleanup. The host awaits task
cleanup and Pingora shuts down its runtimes before `run_until` returns to `main`.
Startup failures preserve the initialization error; unexpected lifecycle task
exit also requests server shutdown. A lease release acknowledges logical removal
without waiting for the physical database deletion.

Implementation: [services.rs](src/services.rs),
[connection_service.rs](src/connection_service.rs), and
[server main](../../apps/server/src/main.rs).

## Profiling

The existing Hotpath build features provide function, channel, and SQL timings.
Build with `just docker-build-profile` (musl), and stop gracefully to emit the report.
Elapsed async function times include waits and overlap across concurrent tasks;
their totals are not CPU time and can exceed wall time.
