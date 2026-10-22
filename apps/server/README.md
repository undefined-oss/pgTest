# pgtest server

Contains the pgtest server executable, including environment configuration,
logging, startup of TCP and optional Unix socket listeners, and shutdown handling.

`pgtest-wire-proxy::ProxyServer` runs creation, cleanup, and connection tasks
on one Pingora-owned work-stealing runtime with one shared lease registry.
See the [Pingora structure diagram](../../crates/pgtest-wire-proxy/README.md#pingora-structure)
for runtime ownership, lifecycle tasks, and connection handling.

Before Pingora starts, application bootstrap validates PostgreSQL, cleans stale
databases, and creates the initial inventory on a temporary Tokio runtime. The
synchronous `main` drops that runtime before running Pingora directly on the main
thread. Readiness and shutdown signals are awaited on Pingora's main-loop runtime;
the executable needs no separate supervisor thread. Only database descriptors cross
into the services; each worker opens its own administrative client. Lease
registry entries are created when clients attach with a lease ID.

`PGTEST_CONNECTION_WORKERS=N` sets the shared runtime's worker count. It must be
positive and defaults to the available CPU parallelism reported by the OS.

Ctrl-C or SIGTERM stops the tasks and joins Pingora before the process exits.
The returning `Server::run` API allows socket cleanup and Hotpath report guards
to finish normally. Pingora's signal-handling main loop has its own runtime.

`PGTEST_LISTEN_ADDR` selects the TCP bind IP address (default `127.0.0.1`), and
`PGTEST_LISTEN_PORT` selects its port (default `6432`). Use an IP without a port,
such as `0.0.0.0` or `::1`. Port `0` selects an available port. Docker defaults to
`0.0.0.0` so published ports are reachable.

Set `PGTEST_UNIX_SOCKET_DIR` to an existing directory to enable the Unix listener.
`PGTEST_UNIX_SOCKET_PORT` selects its `.s.PGSQL.<port>` filename and defaults to
`6432`, independently of `PGTEST_LISTEN_PORT` and the upstream `PGTEST_PG_PORT`.
Valid values are `1`–`65535`; setting the port alone does not enable the listener.
Startup fails if the socket path already exists. Shutdown and failed or cancelled
startup remove the socket created by this process while preserving replacement files.
