# pgtest CLI

Runs pgtest natively against an existing PostgreSQL instance. Configure it with
arguments; `PGTEST_*` variables and `RUST_LOG` do not configure this executable.
PostgreSQL must already contain the template database and permit the existing
pgtest authentication setup: leased sessions require trust authentication, while
database management uses the supplied user and the existing `postgres` password.
The user needs permission to create and drop databases. Upstream TLS is not supported.

## Build and run

```sh
just cli::build-release dev dev
./target/release/pgtest --help
./target/release/pgtest serve --help
```

`pgtest version` (also `--version` or `-V`) prints the compiled version and commit
SHA. The CLI build recipes and the root workspace `build` recipe require a
nonempty version and commit SHA, which they pass as `PGTEST_VERSION` and
`PGTEST_COMMIT_SHA` to the compiler:

```sh
just cli::build-release 0.1.0 "$(git rev-parse HEAD)"
./target/release/pgtest version
```

Use `just cli::build dev dev` or `just build dev dev` for development. Direct Cargo
builds default each value to `dev` when its environment variable is unset or
empty. Changing these variables at runtime does not change the reported metadata.

The upstream host, user, and database arguments are required; `--pg-port` defaults to `5432`. Choose TCP, a Unix socket, or both:

```sh
# TCP only
pgtest serve --pg-host localhost --pg-port 5432 --pg-user postgres \
  --pg-database template --listen-addr 127.0.0.1 --listen-port 6432

# Unix socket only (Linux/macOS); the directory must already exist
mkdir -p /tmp/pgtest
pgtest serve --pg-host localhost --pg-port 5432 --pg-user postgres \
  --pg-database template --unix-socket-dir /tmp/pgtest

# Both listeners, with an independent Unix socket port
pgtest serve --pg-host /var/run/postgresql --pg-port 5432 --pg-user postgres \
  --pg-database template --listen-addr 127.0.0.1 --listen-port 6432 \
  --unix-socket-dir /tmp/pgtest --unix-socket-port 7432
```

The examples using `pgtest` assume the executable is on your `PATH`. Application
connections use `template/<lease-id>` as the database name and are routed to that
lease's database. Connect to `pgtest` for control commands such as
`SELECT pgtest_release('test-42');`.

The Unix socket is `<directory>/.s.PGSQL.<port>`; its port defaults to `6432` and
does not follow `--listen-port`. `--unix-socket-port` requires `--unix-socket-dir`.
`--listen-addr` accepts an IPv4 or IPv6 address without a port (for example,
`127.0.0.1` or `::1`). `--listen-port` defaults to `6432` and requires
`--listen-addr`; port `0` chooses an available port, printed in the startup log.
Ctrl-C and SIGTERM close client sessions and stop the engine. Shutdown removes
the owned Unix socket; startup never overwrites an existing file or socket.

## Optional settings

| Argument | Default |
| --- | --- |
| `--creation-pool-connection` | `10` |
| `--cleanup-pool-connection` | `5` |
| `--pool-initial-size` | `16` |
| `--pool-starvation-threshold` | `8` |
| `--pool-grow-batch-size` | `16` |
| `--lease-claim-timeout-ms` | `30000` |
| `--log-filter` | `info` |

For example, append `--pool-initial-size 32 --creation-pool-connection 8
--log-filter debug` to a `serve` command. Zero growth batch size disables growth.
Pool connection counts must be greater than zero.

Profiling is off by default. Build with `--features hotpath` to enable it;
`hotpath-alloc` and `hotpath-prometheus` are also available alongside `hotpath`.
Hotpath's own environment controls, such as `HOTPATH_OUTPUT_FORMAT=json`, still apply.
The SQL report includes `tokio-postgres` query timings, with generated database
names grouped under stable CREATE/DROP labels. Pool waits are measured separately
by the database manager's acquisition functions.

The channel report includes `manager`, `creation`, and `cleanup`, with sent and
received message counts, current/peak queue depth, and queue latency. Set
`HOTPATH_REPORT=channels HOTPATH_OUTPUT_FORMAT=json` to capture only this report.
The same controls work for the server built with `--features hotpath`.
Creation counts are batch requests; each completed request returns one message
containing every database's success or failure. Results wait for the full batch,
including its slowest creation. The manager total counts these batch completions,
plus attach, release, expiry, and shutdown messages. Attaches served directly
from the shared lease map add no messages.
Instrumentation wraps the existing endpoints without forwarding tasks and is
disabled in builds without Hotpath.

The function report measures the papaya read path separately as
`manager_handle::lease_lookup`, including pinning, lookup, and cloning a published
session. Both hits and misses count. Use `HOTPATH_REPORT=functions,channels` to
see lookup timings alongside queue message counts.

Creation batch processing has separate function timings: `handle_creation_results`
for the full manager handler, `settle_creation_results` for updating inventory,
`dispatch_waiters` for lease assignment, and `grow` for replenishment decisions.
`creation_worker::send_results` measures enqueueing the completed batch; channel
latency measures the subsequent wait until receipt. The results vector moves
through the channel without cloning its contents. These timings are nested;
the full handler already includes settlement, dispatch, and growth.

## Shell completion

Completion uses bpaf's built-in shell support and works without PostgreSQL.
It suggests commands and flags, and directories for `--unix-socket-dir`.

For Bash, load bash-completion, then add this to your shell configuration:

```sh
source <(pgtest --bpaf-complete-style-bash)
```

For Zsh, generate a completion file in a directory on `fpath`, then initialize
completion in `.zshrc`:

```sh
mkdir -p ~/.zsh/completions
pgtest --bpaf-complete-style-zsh > ~/.zsh/completions/_pgtest
# Add these lines to .zshrc, placing fpath before any existing compinit call:
fpath=(~/.zsh/completions $fpath)
autoload -Uz compinit
compinit
```

For Fish:

```fish
mkdir -p ~/.config/fish/completions
pgtest --bpaf-complete-style-fish > ~/.config/fish/completions/pgtest.fish
```
