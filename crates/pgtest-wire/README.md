# pgtest-wire

Handles PostgreSQL client connections over TCP and Unix sockets and supports
pgtest control commands. Application connections use `template/lease-id` as the
database name; pgtest-wire routes each connection to the PostgreSQL database
assigned to that lease and forwards its SQL there.

For application connections, the proxy rewrites the startup database name and
preserves the client's protocol version and other parameters. It then relays
bytes immediately, including authentication, protocol negotiation, startup errors,
and session parameters. PostgreSQL authenticates the application user directly;
the proxy does not synthesize authentication success or wait for `ReadyForQuery`.
Releasing a lease also closes connections that are still authenticating.

With profiling enabled, `postgres_upstream::connect` covers socket connection and
startup submission. Waiting for PostgreSQL's startup response is part of
`session_relay::run`, so connect timing is not the complete client startup latency.

## Accepted SQL commands

Connect to the virtual `pgtest` database to run these control commands:

| SQL | Result |
| --- | --- |
| `SELECT 1;` | Returns the integer `1`. |
| `SELECT pgtest_release('test-42');` | Releases the lease and returns `true` on success. |
| `SELECT pgtest_release($1);` | Releases the lease supplied as a bound text parameter and returns `true` on success. |
| `SELECT pgtest_release($1::text);` | Same, with an explicit text cast. |

Bound parameters require the extended query protocol, as used by client prepared
statements. Keywords and the function name are case-insensitive, whitespace is
allowed between tokens, and the trailing semicolon is optional. Escape single
quotes inside a lease ID by doubling them, for example `'suite''s test'`.

Other SQL and multiple statements in one query are unsupported on the control
database. Application connections forward SQL to their leased PostgreSQL database.
