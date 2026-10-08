use std::{
    net::{IpAddr, SocketAddr},
    num::{NonZeroU16, NonZeroUsize},
    path::PathBuf,
};

use anyhow::{Result, ensure};
use bpaf::{Bpaf, Parser, ShellComp};
use pgtest::worker_engine::core::WorkerEngineConfig;
use pgtest_database_operations::manager::config::PostgresConfig;

/// Run pgtest against an existing PostgreSQL instance.
#[derive(Clone, Debug, Bpaf)]
#[bpaf(options, generate(options), version(crate::version::display().as_str()))]
pub enum Command {
    /// Start the test database server.
    #[bpaf(command)]
    Serve(#[bpaf(external(serve_options))] ServeOptions),
    /// Show the compiled version and commit SHA.
    #[bpaf(command)]
    Version,
}

#[derive(Clone, Debug, Bpaf)]
#[bpaf(generate(parse_serve_options))]
pub struct ServeOptions {
    /// Upstream hostname, IP address, or Unix socket directory.
    #[bpaf(long, argument("HOST"), guard(|value| !value.is_empty(), "--pg-host cannot be empty"))]
    pub pg_host: String,
    /// Upstream PostgreSQL port (also used for its Unix socket filename).
    #[bpaf(long, argument("PORT"))]
    pub pg_port: NonZeroU16,
    /// Upstream PostgreSQL user.
    #[bpaf(long, argument("USER"), guard(|value| !value.is_empty(), "--pg-user cannot be empty"))]
    pub pg_user: String,
    /// Existing PostgreSQL template database to clone for tests.
    #[bpaf(long, argument("DATABASE"), guard(|value| !value.is_empty(), "--pg-database cannot be empty"))]
    pub pg_database: String,
    /// Enable TCP on this IP address, e.g. 127.0.0.1 or ::1.
    #[bpaf(long, argument("IP"), optional)]
    pub listen_addr: Option<IpAddr>,
    /// TCP port; defaults to 6432. Zero selects an available port.
    #[bpaf(long, argument("PORT"), optional)]
    pub listen_port: Option<u16>,
    /// Existing directory for the frontend Unix socket.
    #[bpaf(long, argument("DIR"), complete_shell(ShellComp::Dir { mask: None }), optional)]
    pub unix_socket_dir: Option<PathBuf>,
    /// Unix socket filename port; defaults to 6432, independent of TCP.
    #[bpaf(long, argument("PORT"), optional)]
    pub unix_socket_port: Option<NonZeroU16>,
    /// Maximum PostgreSQL connections used for database creation.
    #[bpaf(
        long,
        argument("COUNT"),
        fallback(NonZeroUsize::new(10).unwrap())
    )]
    pub creation_pool_connection: NonZeroUsize,
    /// Maximum PostgreSQL connections used for database cleanup.
    #[bpaf(
        long,
        argument("COUNT"),
        fallback(NonZeroUsize::new(5).unwrap())
    )]
    pub cleanup_pool_connection: NonZeroUsize,
    /// Initial number of ready test databases.
    #[bpaf(long, argument("COUNT"), fallback(16))]
    pub pool_initial_size: u16,
    /// Ready database threshold that triggers replenishment.
    #[bpaf(long, argument("COUNT"), fallback(8))]
    pub pool_starvation_threshold: u16,
    /// Number of databases per growth batch; zero disables growth.
    #[bpaf(long, argument("COUNT"), fallback(16))]
    pub pool_grow_batch_size: u16,
    /// Maximum lease lifetime in milliseconds.
    #[bpaf(long, argument("MS"), fallback(30000))]
    pub lease_claim_timeout_ms: u64,
    /// Maximum admitted lease IDs, including closed IDs.
    #[bpaf(
        long,
        argument("COUNT"),
        fallback(NonZeroUsize::new(100000).unwrap())
    )]
    pub max_lease_records: NonZeroUsize,
    /// Tracing filter; defaults to info. Does not read RUST_LOG.
    #[bpaf(long, argument("FILTER"), fallback(String::from("info")))]
    pub log_filter: String,
}

fn serve_options() -> impl Parser<ServeOptions> {
    parse_serve_options()
        .guard(
            |options| options.listen_addr.is_some() || options.unix_socket_dir.is_some(),
            "provide --listen-addr, --unix-socket-dir, or both",
        )
        .guard(
            |options| options.listen_port.is_none() || options.listen_addr.is_some(),
            "--listen-port requires --listen-addr",
        )
        .guard(
            |options| options.unix_socket_port.is_none() || options.unix_socket_dir.is_some(),
            "--unix-socket-port requires --unix-socket-dir",
        )
}

impl ServeOptions {
    pub fn tcp_address(&self) -> Option<SocketAddr> {
        self.listen_addr.map(|address| SocketAddr::new(address, self.listen_port.unwrap_or(6432)))
    }

    pub fn validate(&self) -> Result<()> {
        if let Some(directory) = &self.unix_socket_dir {
            ensure!(cfg!(unix), "Unix sockets are unsupported on this platform");
            ensure!(directory.is_dir(), "--unix-socket-dir must be an existing directory");
        }
        Ok(())
    }

    pub fn postgres_config(&self) -> PostgresConfig {
        PostgresConfig {
            pgtest_pg_host: self.pg_host.clone(),
            pgtest_pg_port: self.pg_port,
            pgtest_pg_user: self.pg_user.clone(),
            pgtest_pg_database: self.pg_database.clone(),
            pgtest_pg_creation_pool_connection: self.creation_pool_connection,
            pgtest_pg_cleanup_pool_connection: self.cleanup_pool_connection,
        }
    }

    pub fn engine_config(&self) -> WorkerEngineConfig {
        WorkerEngineConfig {
            initial_slots: self.pool_initial_size,
            starvation_threshold: self.pool_starvation_threshold,
            grow_batch_size: self.pool_grow_batch_size,
            lease_claim_timeout_ms: self.lease_claim_timeout_ms,
            max_lease_records: self.max_lease_records,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const UPSTREAM: &[&str] = &[
        "serve",
        "--pg-host",
        "localhost",
        "--pg-port",
        "5432",
        "--pg-user",
        "postgres",
        "--pg-database",
        "template",
    ];

    fn parse(extra: &[&str]) -> Result<ServeOptions, bpaf::ParseFailure> {
        let args = [UPSTREAM, extra].concat();
        options().run_inner(args.as_slice()).map(|command| match command {
            Command::Serve(value) => value,
            Command::Version => panic!("expected serve command"),
        })
    }

    #[test]
    fn listeners_are_explicit_and_can_be_combined() {
        assert!(parse(&[]).is_err());
        let tcp = parse(&["--listen-addr", "::1", "--listen-port", "0"]).unwrap();
        assert!(tcp.unix_socket_dir.is_none());
        assert_eq!(tcp.tcp_address().unwrap().port(), 0);
        let unix = parse(&["--unix-socket-dir", "/tmp", "--unix-socket-port", "7432"]).unwrap();
        assert!(unix.listen_addr.is_none());
        assert_eq!(unix.unix_socket_port.map(NonZeroU16::get), Some(7432));
        assert!(parse(&["--listen-addr", "127.0.0.1", "--unix-socket-dir", "/tmp"]).is_ok());
        assert!(parse(&["--listen-addr", "127.0.0.1", "--unix-socket-port", "7432"]).is_err());
        assert!(parse(&["--unix-socket-dir", "/tmp", "--listen-port", "7432"]).is_err());
        let tcp = parse(&["--listen-addr", "127.0.0.1", "--listen-port", "8432"]).unwrap();
        assert_eq!(tcp.tcp_address().unwrap(), "127.0.0.1:8432".parse().unwrap());
    }

    #[test]
    fn each_upstream_value_is_required() {
        for index in [1, 3, 5, 7] {
            let mut args = UPSTREAM.to_vec();
            args.drain(index..index + 2);
            args.extend(["--listen-addr", "127.0.0.1"]);
            assert!(options().run_inner(args.as_slice()).is_err());
        }
    }

    #[test]
    fn defaults_match_server_configuration() {
        let options = parse(&["--listen-addr", "127.0.0.1"]).unwrap();
        assert_eq!(options.tcp_address().unwrap().port(), 6432);
        let postgres = options.postgres_config();
        assert_eq!(postgres.pgtest_pg_host, "localhost");
        assert_eq!(postgres.pgtest_pg_port.get(), 5432);
        assert_eq!(postgres.pgtest_pg_user, "postgres");
        assert_eq!(postgres.pgtest_pg_database, "template");
        assert_eq!(postgres.pgtest_pg_creation_pool_connection.get(), 10);
        assert_eq!(postgres.pgtest_pg_cleanup_pool_connection.get(), 5);
        let engine = options.engine_config();
        assert_eq!(engine.initial_slots, 16);
        assert_eq!(engine.starvation_threshold, 8);
        assert_eq!(engine.grow_batch_size, 16);
        assert_eq!(engine.lease_claim_timeout_ms, 30000);
        assert_eq!(engine.max_lease_records.get(), 100000);
        assert_eq!(options.unix_socket_port.map_or(6432, NonZeroU16::get), 6432);
        assert_eq!(options.log_filter, "info");
    }

    #[test]
    fn overrides_reach_the_engine_and_postgres_configs() {
        let options = parse(&[
            "--unix-socket-dir",
            "/tmp",
            "--creation-pool-connection",
            "2",
            "--cleanup-pool-connection",
            "3",
            "--pool-initial-size",
            "4",
            "--pool-starvation-threshold",
            "5",
            "--pool-grow-batch-size",
            "0",
            "--lease-claim-timeout-ms",
            "60000",
            "--max-lease-records",
            "7",
            "--log-filter",
            "warn",
        ])
        .unwrap();
        let postgres = options.postgres_config();
        assert_eq!(postgres.pgtest_pg_creation_pool_connection.get(), 2);
        assert_eq!(postgres.pgtest_pg_cleanup_pool_connection.get(), 3);
        let engine = options.engine_config();
        assert_eq!(engine.initial_slots, 4);
        assert_eq!(engine.starvation_threshold, 5);
        assert_eq!(engine.grow_batch_size, 0);
        assert_eq!(engine.lease_claim_timeout_ms, 60000);
        assert_eq!(engine.max_lease_records.get(), 7);
        assert_eq!(options.log_filter, "warn");
    }

    #[test]
    fn invalid_values_are_rejected() {
        for (flag, value) in [
            ("--creation-pool-connection", "0"),
            ("--cleanup-pool-connection", "0"),
            ("--max-lease-records", "0"),
            ("--pool-initial-size", "65536"),
            ("--pool-grow-batch-size", "-1"),
            ("--unix-socket-port", "0"),
            ("--unix-socket-port", "65536"),
            ("--listen-addr", "localhost:6432"),
            ("--listen-addr", "127.0.0.1:65536"),
            ("--listen-addr", "[::1]:6432"),
        ] {
            assert!(parse(&["--unix-socket-dir", "/tmp", flag, value]).is_err(), "{flag}={value}");
        }
        for port in ["65536", "-1", "invalid", ""] {
            assert!(parse(&["--listen-addr", "127.0.0.1", "--listen-port", port]).is_err());
        }
        for (index, value) in [(2, ""), (4, "0"), (4, "65536"), (6, ""), (8, "")] {
            let mut args = UPSTREAM.to_vec();
            args[index] = value;
            args.extend(["--listen-addr", "127.0.0.1"]);
            assert!(options().run_inner(args.as_slice()).is_err());
        }
    }

    #[test]
    fn socket_directory_must_exist() {
        let directory = tempfile::tempdir().unwrap();
        let missing = directory.path().join("missing");
        let options = parse(&["--unix-socket-dir", missing.to_str().unwrap()]).unwrap();
        assert!(options.validate().is_err());
        assert!(!missing.exists());
    }

    #[test]
    fn help_and_version_do_not_require_server_arguments() {
        for args in [&["--help"][..], &["--version"][..], &["serve", "--help"][..]] {
            let text = options().run_inner(args).unwrap_err().unwrap_stdout();
            assert!(!text.is_empty());
        }
    }
}
