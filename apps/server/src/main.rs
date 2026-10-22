#[cfg(not(feature = "hotpath-alloc"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::{
    net::{IpAddr, SocketAddr, TcpListener},
    num::{NonZeroU16, NonZeroUsize},
    path::PathBuf,
};

use anyhow::{Context, Result, ensure};
use envconfig::Envconfig;
use pgtest::config::PoolConfig;
use pgtest_database_operations::{backend::PreparedPostgres, config::PostgresConfig};
use pgtest_wire_proxy::{Listeners, ProxyServer};
use tracing_subscriber::{EnvFilter, prelude::*};

#[derive(Envconfig)]
struct ServerConfig {
    #[envconfig(from = "PGTEST_LISTEN_ADDR", default = "127.0.0.1")]
    listen_addr: IpAddr,
    #[envconfig(from = "PGTEST_LISTEN_PORT", default = "6432")]
    listen_port: u16,
    #[envconfig(from = "PGTEST_UNIX_SOCKET_DIR")]
    unix_socket_dir: Option<PathBuf>,
    #[envconfig(from = "PGTEST_UNIX_SOCKET_PORT", default = "6432")]
    unix_socket_port: NonZeroU16,
    #[envconfig(from = "PGTEST_CONNECTION_WORKERS")]
    connection_workers: Option<NonZeroUsize>,
}

impl ServerConfig {
    fn workers(&self) -> NonZeroUsize {
        self.connection_workers
            .unwrap_or_else(|| std::thread::available_parallelism().unwrap_or(NonZeroUsize::MIN))
    }
}

#[hotpath::main(allocator = mimalloc::MiMalloc)]
fn main() -> Result<()> {
    let log_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let subscriber = tracing_subscriber::registry()
        // Filter console logs separately so SQL query events reach the profiler.
        .with(tracing_subscriber::fmt::layer().with_filter(log_filter));
    // The tokio-postgres adapter emits the SQL completion schema this layer
    // consumes.
    #[cfg(feature = "hotpath")]
    let subscriber = subscriber.with(pgtest_database_operations::sql_tracing_layer());
    subscriber.init();

    let server_config = ServerConfig::init_from_env().context("invalid server configuration")?;
    ensure!(
        cfg!(unix) || server_config.unix_socket_dir.is_none(),
        "Unix sockets are unsupported on this platform"
    );

    let postgres_config =
        PostgresConfig::init_from_env().context("invalid PostgreSQL configuration")?;
    let pool_config = PoolConfig::init_from_env().context("invalid database pool configuration")?;

    let workers = server_config.workers();
    let tcp =
        TcpListener::bind(SocketAddr::new(server_config.listen_addr, server_config.listen_port))
            .context("failed to bind TCP listener")?;
    let listeners = Listeners::new(tcp);
    #[cfg(unix)]
    let listeners = if let Some(directory) = &server_config.unix_socket_dir {
        listeners
            .bind_unix(directory, server_config.unix_socket_port)
            .context("failed to bind Unix listener")?
    } else {
        listeners
    };
    // Bootstrap clients live only on this temporary runtime. Pingora creates
    // and owns every runtime used once the server starts.
    let (prepared, initial_databases) = {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("failed to create PostgreSQL bootstrap runtime")?;
        runtime.block_on(async {
            let prepared = PreparedPostgres::prepare(postgres_config)
                .await
                .context("failed to prepare PostgreSQL")?;
            let initial_databases = prepared
                .create_initial_databases(usize::from(*pool_config.initial_slots))
                .await
                .context("failed to create initial databases")?;
            Ok::<_, anyhow::Error>((prepared, initial_databases))
        })?
    };
    ProxyServer::run_until(
        prepared,
        pool_config,
        initial_databases,
        listeners,
        workers,
        shutdown_signal(),
    )
    .context("Pingora server failed")
}

async fn shutdown_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, net::IpAddr, path::PathBuf};

    use super::*;

    #[test]
    fn default_listener_stays_on_loopback() {
        let config = ServerConfig::init_from_hashmap(&HashMap::new()).unwrap();
        assert_eq!(config.listen_addr, "127.0.0.1".parse::<IpAddr>().unwrap());
        assert_eq!(config.listen_port, 6432);
        assert_eq!(config.unix_socket_port.get(), 6432);
    }

    #[test]
    fn listener_accepts_explicit_ipv4_and_ipv6_addresses() {
        for address in ["0.0.0.0", "127.0.0.1", "::"] {
            let vars = HashMap::from([
                ("PGTEST_LISTEN_ADDR".to_owned(), address.to_owned()),
                ("PGTEST_LISTEN_PORT".to_owned(), "0".to_owned()),
            ]);
            let config = ServerConfig::init_from_hashmap(&vars).unwrap();
            assert_eq!(config.listen_addr, address.parse::<IpAddr>().unwrap());
            assert_eq!(config.listen_port, 0);
        }
    }

    #[test]
    fn invalid_listener_address_is_rejected() {
        for address in ["localhost", "127.0.0.1:6432", "[::]:6432", "[::]", ""] {
            let vars = HashMap::from([("PGTEST_LISTEN_ADDR".to_owned(), address.to_owned())]);
            assert!(ServerConfig::init_from_hashmap(&vars).is_err());
        }
    }

    #[test]
    fn unix_socket_port_is_independent_of_tcp_port() {
        let vars = HashMap::from([
            ("PGTEST_LISTEN_ADDR".to_owned(), "127.0.0.1".to_owned()),
            ("PGTEST_LISTEN_PORT".to_owned(), "8432".to_owned()),
            ("PGTEST_UNIX_SOCKET_DIR".to_owned(), "/tmp/pgtest".to_owned()),
            ("PGTEST_UNIX_SOCKET_PORT".to_owned(), "7432".to_owned()),
        ]);
        let config = ServerConfig::init_from_hashmap(&vars).unwrap();
        assert_eq!(config.listen_port, 8432);
        assert_eq!(config.unix_socket_port.get(), 7432);
        assert_eq!(config.unix_socket_dir, Some(PathBuf::from("/tmp/pgtest")));
    }

    #[test]
    fn invalid_tcp_ports_are_rejected() {
        for port in ["65536", "-1", "invalid", ""] {
            let vars = HashMap::from([("PGTEST_LISTEN_PORT".to_owned(), port.to_owned())]);
            assert!(ServerConfig::init_from_hashmap(&vars).is_err(), "accepted port {port:?}");
        }
    }

    #[test]
    fn invalid_unix_socket_ports_are_rejected() {
        for port in ["0", "65536", "-1", "invalid", ""] {
            let vars = HashMap::from([("PGTEST_UNIX_SOCKET_PORT".to_owned(), port.to_owned())]);
            assert!(ServerConfig::init_from_hashmap(&vars).is_err(), "accepted port {port:?}");
        }
    }

    #[test]
    fn connection_workers_default_to_available_parallelism_and_accept_override() {
        let config = ServerConfig::init_from_hashmap(&HashMap::new()).unwrap();
        assert_eq!(
            config.workers(),
            std::thread::available_parallelism().unwrap_or(NonZeroUsize::MIN)
        );
        let config = ServerConfig::init_from_hashmap(&HashMap::from([(
            "PGTEST_CONNECTION_WORKERS".to_owned(),
            "3".to_owned(),
        )]))
        .unwrap();
        assert_eq!(config.workers().get(), 3);
        for value in ["0", "-1", "invalid", ""] {
            let vars = HashMap::from([("PGTEST_CONNECTION_WORKERS".to_owned(), value.to_owned())]);
            assert!(ServerConfig::init_from_hashmap(&vars).is_err());
        }
    }
}
