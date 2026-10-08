#[cfg(not(feature = "hotpath-alloc"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::{net::SocketAddr, sync::Arc};

use anyhow::{Context, Result, ensure};
use envconfig::Envconfig;
use pgtest::{worker_engine::core::WorkerEngineConfig, worker_manager::TokioRuntime};
use pgtest_database_operations::config::PostgresConfig;
use pgtest_pg_wire::{
    listener_addr::ListenAddr,
    listener_port::{SocketListenerPort, TCPListenerPort},
    unix_socket_dir::UnixSocketDir,
    wire_listener,
};
use tracing_subscriber::{EnvFilter, prelude::*};

#[derive(Envconfig)]
struct ServerConfig {
    #[envconfig(from = "PGTEST_LISTEN_ADDR", default = "127.0.0.1")]
    listen_addr: ListenAddr,
    #[envconfig(from = "PGTEST_LISTEN_PORT", default = "6432")]
    listen_port: TCPListenerPort,
    #[envconfig(from = "PGTEST_UNIX_SOCKET_DIR")]
    unix_socket_dir: Option<UnixSocketDir>,
    #[envconfig(from = "PGTEST_UNIX_SOCKET_PORT", default = "6432")]
    unix_socket_port: SocketListenerPort,
}

#[tokio::main]
#[hotpath::main(allocator = mimalloc::MiMalloc)]
async fn main() -> Result<()> {
    let log_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let subscriber = tracing_subscriber::registry()
        // Filter console logs separately so SQL query events reach the profiler.
        .with(tracing_subscriber::fmt::layer().with_filter(log_filter));
    // The tokio-postgres adapter emits the SQL completion schema this layer
    // consumes.
    #[cfg(feature = "hotpath")]
    let subscriber = subscriber.with(pgtest_database_operations::sql_tracing_layer());
    subscriber.init();
    hotpath::tokio_runtime!();

    let server_config = ServerConfig::init_from_env().context("invalid server configuration")?;
    ensure!(
        cfg!(unix) || server_config.unix_socket_dir.is_none(),
        "Unix sockets are unsupported on this platform"
    );

    let postgres_config =
        PostgresConfig::init_from_env().context("invalid PostgreSQL configuration")?;
    let worker_engine_config =
        WorkerEngineConfig::init_from_env().context("invalid worker engine configuration")?;

    let runtime = TokioRuntime::start(postgres_config, worker_engine_config)
        .await
        .context("failed to start worker engine manager")?;
    let engine = Arc::new(runtime.handle());
    let tcp_listener = wire_listener::run_with_handle(
        engine.clone(),
        SocketAddr::new(server_config.listen_addr.into(), server_config.listen_port.get()),
    )
    .await
    .context("failed to start wire listener")?;
    tracing::info!(address = %tcp_listener.local_addr(), "pgtest server listening");

    #[cfg(unix)]
    let unix_listener = if let Some(directory) = &server_config.unix_socket_dir {
        let listener = wire_listener::run_unix_on_port(
            engine.clone(),
            directory,
            server_config.unix_socket_port.get(),
        )
        .await
        .context("failed to start Unix wire listener")?;
        tracing::info!(path = %listener.path().display(), "pgtest Unix socket listening");
        Some(listener)
    } else {
        None
    };

    tokio::select! {
        result = tokio::signal::ctrl_c() => result.context("failed to wait for Ctrl-C")?,
        _ = engine.stopped() => {},
    }
    tracing::info!("Stopping pgtest server");
    #[cfg(unix)]
    if let Some(listener) = unix_listener {
        listener.shutdown().await;
    }
    tcp_listener.shutdown().await;
    runtime.shutdown().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, net::IpAddr, path::PathBuf};

    use super::*;

    #[test]
    fn default_listener_stays_on_loopback() {
        let config = ServerConfig::init_from_hashmap(&HashMap::new()).unwrap();
        assert_eq!(*config.listen_addr, "127.0.0.1".parse::<IpAddr>().unwrap());
        assert_eq!(config.listen_addr, ListenAddr::default());
        assert_eq!(config.listen_port.get(), 6432);
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
            assert_eq!(*config.listen_addr, address.parse::<IpAddr>().unwrap());
            assert_eq!(config.listen_port.get(), 0);
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
        assert_eq!(config.listen_port.get(), 8432);
        assert_eq!(config.unix_socket_port.get(), 7432);
        assert_eq!(config.unix_socket_dir, Some(PathBuf::from("/tmp/pgtest").into()));
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
}
