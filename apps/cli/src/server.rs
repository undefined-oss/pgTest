use std::sync::Arc;

use anyhow::{Context, Result};
use pgtest::worker_manager::{RuntimeConfig, TokioRuntime};
use pgtest_pg_wire::wire_listener;

use crate::args::ServeOptions;

pub async fn serve(options: ServeOptions) -> Result<()> {
    #[cfg(unix)]
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    let startup = async {
        let prepared = pgtest_database_operations::backend::PreparedPostgres::prepare(
            options.postgres_config(),
        )
        .await?;
        let runtime = TokioRuntime::start(
            prepared.backend,
            RuntimeConfig {
                template: prepared.template,
                engine: options.engine_config(),
                creation_concurrency: prepared.creation_concurrency,
                cleanup_concurrency: prepared.cleanup_concurrency,
                stale_resources: prepared.stale_resources,
            },
        )
        .await?;
        Ok::<_, anyhow::Error>(runtime)
    };
    #[cfg(unix)]
    let engine = tokio::select! {
        result = startup => result.context("failed to start database engine")?,
        _ = interrupt.recv() => return Ok(()),
        _ = terminate.recv() => return Ok(()),
    };
    #[cfg(not(unix))]
    let engine = startup.await.context("failed to start database engine")?;
    let runtime = engine;
    let engine = Arc::new(runtime.handle());
    let mut tcp_listener = None;
    #[cfg(unix)]
    let mut unix_listener = None;

    let result: Result<()> = async {
        if let Some(address) = options.tcp_address() {
            let listener = wire_listener::run_with_handle(engine.clone(), address)
                .await
                .context("failed to start TCP listener")?;
            tracing::info!(address = %listener.local_addr(), "pgtest TCP listening");
            tcp_listener = Some(listener);
        }
        #[cfg(unix)]
        if let Some(directory) = &options.unix_socket_dir {
            let listener = wire_listener::run_unix_on_port(
                engine.clone(),
                directory,
                options.unix_socket_port.unwrap_or_default().get(),
            )
            .await
            .context("failed to start Unix listener")?;
            tracing::info!(path = %listener.path().display(), "pgtest Unix listening");
            unix_listener = Some(listener);
        }
        #[cfg(unix)]
        tokio::select! {
            _ = interrupt.recv() => {},
            _ = terminate.recv() => {},
            _ = engine.stopped() => {},
        }
        #[cfg(not(unix))]
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.context("failed to wait for Ctrl-C")?,
            _ = engine.stopped() => {},
        }
        Ok(())
    }
    .await;

    tracing::info!("Stopping pgtest server");
    if let Some(listener) = tcp_listener {
        listener.shutdown().await;
    }
    #[cfg(unix)]
    if let Some(listener) = unix_listener {
        listener.shutdown().await;
    }
    runtime.shutdown().await?;
    result
}
