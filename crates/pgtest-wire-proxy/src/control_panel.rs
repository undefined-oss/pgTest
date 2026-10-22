//! Control SQL served on the accepting Pingora worker.
use std::sync::Arc;

use futures::StreamExt;
use pgtest::{LeaseId, LeaseService};
use pgwire::{
    api::{
        PgWireServerHandlers,
        auth::StartupHandler,
        results::{FieldFormat, Response},
    },
    error::PgWireResult,
    messages::{PgWireFrontendMessage, startup::Startup},
    tokio::server::{process_error, process_message},
};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::startup::ClientConnection;

mod protocol;
mod response;
mod statement;

pub use statement::{LeaseArgument, PgTestQueryTypeControlStatement};

/// Complete control startup within the caller's startup deadline.
#[hotpath::measure(future = true)]
pub(crate) async fn initialize<S>(
    framed: &mut ClientConnection<S>,
    startup: Startup,
    handlers: &PgTestControlPanel,
) -> std::io::Result<bool>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
{
    let startup_handler = handlers.startup_handler();

    if let Err(error) =
        startup_handler.on_startup(framed, PgWireFrontendMessage::Startup(startup)).await
    {
        process_error(framed, error, false).await?;
        return Ok(false);
    }
    Ok(true)
}

#[hotpath::measure(future = true)]
pub(crate) async fn run<S>(
    mut framed: ClientConnection<S>,
    handlers: Arc<PgTestControlPanel>,
) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
{
    let startup_handler = handlers.startup_handler();
    let copy_handler = handlers.copy_handler();
    let cancel_handler = handlers.cancel_handler();

    while let Some(message) = framed.next().await {
        let message = message?;
        if matches!(message, PgWireFrontendMessage::Terminate(_)) {
            break;
        }

        let wait_for_sync = message.is_extended_query();
        // Use the concrete query handler so its Statement type matches the
        // codec.
        if let Err(error) = process_message(
            message,
            &mut framed,
            startup_handler.clone(),
            handlers.clone(),
            handlers.clone(),
            copy_handler.clone(),
            cancel_handler.clone(),
        )
        .await
        {
            process_error(&mut framed, error, wait_for_sync).await?;
        }
    }

    Ok(())
}

#[derive(Clone)]
pub struct PgTestControlPanel {
    leases: Arc<LeaseService>,
}

impl PgTestControlPanel {
    pub fn new(leases: Arc<LeaseService>) -> Self {
        Self { leases }
    }

    async fn release(&self, lease: LeaseId, format: FieldFormat) -> PgWireResult<Response> {
        self.leases.release(lease).await.map_err(response::release_error)?;
        response::released(format)
    }
}

impl PgWireServerHandlers for PgTestControlPanel {
    fn simple_query_handler(&self) -> Arc<impl pgwire::api::query::SimpleQueryHandler> {
        Arc::new(self.clone())
    }

    fn extended_query_handler(&self) -> Arc<impl pgwire::api::query::ExtendedQueryHandler> {
        Arc::new(self.clone())
    }
}
