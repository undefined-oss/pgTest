use std::sync::Arc;

use futures::StreamExt;
use pgtest::{worker_engine::core::LeaseId, worker_manager::ManagerHandle};
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

use crate::connection::ClientConnection;

mod protocol;
mod response;
mod statement;

pub use statement::{LeaseArgument, PgTestQueryTypeControlStatement};

#[hotpath::measure]
pub(crate) async fn serve(
    mut framed: ClientConnection,
    startup: Startup,
    manager: Arc<ManagerHandle>,
) -> std::io::Result<()> {
    let handlers = Arc::new(PgTestControlPanel::new(manager));
    let startup_handler = handlers.startup_handler();

    if let Err(error) =
        startup_handler.on_startup(&mut framed, PgWireFrontendMessage::Startup(startup)).await
    {
        process_error(&mut framed, error, false).await?;
        return Ok(());
    }

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
    manager: Arc<ManagerHandle>,
}

impl PgTestControlPanel {
    pub fn new(manager: Arc<ManagerHandle>) -> Self {
        Self { manager }
    }

    async fn release(&self, lease: LeaseId, format: FieldFormat) -> PgWireResult<Response> {
        self.manager.release(lease).await.map_err(response::release_error)?;
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
