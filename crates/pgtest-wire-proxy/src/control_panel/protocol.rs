use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use futures::{Sink, SinkExt};
use pgtest::{LeaseId, errors::ReleaseError};
use pgwire::{
    api::{
        ClientInfo, ClientPortalStore, DEFAULT_NAME, Type,
        portal::{Format, Portal},
        query::{ExtendedQueryHandler, SimpleQueryHandler},
        results::{FieldFormat, FieldInfo, Response},
        stmt::{QueryParser, StoredStatement},
        store::{Entry, PortalStore},
    },
    error::{PgWireError, PgWireResult},
    messages::{
        PgWireBackendMessage,
        extendedquery::{Bind, BindComplete, Parse, ParseComplete},
    },
};

use super::{LeaseArgument, PgTestControlPanel, PgTestQueryTypeControlStatement, response};

fn parse_lease_id(lease: String) -> PgWireResult<LeaseId> {
    LeaseId::new(lease).map_err(|_| response::release_error(ReleaseError::InvalidLeaseId))
}

fn resolve_lease_id(
    argument: &LeaseArgument,
    portal: &Portal<PgTestQueryTypeControlStatement>,
) -> PgWireResult<LeaseId> {
    let lease = match argument {
        LeaseArgument::Literal(literal) => literal.clone(),
        LeaseArgument::Parameter { index } => portal
            .parameter::<String>(*index, &Type::TEXT)?
            .ok_or_else(|| response::user_error("22004", "Lease ID cannot be NULL"))?,
    };
    parse_lease_id(lease)
}

fn validate_format_codes(codes: &[i16], kind: &str) -> PgWireResult<()> {
    if codes.len() > 1 {
        return Err(response::user_error(
            "08P01",
            format!("Expected 1 or 0 {kind} format codes, received {}", codes.len()),
        ));
    }
    if codes.first().is_some_and(|code| *code != 0 && *code != 1) {
        return Err(response::user_error(
            "08P01",
            format!("Expected {kind} format code to be text or binary"),
        ));
    }
    Ok(())
}

#[async_trait]
impl SimpleQueryHandler for PgTestControlPanel {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
    {
        tracing::debug!("SimpleQueryHandler query {:?}", query);
        let statement = PgTestQueryTypeControlStatement::parse(query)
            .map_err(|_| response::unsupported_command())?;
        let result = match statement {
            PgTestQueryTypeControlStatement::Ping => response::ping(FieldFormat::Text)?,
            PgTestQueryTypeControlStatement::Release(LeaseArgument::Literal(lease)) => {
                self.release(parse_lease_id(lease)?, FieldFormat::Text).await?
            }
            PgTestQueryTypeControlStatement::Release(LeaseArgument::Parameter { .. }) => {
                return Err(response::user_error(
                    "42P02",
                    "parameters require the extended query protocol",
                ));
            }
        };
        Ok(vec![result])
    }
}

#[async_trait]
impl ExtendedQueryHandler for PgTestControlPanel {
    type QueryParser = ControlQueryParser;
    type Statement = PgTestQueryTypeControlStatement;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(ControlQueryParser)
    }

    async fn do_query<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let statement = &portal.statement.statement;
        let expected = ControlQueryParser.get_parameter_types(statement)?.len();
        if portal.parameter_len() != expected {
            return Err(response::user_error(
                "08P01",
                format!(
                    "Expected {expected} bound parameters, received {}",
                    portal.parameter_len()
                ),
            ));
        }

        let format = portal.result_column_format.format_for(0);
        match statement {
            PgTestQueryTypeControlStatement::Ping => response::ping(format),
            PgTestQueryTypeControlStatement::Release(argument) => {
                self.release(resolve_lease_id(argument, portal)?, format).await
            }
        }
    }

    async fn on_parse<C>(&self, client: &mut C, message: Parse) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if message.type_oids.iter().any(|oid| *oid != 0 && *oid != Type::TEXT.oid()) {
            return Err(response::user_error("42804", "pgtest_release requires a TEXT parameter"));
        }
        let name = message.name.as_deref().unwrap_or(DEFAULT_NAME);
        match StoredStatement::parse(client, &message, self.query_parser()).await? {
            Some(statement) => client.portal_store().put_statement(Arc::new(statement)),
            None => client.portal_store().put_empty_statement(name),
        }
        // Batch acknowledgements until a subsequent response, Flush, or Sync
        // flushes them.
        client.feed(PgWireBackendMessage::ParseComplete(ParseComplete::new())).await?;
        Ok(())
    }

    async fn on_bind<C>(&self, client: &mut C, message: Bind) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        validate_format_codes(&message.parameter_format_codes, "parameter")?;
        validate_format_codes(&message.result_column_format_codes, "result column")?;

        let statement_name = message.statement_name.as_deref().unwrap_or(DEFAULT_NAME);
        let portal_name = message.portal_name.as_deref().unwrap_or(DEFAULT_NAME);
        match client.portal_store().get_statement(statement_name) {
            Some(Entry::Value(statement)) => {
                let expected = ControlQueryParser.get_parameter_types(&statement.statement)?.len();
                if message.parameters.len() != expected {
                    return Err(response::user_error(
                        "08P01",
                        format!(
                            "Expected {expected} bind parameters, received {}",
                            message.parameters.len()
                        ),
                    ));
                }
                let portal = Portal::try_new(&message, statement.clone())?;
                if let PgTestQueryTypeControlStatement::Release(argument) = &statement.statement {
                    resolve_lease_id(argument, &portal)?;
                }
                client.portal_store().put_portal(Arc::new(portal));
            }
            Some(Entry::Empty) => {
                if !message.parameters.is_empty() {
                    return Err(response::user_error(
                        "08P01",
                        format!(
                            "bind message supplies {} parameters, but prepared statement {:?} \
                             requires 0",
                            message.parameters.len(),
                            statement_name,
                        ),
                    ));
                }
                client.portal_store().put_empty_portal(portal_name);
            }
            None => return Err(PgWireError::StatementNotFound(statement_name.to_owned())),
        }
        client.feed(PgWireBackendMessage::BindComplete(BindComplete::new())).await?;
        Ok(())
    }
}

pub struct ControlQueryParser;

#[async_trait]
impl QueryParser for ControlQueryParser {
    type Statement = PgTestQueryTypeControlStatement;

    async fn parse_sql<C>(
        &self,
        _client: &C,
        sql: &str,
        types: &[Option<Type>],
    ) -> PgWireResult<Option<Self::Statement>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        tracing::debug!("ControlQueryParser parse sql {:?}", sql);
        let statement = PgTestQueryTypeControlStatement::parse(sql)
            .map_err(|_| response::unsupported_command())?;
        match &statement {
            PgTestQueryTypeControlStatement::Ping
            | PgTestQueryTypeControlStatement::Release(LeaseArgument::Literal(_)) => {
                if !types.is_empty() {
                    return Err(response::user_error(
                        "42804",
                        "Parameters not expect for ping command",
                    ));
                }
            }
            PgTestQueryTypeControlStatement::Release(LeaseArgument::Parameter { index }) => {
                let expected = self.get_parameter_types(&statement)?.len();
                if types.len() > expected {
                    return Err(response::user_error(
                        "0A000",
                        format!("Expected {expected} bound parameters, received {}", types.len()),
                    ));
                }
                if types.get(*index).and_then(Option::as_ref).is_some_and(|ty| ty != &Type::TEXT) {
                    return Err(response::user_error(
                        "42804",
                        format!("pgtest_release expected {} argument", Type::TEXT),
                    ));
                }
            }
        }
        Ok(Some(statement))
    }

    fn get_parameter_types(&self, statement: &Self::Statement) -> PgWireResult<Vec<Type>> {
        Ok(match statement {
            PgTestQueryTypeControlStatement::Release(LeaseArgument::Parameter { .. }) => {
                vec![Type::TEXT]
            }
            _ => vec![],
        })
    }

    fn get_result_schema(
        &self,
        statement: &Self::Statement,
        column_format: Option<&Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        let format = column_format.map_or(FieldFormat::Text, |format| format.format_for(0));
        Ok(response::result_schema(statement, format))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        pin::Pin,
        sync::Arc,
        task::{Context, Poll},
    };

    use bytes::Bytes;
    use pgtest::{config::PoolConfig, lifecycle::LifecycleWorkers};
    use pgwire::{
        api::{
            ClientInfo, DefaultClient, PgWireConnectionState, PgWireServerHandlers, Type,
            portal::Portal, stmt::StoredStatement,
        },
        error::PgWireError,
        messages::{
            PgWireFrontendMessage,
            extendedquery::{Bind, Describe, Execute, Flush, Parse, Sync},
        },
        tokio::server::{PgWireMessageServerCodec, process_error, process_message},
    };
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
    use tokio_util::{codec::Framed, sync::CancellationToken};

    use super::{parse_lease_id, resolve_lease_id};
    use crate::control_panel::{
        LeaseArgument, PgTestControlPanel, PgTestQueryTypeControlStatement,
    };

    #[derive(Default)]
    struct RecordingIo {
        writes: Vec<Vec<u8>>,
    }

    impl AsyncRead for RecordingIo {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }

    impl AsyncWrite for RecordingIo {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.writes.push(bytes.to_vec());
            Poll::Ready(Ok(bytes.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    type TestConnection =
        Framed<RecordingIo, PgWireMessageServerCodec<PgTestQueryTypeControlStatement>>;

    async fn dispatch(
        client: &mut TestConnection,
        handlers: &Arc<PgTestControlPanel>,
        message: PgWireFrontendMessage,
    ) {
        let wait_for_sync = message.is_extended_query();
        if let Err(error) = process_message(
            message,
            client,
            handlers.startup_handler(),
            handlers.clone(),
            handlers.clone(),
            handlers.copy_handler(),
            handlers.cancel_handler(),
        )
        .await
        {
            process_error(client, error, wait_for_sync).await.unwrap();
        }
    }

    fn parse() -> PgWireFrontendMessage {
        PgWireFrontendMessage::Parse(Parse::new(None, "SELECT 1".into(), vec![]))
    }

    fn bind() -> PgWireFrontendMessage {
        PgWireFrontendMessage::Bind(Bind::new(None, None, vec![], vec![], vec![]))
    }

    // Return the protocol message tags in each actual transport write.
    fn take_writes(client: &mut TestConnection) -> Vec<Vec<u8>> {
        std::mem::take(&mut client.get_mut().writes)
            .into_iter()
            .map(|write| {
                let mut tags = Vec::new();
                let mut remaining = write.as_slice();
                while !remaining.is_empty() {
                    tags.push(remaining[0]);
                    let length = u32::from_be_bytes(remaining[1..5].try_into().unwrap()) as usize;
                    assert!(length >= 4);
                    remaining = &remaining[1 + length..];
                }
                tags
            })
            .collect()
    }

    #[tokio::test]
    async fn extended_acknowledgements_preserve_flush_boundaries_and_error_recovery() {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            // Creation serializes releases that miss the ready registry.
            // Releasing an unseen lease must not create any database.
            struct NoCreation;
            impl pgtest_engine_backend::DatabaseCreator for NoCreation {
                async fn create_database(
                    &self,
                ) -> Result<
                    pgtest_engine_backend::ProvisionedDatabase,
                    pgtest_engine_backend::BackendError,
                > {
                    panic!("protocol-only release must not create a database")
                }
            }
            let stop = CancellationToken::new();
            let _stop_on_exit = stop.clone().drop_guard();
            let workers = LifecycleWorkers::new(PoolConfig::default(), Vec::new(), stop.clone());
            let (ready, started) = tokio::sync::oneshot::channel();
            let creation = tokio::spawn(workers.creation.run(
                async { Ok(Arc::new(NoCreation)) },
                std::num::NonZeroUsize::MIN,
                ready,
            ));
            started.await.unwrap().unwrap();
            let handlers = Arc::new(PgTestControlPanel::new(Arc::new(workers.service.clone())));
            let codec = PgWireMessageServerCodec::new(DefaultClient::new(
                ([127, 0, 0, 1], 0).into(),
                false,
            ));
            let mut client = Framed::new(RecordingIo::default(), codec);
            client.set_state(PgWireConnectionState::ReadyForQuery);

            // Exercise the actual release response from the profiled workload:
            // 26 bytes of result/completion plus 6 bytes of ReadyForQuery.
            dispatch(
                &mut client,
                &handlers,
                PgWireFrontendMessage::Parse(Parse::new(
                    None,
                    "SELECT pgtest_release('batching-test')".into(),
                    vec![],
                )),
            )
            .await;
            dispatch(&mut client, &handlers, bind()).await;
            dispatch(
                &mut client,
                &handlers,
                PgWireFrontendMessage::Describe(Describe::new(b'P', None)),
            )
            .await;
            assert_eq!(take_writes(&mut client), vec![b"12T".to_vec()]);
            dispatch(&mut client, &handlers, PgWireFrontendMessage::Execute(Execute::new(None, 0)))
                .await;
            assert_eq!(client.get_ref().writes.iter().map(Vec::len).collect::<Vec<_>>(), vec![26]);
            assert_eq!(take_writes(&mut client), vec![b"DC".to_vec()]);
            assert!(client.write_buffer().is_empty());
            dispatch(&mut client, &handlers, PgWireFrontendMessage::Sync(Sync::new())).await;
            assert_eq!(client.get_ref().writes.iter().map(Vec::len).collect::<Vec<_>>(), vec![6]);
            assert_eq!(take_writes(&mut client), vec![b"Z".to_vec()]);

            // The common Parse/Bind/Describe sequence becomes one transport
            // write.
            dispatch(&mut client, &handlers, parse()).await;
            dispatch(&mut client, &handlers, bind()).await;
            assert!(take_writes(&mut client).is_empty());
            dispatch(
                &mut client,
                &handlers,
                PgWireFrontendMessage::Describe(Describe::new(b'P', None)),
            )
            .await;
            assert_eq!(take_writes(&mut client), vec![b"12T".to_vec()]);
            dispatch(&mut client, &handlers, PgWireFrontendMessage::Execute(Execute::new(None, 0)))
                .await;
            assert_eq!(take_writes(&mut client), vec![b"DC".to_vec()]);
            dispatch(&mut client, &handlers, PgWireFrontendMessage::Sync(Sync::new())).await;
            assert_eq!(take_writes(&mut client), vec![b"Z".to_vec()]);

            // Clients can explicitly request either acknowledgement before
            // continuing.
            for (message, tag) in [(parse(), b'1'), (bind(), b'2')] {
                dispatch(&mut client, &handlers, message).await;
                assert!(take_writes(&mut client).is_empty());
                dispatch(&mut client, &handlers, PgWireFrontendMessage::Flush(Flush::new())).await;
                assert_eq!(take_writes(&mut client), vec![vec![tag]]);
            }

            // Execute delivers its result immediately; a subsequent Flush
            // does not send duplicate bytes.
            dispatch(&mut client, &handlers, PgWireFrontendMessage::Execute(Execute::new(None, 0)))
                .await;
            assert_eq!(take_writes(&mut client), vec![b"DC".to_vec()]);
            dispatch(&mut client, &handlers, PgWireFrontendMessage::Flush(Flush::new())).await;
            assert!(take_writes(&mut client).is_empty());
            dispatch(&mut client, &handlers, PgWireFrontendMessage::Sync(Sync::new())).await;
            assert_eq!(take_writes(&mut client), vec![b"Z".to_vec()]);

            // Sync also drains buffered acknowledgements without
            // Describe/Execute.
            dispatch(&mut client, &handlers, parse()).await;
            dispatch(&mut client, &handlers, bind()).await;
            dispatch(&mut client, &handlers, PgWireFrontendMessage::Sync(Sync::new())).await;
            assert_eq!(take_writes(&mut client), vec![b"12Z".to_vec()]);

            // A Bind error flushes the earlier ParseComplete before
            // ErrorResponse.
            dispatch(&mut client, &handlers, parse()).await;
            let invalid_bind = Bind::new(
                None,
                None,
                vec![],
                vec![Some(Bytes::from_static(b"unexpected"))],
                vec![],
            );
            dispatch(&mut client, &handlers, PgWireFrontendMessage::Bind(invalid_bind)).await;
            assert_eq!(take_writes(&mut client), vec![b"1E".to_vec()]);
            assert!(matches!(client.state(), PgWireConnectionState::AwaitingSync));
            dispatch(&mut client, &handlers, bind()).await;
            assert!(take_writes(&mut client).is_empty());
            dispatch(&mut client, &handlers, PgWireFrontendMessage::Sync(Sync::new())).await;
            assert_eq!(take_writes(&mut client), vec![b"Z".to_vec()]);
            assert!(matches!(client.state(), PgWireConnectionState::ReadyForQuery));

            // A fresh extended query succeeds after error recovery, without
            // Describe.
            dispatch(&mut client, &handlers, parse()).await;
            dispatch(&mut client, &handlers, bind()).await;
            dispatch(&mut client, &handlers, PgWireFrontendMessage::Execute(Execute::new(None, 0)))
                .await;
            assert_eq!(take_writes(&mut client), vec![b"12DC".to_vec()]);
            dispatch(&mut client, &handlers, PgWireFrontendMessage::Sync(Sync::new())).await;
            assert_eq!(take_writes(&mut client), vec![b"Z".to_vec()]);

            // A later pipeline error follows the already flushed completion.
            dispatch(&mut client, &handlers, parse()).await;
            dispatch(&mut client, &handlers, bind()).await;
            dispatch(&mut client, &handlers, PgWireFrontendMessage::Execute(Execute::new(None, 0)))
                .await;
            dispatch(
                &mut client,
                &handlers,
                PgWireFrontendMessage::Parse(Parse::new(None, "SELECT 2".into(), vec![])),
            )
            .await;
            assert_eq!(take_writes(&mut client), vec![b"12DC".to_vec(), b"E".to_vec()]);
            assert!(matches!(client.state(), PgWireConnectionState::AwaitingSync));
            dispatch(&mut client, &handlers, PgWireFrontendMessage::Sync(Sync::new())).await;
            assert_eq!(take_writes(&mut client), vec![b"Z".to_vec()]);

            // A bounded fetch keeps PortalSuspended's existing immediate flush.
            // Resuming the portal delivers no duplicate row and flushes
            // completion immediately, as with an unbounded Execute.
            dispatch(&mut client, &handlers, parse()).await;
            dispatch(&mut client, &handlers, bind()).await;
            dispatch(&mut client, &handlers, PgWireFrontendMessage::Execute(Execute::new(None, 1)))
                .await;
            assert_eq!(take_writes(&mut client), vec![b"12Ds".to_vec()]);
            dispatch(&mut client, &handlers, PgWireFrontendMessage::Execute(Execute::new(None, 0)))
                .await;
            assert_eq!(take_writes(&mut client), vec![b"C".to_vec()]);
            dispatch(&mut client, &handlers, PgWireFrontendMessage::Sync(Sync::new())).await;
            assert_eq!(take_writes(&mut client), vec![b"Z".to_vec()]);

            // Empty portals retain pgwire's EmptyQueryResponse handling.
            dispatch(
                &mut client,
                &handlers,
                PgWireFrontendMessage::Parse(Parse::new(None, "".into(), vec![])),
            )
            .await;
            dispatch(&mut client, &handlers, bind()).await;
            dispatch(&mut client, &handlers, PgWireFrontendMessage::Execute(Execute::new(None, 0)))
                .await;
            dispatch(&mut client, &handlers, PgWireFrontendMessage::Sync(Sync::new())).await;
            assert_eq!(take_writes(&mut client), vec![b"12IZ".to_vec()]);

            // Buffer pressure may flush early; every response still arrives in
            // order, and Sync remains the final boundary.
            client.set_backpressure_boundary(1);
            dispatch(&mut client, &handlers, parse()).await;
            dispatch(&mut client, &handlers, bind()).await;
            dispatch(&mut client, &handlers, PgWireFrontendMessage::Execute(Execute::new(None, 0)))
                .await;
            dispatch(&mut client, &handlers, PgWireFrontendMessage::Sync(Sync::new())).await;
            assert_eq!(take_writes(&mut client).concat(), b"12DCZ");

            // A missing portal still produces an immediate Execute error.
            dispatch(
                &mut client,
                &handlers,
                PgWireFrontendMessage::Execute(Execute::new(Some("missing".into()), 0)),
            )
            .await;
            assert_eq!(take_writes(&mut client), vec![b"E".to_vec()]);
            dispatch(&mut client, &handlers, PgWireFrontendMessage::Sync(Sync::new())).await;
            assert_eq!(take_writes(&mut client), vec![b"Z".to_vec()]);
            stop.cancel();
            creation.await.unwrap();
        })
        .await
        .expect("extended protocol must not stall waiting for a flush");
    }

    #[test]
    fn release_lease_validation_preserves_protocol_errors() {
        for value in ["".to_owned(), "a/b".to_owned(), "a\0b".to_owned(), "a".repeat(257)] {
            let error = parse_lease_id(value).unwrap_err();
            assert!(matches!(error, PgWireError::UserError(info) if info.code == "22023"));
        }
        let value = " Mixed Case 雪 ";
        assert_eq!(parse_lease_id(value.to_owned()).unwrap().as_ref(), value);
    }

    #[test]
    fn release_parameters_validate_ids_and_preserve_null_error() {
        let argument = LeaseArgument::Parameter { index: 0 };
        let statement = Arc::new(StoredStatement::new(
            "release".to_owned(),
            PgTestQueryTypeControlStatement::Release(argument.clone()),
            vec![Some(Type::TEXT)],
        ));
        for format in [0, 1] {
            for (value, error_code) in [
                (None, Some("22004")),
                (Some("a/b"), Some("22023")),
                (Some(" Mixed Case 雪 "), None),
            ] {
                let bind =
                    Bind::new(None, None, vec![format], vec![value.map(Bytes::from)], vec![]);
                let portal = Portal::try_new(&bind, statement.clone()).unwrap();
                let result = resolve_lease_id(&argument, &portal);
                if let Some(code) = error_code {
                    assert!(
                        matches!(result, Err(PgWireError::UserError(info)) if info.code == code)
                    );
                } else {
                    assert_eq!(result.unwrap().as_ref(), value.unwrap());
                }
            }
        }
    }
}
