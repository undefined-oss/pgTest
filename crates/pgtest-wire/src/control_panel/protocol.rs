use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use futures::{Sink, SinkExt};
use pgtest::manager_handle::{LeaseId, errors::ReleaseError};
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
        client.send(PgWireBackendMessage::ParseComplete(ParseComplete::new())).await?;
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
        client.send(PgWireBackendMessage::BindComplete(BindComplete::new())).await?;
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
    use std::sync::Arc;

    use bytes::Bytes;
    use pgwire::{
        api::{Type, portal::Portal, stmt::StoredStatement},
        error::PgWireError,
        messages::extendedquery::Bind,
    };

    use super::{parse_lease_id, resolve_lease_id};
    use crate::control_panel::{LeaseArgument, PgTestQueryTypeControlStatement};

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
