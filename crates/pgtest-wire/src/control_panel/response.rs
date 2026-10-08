use std::sync::Arc;

use futures::stream;
use pgtest::manager_handle::errors::ReleaseError;
use pgwire::{
    api::{
        Type,
        results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response},
    },
    error::{ErrorInfo, PgWireError, PgWireResult},
};

use super::PgTestQueryTypeControlStatement;

pub(super) fn result_schema(
    statement: &PgTestQueryTypeControlStatement,
    format: FieldFormat,
) -> Vec<FieldInfo> {
    match statement {
        PgTestQueryTypeControlStatement::Ping => ping_schema(format),
        PgTestQueryTypeControlStatement::Release(_) => release_schema(format),
    }
}

fn ping_schema(format: FieldFormat) -> Vec<FieldInfo> {
    vec![FieldInfo::new("?column?".into(), None, None, Type::INT4, format)]
}

fn release_schema(format: FieldFormat) -> Vec<FieldInfo> {
    vec![FieldInfo::new("pgtest_release".into(), None, None, Type::BOOL, format)]
}

pub(super) fn ping(format: FieldFormat) -> PgWireResult<Response> {
    let schema = Arc::new(ping_schema(format));
    let mut encoder = DataRowEncoder::new(schema.clone());
    encoder.encode_field(&1_i32)?;
    let row = encoder.take_row();
    Ok(Response::Query(QueryResponse::new(schema, stream::once(async { Ok(row) }))))
}

pub(super) fn released(format: FieldFormat) -> PgWireResult<Response> {
    let schema = Arc::new(release_schema(format));
    let mut encoder = DataRowEncoder::new(schema.clone());
    encoder.encode_field(&true)?;
    let row = encoder.take_row();
    Ok(Response::Query(QueryResponse::new(schema, stream::once(async { Ok(row) }))))
}

pub(super) fn user_error(code: &str, message: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new("ERROR".into(), code.into(), message.into())))
}

pub(super) fn unsupported_command() -> PgWireError {
    user_error("0A000", "command not support by the control panel")
}

pub(super) fn release_error(error: ReleaseError) -> PgWireError {
    let code = match error {
        ReleaseError::InvalidLeaseId => "22023",
        ReleaseError::EngineUnavailable => "08006",
        ReleaseError::ReplyTimedOut => "57014",
        ReleaseError::UnexpectedReply => "XX000",
    };
    user_error(code, error.to_string())
}
