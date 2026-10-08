use thiserror::Error;

#[derive(Error, Debug, Clone, Copy, PartialEq, Eq)]
#[error("lease ID must contain 1 to 256 UTF-8 bytes and cannot contain '/' or NUL")]
pub struct InvalidLeaseId;

#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum ReleaseError {
    #[error("lease ID must contain 1 to 256 UTF-8 bytes and cannot contain '/' or NUL")]
    InvalidLeaseId,
    #[error("the lease record limit has been reached")]
    LeaseRecordLimitReached,
    #[error("the worker engine is unavailable")]
    EngineUnavailable,
    #[error("release acknowledgement timed out; retrying the same lease ID is safe")]
    ReplyTimedOut,
    #[error("the worker engine returned an unexpected release reply")]
    UnexpectedReply,
}

#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum AttachError {
    #[error("invalid lease ID")]
    InvalidLeaseId,
    #[error("the lease record limit has been reached")]
    LeaseRecordLimitReached,
    #[error("the lease has been released")]
    LeaseClosed,
    #[error("database does not match the configured template")]
    TemplateMismatch,
    #[error("the worker engine is unavailable")]
    EngineUnavailable,
    #[error("timed out waiting for a database")]
    TimedOut,
    #[error("the worker engine could not attach this lease")]
    Failed,
}

#[derive(Error, Debug)]
pub enum IOError {
    #[error("failed to deliver a message to the worker engine (channel closed)")]
    FailedToSendTheMessage,
}

#[derive(Error, Debug)]
pub enum ConsumerIOError {
    #[error("failed to reply to the consumer; its reply channel is gone")]
    FailedToReplyTheConsumer,
}

pub use pgtest_engine_backend::BackendError;
