use thiserror::Error;

#[derive(Error, Debug, Clone, Copy, PartialEq, Eq)]
#[error("lease ID must contain 1 to 256 UTF-8 bytes and cannot contain '/' or NUL")]
pub struct InvalidLeaseId;

#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum ReleaseError {
    #[error("lease ID must contain 1 to 256 UTF-8 bytes and cannot contain '/' or NUL")]
    InvalidLeaseId,
    #[error("the worker engine is unavailable")]
    EngineUnavailable,
    #[error("release acknowledgement timed out")]
    ReplyTimedOut,
}

#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum AttachError {
    #[error("the lease has been released")]
    LeaseClosed,
    #[error("the worker engine is unavailable")]
    EngineUnavailable,
    #[error("timed out waiting for a database")]
    TimedOut,
}
