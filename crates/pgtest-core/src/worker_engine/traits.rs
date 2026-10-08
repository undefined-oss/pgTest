use super::{
    database_jobs::{CleanupDatabase, CreateDatabases},
    errors::IOError,
    messages::{ConsumerReply, EngineMessage, LeaseKey, RequestId, Tick},
};

/// Immediate local effects only: never run provider I/O or reenter an actor.
pub trait EngineIO {
    fn request_creation(&mut self, request: CreateDatabases) -> Result<(), IOError>;
    fn request_cleanup(&mut self, request: CleanupDatabase) -> Result<(), IOError>;
    fn reply(&mut self, request: RequestId, reply: ConsumerReply) -> Result<(), ConsumerReply>;
    fn schedule(&mut self, key: LeaseKey, deadline: Tick, message: EngineMessage);
    fn cancel_sessions(&mut self, key: &LeaseKey);
}
