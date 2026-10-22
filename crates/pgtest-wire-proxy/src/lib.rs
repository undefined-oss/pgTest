//! PostgreSQL lease proxy hosted by Pingora.
//!
//! Client listeners can use TCP or Unix sockets. Unix connections use a pgwire
//! metadata placeholder because its client API requires an IP address.
//! One Pingora work-stealing runtime hosts creation, cleanup, and connections.
//! All workers resolve leases through one creation/cleanup service and shared
//! database inventory. PostgreSQL performs authentication through the byte
//! relay. TCP and Unix sockets are supported; SSL/GSS requests receive `N`.
//! The reserved `pgtest` database serves control SQL on the shared runtime.

mod connection_service;
mod control_panel;
mod proxy;
mod relay;
mod route;
mod server;
mod services;
mod startup;
#[cfg(unix)]
mod unix_socket;
mod upstream;

pub use proxy::WireProxy;
pub use server::{Listeners, ProxyServer};
pub use services::ServiceBuilder;

#[cfg(test)]
#[path = "../tests/lifecycle.rs"]
mod lifecycle_integration_tests;
#[cfg(test)]
#[path = "../tests/postgres.rs"]
mod postgres_integration_tests;
#[cfg(test)]
mod test_support;
