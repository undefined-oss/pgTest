pub mod config;
pub(crate) mod connection;
pub mod database_name;
pub mod errors;
pub(crate) mod sql_profile;

#[cfg(test)]
mod client_tests;

#[cfg(any(test, feature = "test-support"))]
pub mod testcontainer;

/// SQL completion collector used by the PostgreSQL profiling adapter.
#[cfg(feature = "hotpath")]
pub use hotpath::sqlx_tracing_layer as sql_tracing_layer;

pub mod backend;

pub mod cleanup_client;
pub mod creation_client;
