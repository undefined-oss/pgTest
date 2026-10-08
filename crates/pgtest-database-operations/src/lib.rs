pub mod manager;

#[cfg(any(test, feature = "test-support"))]
pub mod testcontainer;

/// SQL completion collector used by the PostgreSQL profiling adapter.
#[cfg(feature = "hotpath")]
pub use hotpath::sqlx_tracing_layer as sql_tracing_layer;

pub mod backend;
