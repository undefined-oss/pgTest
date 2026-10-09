use std::num::NonZeroUsize;

use derive_more::{Debug, Deref, Display, From, FromStr, Into};
use envconfig::Envconfig;
use pgtest_utils::{
    network::port::{Port, PortError},
    non_empty_string::NonEmptyString,
};

#[derive(Clone, Debug, PartialEq, Eq, Deref, Display, From, FromStr, Into)]
#[into(NonEmptyString, String)]
pub struct PostgresHost(NonEmptyString);

impl Default for PostgresHost {
    fn default() -> Self {
        Self(NonEmptyString::new("127.0.0.1").unwrap())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deref, Display, From, FromStr, Into)]
#[into(NonEmptyString, String)]
pub struct PostgresUser(NonEmptyString);

impl Default for PostgresUser {
    fn default() -> Self {
        Self(NonEmptyString::new("postgres").unwrap())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deref, Display, From, FromStr, Into)]
#[into(NonEmptyString, String)]
pub struct PostgresDatabase(NonEmptyString);

impl Default for PostgresDatabase {
    fn default() -> Self {
        Self(NonEmptyString::new("pgtest").unwrap())
    }
}

#[derive(Clone, Copy, Debug, FromStr, Display, From)]
pub struct PostgresUpstreamPort(Port);

impl Default for PostgresUpstreamPort {
    fn default() -> Self {
        Self::new(5432).unwrap()
    }
}

impl PostgresUpstreamPort {
    pub fn new(port: u16) -> Result<Self, PortError> {
        let port = Port::new(port).ok_or(PortError::PortValueProvidedIsZero)?;
        Ok(Self(port))
    }

    pub fn port(&self) -> u16 {
        *self.0
    }
}

impl TryFrom<u16> for PostgresUpstreamPort {
    type Error = PortError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl std::ops::Deref for PostgresUpstreamPort {
    type Target = u16;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone, Copy, Deref, FromStr, Debug, Display, From, Into)]
pub struct CreationPoolSize(NonZeroUsize);

impl Default for CreationPoolSize {
    fn default() -> Self {
        Self(NonZeroUsize::new(10).unwrap())
    }
}

#[derive(Clone, Copy, Deref, FromStr, Debug, Display, From, Into)]
pub struct CleanupPoolSize(NonZeroUsize);

impl Default for CleanupPoolSize {
    fn default() -> Self {
        Self(NonZeroUsize::new(5).unwrap())
    }
}

#[derive(Envconfig, Debug, Clone)]
pub struct PostgresConfig {
    #[envconfig(from = "PGTEST_PG_HOST", default = "127.0.0.1")]
    pub pgtest_pg_host: PostgresHost,
    #[envconfig(from = "PGTEST_PG_PORT", default = "5432")]
    pub pgtest_pg_port: PostgresUpstreamPort,
    #[envconfig(from = "PGTEST_PG_USER", default = "postgres")]
    pub pgtest_pg_user: PostgresUser,
    #[envconfig(from = "PGTEST_PG_DATABASE", default = "pgtest")]
    pub pgtest_pg_database: PostgresDatabase,
    #[envconfig(from = "PGTEST_CREATION_POOL_CONNECTION", default = "10")]
    pub pgtest_pg_creation_pool_connection: CreationPoolSize,
    #[envconfig(from = "PGTEST_CLEANUP_POOL_CONNECTION", default = "5")]
    pub pgtest_pg_cleanup_pool_connection: CleanupPoolSize,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn empty_postgres_settings_are_rejected() {
        for variable in ["PGTEST_PG_HOST", "PGTEST_PG_USER", "PGTEST_PG_DATABASE"] {
            let vars = HashMap::from([(variable.to_owned(), String::new())]);
            assert!(PostgresConfig::init_from_hashmap(&vars).is_err(), "{variable}");
        }
        let defaults = PostgresConfig::init_from_hashmap(&HashMap::new()).unwrap();
        assert_eq!(defaults.pgtest_pg_host, PostgresHost::default());
        assert_eq!(defaults.pgtest_pg_user, PostgresUser::default());
        assert_eq!(defaults.pgtest_pg_database, PostgresDatabase::default());
    }

    #[test]
    fn positive_settings_are_checked_when_parsing_configuration() {
        let defaults = PostgresConfig::init_from_hashmap(&HashMap::new()).unwrap();
        assert_eq!(*defaults.pgtest_pg_port, 5432);
        assert_eq!(defaults.pgtest_pg_creation_pool_connection.get(), 10);
        assert_eq!(defaults.pgtest_pg_cleanup_pool_connection.get(), 5);

        for variable in
            ["PGTEST_PG_PORT", "PGTEST_CREATION_POOL_CONNECTION", "PGTEST_CLEANUP_POOL_CONNECTION"]
        {
            for invalid in ["0", "-1", "invalid", "184467440737095516160"] {
                let vars = HashMap::from([(variable.to_owned(), invalid.to_owned())]);
                assert!(PostgresConfig::init_from_hashmap(&vars).is_err(), "{variable}={invalid}");
            }
            let vars = HashMap::from([(variable.to_owned(), "1".to_owned())]);
            assert!(PostgresConfig::init_from_hashmap(&vars).is_ok(), "{variable}=1");
        }
        let vars = HashMap::from([("PGTEST_PG_PORT".to_owned(), "65536".to_owned())]);
        assert!(PostgresConfig::init_from_hashmap(&vars).is_err());
    }
}
