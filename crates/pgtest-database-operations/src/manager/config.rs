use std::num::{NonZeroU16, NonZeroUsize};

use envconfig::Envconfig;

#[derive(Envconfig, Debug)]
pub struct PostgresConfig {
    #[envconfig(from = "PGTEST_PG_HOST", default = "127.0.0.1")]
    pub pgtest_pg_host: String,
    #[envconfig(from = "PGTEST_PG_PORT", default = "5432")]
    pub pgtest_pg_port: NonZeroU16,
    #[envconfig(from = "PGTEST_PG_USER", default = "postgres")]
    pub pgtest_pg_user: String,
    #[envconfig(from = "PGTEST_PG_DATABASE", default = "pgtest")]
    pub pgtest_pg_database: String,
    #[envconfig(from = "PGTEST_CREATION_POOL_CONNECTION", default = "10")]
    pub pgtest_pg_creation_pool_connection: NonZeroUsize,
    #[envconfig(from = "PGTEST_CLEANUP_POOL_CONNECTION", default = "5")]
    pub pgtest_pg_cleanup_pool_connection: NonZeroUsize,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn positive_settings_are_checked_when_parsing_configuration() {
        let defaults = PostgresConfig::init_from_hashmap(&HashMap::new()).unwrap();
        assert_eq!(defaults.pgtest_pg_port.get(), 5432);
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
