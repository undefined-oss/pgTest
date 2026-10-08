//! Configuration for Manager inventory growth and lease timeouts.

use derive_more::{Deref, Display, From, FromStr, Into};
use envconfig::Envconfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deref, Display, From, FromStr, Into)]
pub struct InitialSlots(u16);

impl Default for InitialSlots {
    fn default() -> Self {
        Self(16)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deref, Display, From, FromStr, Into)]
pub struct StarvationThreshold(u16);

impl Default for StarvationThreshold {
    fn default() -> Self {
        Self(8)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deref, Display, From, FromStr, Into)]
pub struct GrowBatchSize(u16);

impl Default for GrowBatchSize {
    fn default() -> Self {
        Self(16)
    }
}

#[derive(Envconfig, Debug, Clone, Copy)]
pub struct ManagerConfig {
    #[envconfig(from = "PGTEST_POOL_INITIAL_SIZE", default = "16")]
    pub initial_slots: InitialSlots,
    #[envconfig(from = "PGTEST_POOL_STARVATION_THRESHOLD", default = "8")]
    pub starvation_threshold: StarvationThreshold,
    #[envconfig(from = "PGTEST_POOL_GROW_BATCH_SIZE", default = "16")]
    pub grow_batch_size: GrowBatchSize,

    #[envconfig(from = "PGTEST_LEASE_CLAIM_TIMEOUT_MS", default = "30000")]
    pub lease_claim_timeout_ms: u128,
}

#[cfg(any(test, feature = "test-support"))]
impl Default for ManagerConfig {
    fn default() -> Self {
        Self {
            initial_slots: 4.into(),
            starvation_threshold: 2.into(),
            grow_batch_size: 4.into(),

            lease_claim_timeout_ms: 30_000,
        }
    }
}

#[cfg(test)]
mod config_tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn pool_settings_defaults_and_bounds_match_configuration() {
        let config = ManagerConfig::init_from_hashmap(&HashMap::new()).unwrap();
        assert_eq!(config.initial_slots, InitialSlots::default());
        assert_eq!(config.starvation_threshold, StarvationThreshold::default());
        assert_eq!(config.grow_batch_size, GrowBatchSize::default());

        for variable in [
            "PGTEST_POOL_INITIAL_SIZE",
            "PGTEST_POOL_STARVATION_THRESHOLD",
            "PGTEST_POOL_GROW_BATCH_SIZE",
        ] {
            for invalid in ["-1", "65536", "invalid"] {
                let vars = HashMap::from([(variable.to_owned(), invalid.to_owned())]);
                assert!(ManagerConfig::init_from_hashmap(&vars).is_err(), "{variable}={invalid}");
            }
            let vars = HashMap::from([(variable.to_owned(), "65535".to_owned())]);
            assert!(ManagerConfig::init_from_hashmap(&vars).is_ok(), "{variable}=65535");
        }
    }

    #[test]
    fn zero_settings_keep_their_existing_meaning() {
        let vars: HashMap<_, _> = [
            "PGTEST_POOL_INITIAL_SIZE",
            "PGTEST_POOL_STARVATION_THRESHOLD",
            "PGTEST_POOL_GROW_BATCH_SIZE",
            "PGTEST_LEASE_CLAIM_TIMEOUT_MS",
        ]
        .into_iter()
        .map(|key| (key.to_owned(), "0".to_owned()))
        .collect();
        let config = ManagerConfig::init_from_hashmap(&vars).unwrap();
        assert_eq!(*config.initial_slots, 0);
        assert_eq!(*config.starvation_threshold, 0);
        assert_eq!(*config.grow_batch_size, 0);
        assert_eq!(config.lease_claim_timeout_ms, 0);
    }
}
