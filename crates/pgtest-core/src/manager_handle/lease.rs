#[cfg(feature = "tokio-runtime")]
use std::sync::Arc;
use std::{borrow::Borrow, fmt, ops::Deref};

#[cfg(feature = "tokio-runtime")]
use pgtest_engine_backend::{PgTarget, jobs::DatabaseId};
use pgtest_utils::read_string::ReadString;
#[cfg(feature = "tokio-runtime")]
use tokio_util::sync::CancellationToken;

use super::{database_inventory::Database, errors::InvalidLeaseId};

/// A validated lease identifier, preserving its original UTF-8 value.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LeaseId(ReadString);

impl LeaseId {
    /// Returns an error unless the value contains 1–256 UTF-8 bytes and no '/'
    /// or NUL.
    pub fn new(value: impl Into<ReadString>) -> Result<Self, InvalidLeaseId> {
        let value = value.into();
        if value.is_empty() || value.len() > 256 || value.contains(['/', '\0']) {
            return Err(InvalidLeaseId);
        }
        Ok(Self(value))
    }
}

impl Deref for LeaseId {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for LeaseId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for LeaseId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for LeaseId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LeaseKey {
    pub lease: LeaseId,
    pub generation: u64,
}

#[cfg(feature = "tokio-runtime")]
pub struct LeaseSession {
    pub database_id: DatabaseId,
    pub target: Arc<PgTarget>,
    pub(crate) cancellation: CancellationToken,
}
#[cfg(feature = "tokio-runtime")]
impl LeaseSession {
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }
}

#[derive(Clone, Debug)]
pub struct LeaseEntry {
    pub database: Database,
    pub generation: u64,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn validates_utf8_byte_boundaries_and_forbidden_characters() {
        for value in [
            "".to_owned(),
            "a".repeat(257),
            "é".repeat(129),
            format!("{}a", "é".repeat(128)),
            "a/b".to_owned(),
            "a\0b".to_owned(),
        ] {
            assert_eq!(LeaseId::new(value), Err(InvalidLeaseId));
        }
        for value in
            ["a".to_owned(), "a".repeat(256), "é".repeat(128), " Mixed Case ' 雪 ".to_owned()]
        {
            let lease = LeaseId::new(value.clone()).unwrap();
            assert_eq!(lease.as_ref(), value);
            assert_eq!(lease.to_string(), value);
        }
    }

    #[test]
    fn accepts_read_strings_and_supports_borrowed_map_lookup() {
        let lease = LeaseId::new(ReadString::from("shared")).unwrap();
        assert_eq!(lease, LeaseId::new("shared").unwrap());
        let mut leases = HashMap::new();
        leases.insert(lease.clone(), 42);
        assert_eq!(leases.get(&lease), Some(&42));
        assert_eq!(leases.get("shared"), Some(&42));
        assert_eq!(&*lease, "shared");
    }
}
