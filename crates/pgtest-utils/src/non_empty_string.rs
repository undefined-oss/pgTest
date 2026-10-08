use std::str::FromStr;

use derive_more::{Deref, Display, Into};
use thiserror::Error;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
#[error("string cannot be empty")]
pub struct EmptyStringError;

#[derive(Clone, Debug, PartialEq, Eq, Deref, Display, Into)]
pub struct NonEmptyString(String);

impl NonEmptyString {
    pub fn new(value: impl Into<String>) -> Result<Self, EmptyStringError> {
        let value = value.into();
        if value.is_empty() {
            return Err(EmptyStringError);
        }
        Ok(Self(value))
    }
}

impl TryFrom<String> for NonEmptyString {
    type Error = EmptyStringError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for NonEmptyString {
    type Error = EmptyStringError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl FromStr for NonEmptyString {
    type Err = EmptyStringError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_strings_and_preserves_nonempty_values() {
        assert!(NonEmptyString::new("").is_err());
        assert!(NonEmptyString::try_from(String::new()).is_err());
        assert!(NonEmptyString::try_from("").is_err());
        assert!("".parse::<NonEmptyString>().is_err());

        for value in [" ", " /tmp/postgres 雪 ", "postgres"] {
            let parsed: NonEmptyString = value.parse().unwrap();
            assert_eq!(parsed.as_str(), value);
            assert_eq!(String::from(parsed), value);
        }
    }
}
