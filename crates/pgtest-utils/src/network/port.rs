use std::{num::ParseIntError, ops::Deref, str::FromStr};

use derive_more::{Deref, Display, From, Into};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PortError {
    #[error("value can't be zero")]
    PortValueProvidedIsZero,
    #[error("value should be a value between 1-65535")]
    InvalidPortValueProvided,
}

impl From<ParseIntError> for PortError {
    fn from(_value: ParseIntError) -> Self {
        PortError::InvalidPortValueProvided
    }
}

#[derive(Clone, Copy, Debug, Display, Deref, Into, From)]
pub struct Port(u16);

impl FromStr for Port {
    type Err = PortError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value.parse()?).ok_or(PortError::PortValueProvidedIsZero)
    }
}

impl Port {
    pub fn new(port: u16) -> Option<Self> {
        if port < 1 {
            return None;
        }

        return Some(Self(port));
    }
}

#[derive(Clone, Copy, Debug, Display, From, derive_more::FromStr, Into)]
pub struct ListenerPort(u16);

impl ListenerPort {
    pub fn new(port: u16) -> Self {
        return Self(port);
    }

    pub fn get(self) -> u16 {
        self.0
    }
}

impl Deref for ListenerPort {
    type Target = u16;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
