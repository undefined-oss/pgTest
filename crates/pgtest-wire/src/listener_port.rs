use std::str::FromStr;

use derive_more::{Deref, Display, From, Into};
use pgtest_utils::network::port::{ListenerPort, PortError};

/// TCP listener port. Zero asks the OS to allocate an available port.
#[derive(Clone, Copy, Debug, Deref, Display, From, derive_more::FromStr, Into)]
pub struct TCPListenerPort(ListenerPort);

impl Default for TCPListenerPort {
    fn default() -> Self {
        Self(ListenerPort::new(6432))
    }
}

/// Nonzero port used in the Unix socket filename, independent of TCP.
#[derive(Clone, Copy, Debug, Deref, Display, Into)]
pub struct SocketListenerPort(ListenerPort);

impl Default for SocketListenerPort {
    fn default() -> Self {
        Self(ListenerPort::new(6432))
    }
}

impl TryFrom<ListenerPort> for SocketListenerPort {
    type Error = PortError;

    fn try_from(port: ListenerPort) -> Result<Self, Self::Error> {
        if port.get() == 0 {
            return Err(PortError::PortValueProvidedIsZero);
        }
        Ok(Self(port))
    }
}

impl FromStr for SocketListenerPort {
    type Err = PortError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::try_from(value.parse::<ListenerPort>()?)
    }
}
