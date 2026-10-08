use std::net::{IpAddr, Ipv4Addr};

use derive_more::{Deref, Display, From, FromStr, Into};

/// IP address on which the TCP listener binds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deref, Display, From, FromStr, Into)]
pub struct ListenAddr(IpAddr);

impl Default for ListenAddr {
    fn default() -> Self {
        Self(Ipv4Addr::LOCALHOST.into())
    }
}
