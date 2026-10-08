use std::path::PathBuf;

use derive_more::{Deref, From, FromStr, Into};

/// Directory in which the frontend Unix socket is created.
#[derive(Clone, Debug, PartialEq, Eq, Deref, From, FromStr, Into)]
pub struct UnixSocketDir(PathBuf);
