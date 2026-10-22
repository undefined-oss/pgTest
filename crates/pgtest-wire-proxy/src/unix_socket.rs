//! Socket-file ownership shared by all duplicates of a listener.
use std::{
    io,
    num::NonZeroU16,
    os::unix::{
        fs::{FileTypeExt, MetadataExt},
        net::UnixListener,
    },
    path::{Path, PathBuf},
    sync::Arc,
};

pub(crate) struct SocketFile {
    path: PathBuf,
    device: u64,
    inode: u64,
}

impl SocketFile {
    pub(crate) fn bind(
        directory: &Path,
        port: NonZeroU16,
    ) -> io::Result<(UnixListener, Arc<Self>)> {
        let path = directory.canonicalize()?.join(format!(".s.PGSQL.{port}"));
        // Binding must fail if a socket or any other file already exists.
        let listener = UnixListener::bind(&path)?;
        let metadata = std::fs::symlink_metadata(&path)?;
        let socket = Self { path, device: metadata.dev(), inode: metadata.ino() };
        Ok((listener, Arc::new(socket)))
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for SocketFile {
    fn drop(&mut self) {
        if let Ok(metadata) = std::fs::symlink_metadata(&self.path) {
            if metadata.file_type().is_socket()
                && metadata.dev() == self.device
                && metadata.ino() == self.inode
            {
                if let Err(error) = std::fs::remove_file(&self.path) {
                    tracing::warn!(path = %self.path.display(), %error, "unable to remove Unix socket");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn directory() -> tempfile::TempDir {
        tempfile::Builder::new().prefix("pgu-").tempdir_in("/tmp").unwrap()
    }

    #[test]
    fn failed_second_bind_preserves_live_socket_and_last_owner_removes_it() {
        let directory = directory();
        let port = NonZeroU16::new(6432).unwrap();
        let (listener, socket) = SocketFile::bind(directory.path(), port).unwrap();
        let path = socket.path().to_owned();
        assert!(SocketFile::bind(directory.path(), port).is_err());
        let duplicate = listener.try_clone().unwrap();
        let owner = socket.clone();
        drop((listener, socket));
        let client = std::os::unix::net::UnixStream::connect(&path).unwrap();
        let (server, _) = duplicate.accept().unwrap();
        drop((client, server, duplicate, owner));
        assert!(!path.exists());
    }

    #[test]
    fn existing_file_and_replacement_path_are_preserved() {
        let directory = directory();
        let port = NonZeroU16::new(6432).unwrap();
        let path = directory.path().join(".s.PGSQL.6432");
        std::fs::write(&path, "existing").unwrap();
        assert!(SocketFile::bind(directory.path(), port).is_err());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "existing");
        std::fs::remove_file(&path).unwrap();
        let (listener, socket) = SocketFile::bind(directory.path(), port).unwrap();
        std::fs::remove_file(&path).unwrap();
        std::fs::write(&path, "replacement").unwrap();
        drop((listener, socket));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "replacement");
    }
}
