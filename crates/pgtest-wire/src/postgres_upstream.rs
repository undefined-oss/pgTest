use std::io;

use bytes::BytesMut;
use pgtest_engine_backend::{PgEndpoint, PgTarget};
use pgwire::{
    error::PgWireError,
    messages::{Message, startup::Startup},
};
use tokio::{io::AsyncWriteExt, net::TcpStream};

use crate::postgres_upstream::upstream_stream::UpstreamStream;

pub(crate) mod upstream_stream;

#[derive(Debug, thiserror::Error)]
pub(crate) enum UpstreamError {
    #[error("upstream I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("invalid upstream protocol message: {0}")]
    Protocol(#[from] PgWireError),
}

/// Forward startup, then leave authentication and protocol negotiation to the
/// client and PostgreSQL through the byte relay.
#[hotpath::measure]
pub(crate) async fn connect(
    target: &PgTarget,
    startup: Startup,
) -> Result<UpstreamStream, UpstreamError> {
    let (host, port) = match &target.endpoint {
        PgEndpoint::Tcp { host, port } => (host.clone(), *port),
        PgEndpoint::Unix { directory, port } => (directory.to_string_lossy().into_owned(), *port),
    };
    let mut stream = connect_stream(&host, port).await?;
    send_startup(&mut stream, &target.database, startup).await?;
    Ok(stream)
}

#[hotpath::measure]
async fn connect_stream(host: &str, port: u16) -> Result<UpstreamStream, UpstreamError> {
    if host.starts_with('/') {
        #[cfg(unix)]
        {
            let path = std::path::Path::new(host).join(format!(".s.PGSQL.{port}"));
            return Ok(UpstreamStream::Unix(tokio::net::UnixStream::connect(path).await?));
        }
        #[cfg(not(unix))]
        {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Unix upstream sockets are unsupported on this platform",
            )
            .into());
        }
    }
    Ok(UpstreamStream::Tcp(connect_tcp(host, port).await?))
}

#[hotpath::measure]
async fn connect_tcp(host: &str, port: u16) -> io::Result<TcpStream> {
    let stream = TcpStream::connect((host, port)).await?;
    stream.set_nodelay(true)?;
    Ok(stream)
}

#[hotpath::measure(future = true)]
async fn send_startup(
    stream: &mut UpstreamStream,
    db_name: &str,
    mut startup: Startup,
) -> Result<(), UpstreamError> {
    startup.parameters.insert("database".into(), db_name.to_owned());
    let mut out = BytesMut::with_capacity(256);
    hotpath::measure_block!("postgres_upstream::encode_startup", startup.encode(&mut out))?;
    hotpath::future!(stream.write_all(&out), label = "postgres_upstream::write_startup").await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use pgwire::messages::DecodeContext;
    use tokio::io::AsyncReadExt;

    use super::*;

    #[tokio::test]
    async fn connect_forwards_startup_without_waiting_for_a_reply() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
            let target = PgTarget {
                database: "allocated_database".into(),
                endpoint: PgEndpoint::Tcp {
                    host: "127.0.0.1".into(),
                    port: listener.local_addr().unwrap().port(),
                },
            };
            let mut startup = Startup::new();
            startup.protocol_number_minor = 2;
            startup.parameters.extend([
                ("database".into(), "template/lease".into()),
                ("user".into(), "application_user".into()),
                ("application_name".into(), "suite".into()),
                ("_pq_.custom".into(), "preserved".into()),
            ]);
            let mut expected = startup.parameters.clone();
            expected.insert("database".into(), target.database.clone());
            // The peer has not sent any response: connect must still finish.
            let _stream = connect(&target, startup).await.unwrap();
            let (mut peer, _) = listener.accept().await.unwrap();
            let length = peer.read_i32().await.unwrap();
            let mut bytes = vec![0; length as usize];
            bytes[..4].copy_from_slice(&length.to_be_bytes());
            peer.read_exact(&mut bytes[4..]).await.unwrap();
            let forwarded =
                Startup::decode(&mut BytesMut::from(bytes.as_slice()), &DecodeContext::default())
                    .unwrap()
                    .unwrap();
            assert_eq!(forwarded.protocol_number_major, 3);
            assert_eq!(forwarded.protocol_number_minor, 2);
            assert_eq!(forwarded.parameters, expected);
        })
        .await
        .expect("connect must not wait for authentication");
    }

    #[tokio::test]
    async fn oversized_startup_returns_the_encoding_error() {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let mut stream =
            connect_stream("127.0.0.1", listener.local_addr().unwrap().port()).await.unwrap();
        let mut startup = Startup::new();
        startup
            .parameters
            .insert("application_name".into(), "a".repeat(Startup::max_message_length()));
        let result = send_startup(&mut stream, "test", startup).await;
        assert!(matches!(result, Err(UpstreamError::Protocol(PgWireError::MessageTooLarge(_, _)))));
    }

    #[tokio::test]
    async fn tcp_uses_configured_hostname_and_port() {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let mut stream = connect_stream("localhost", port).await.unwrap();
        let (mut peer, _) = listener.accept().await.unwrap();
        assert!(matches!(&stream, UpstreamStream::Tcp(tcp) if tcp.nodelay().unwrap()));
        stream.write_all(b"request").await.unwrap();
        let mut request = [0; 7];
        peer.read_exact(&mut request).await.unwrap();
        assert_eq!(&request, b"request");
        peer.write_all(b"reply").await.unwrap();
        let mut reply = [0; 5];
        stream.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"reply");
        stream.shutdown().await.unwrap();
        assert_eq!(peer.read(&mut request).await.unwrap(), 0);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unix_uses_directory_and_port_and_reports_missing_socket() {
        struct Directory(std::path::PathBuf);
        impl Drop for Directory {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let directory =
            Directory(std::env::temp_dir().join(format!("pgu-up-{}", std::process::id())));
        std::fs::create_dir(&directory.0).unwrap();
        let host = directory.0.to_str().unwrap();
        let listener = crate::unix_listener::BoundUnixListener::bind(&directory.0, 5433).unwrap();
        let mut stream = connect_stream(host, 5433).await.unwrap();
        assert!(matches!(stream, UpstreamStream::Unix(_)));
        let mut peer = listener.accept().await.unwrap();
        stream.write_all(b"request").await.unwrap();
        stream.flush().await.unwrap();
        let mut request = [0; 7];
        peer.read_exact(&mut request).await.unwrap();
        assert_eq!(&request, b"request");
        peer.write_all(b"reply").await.unwrap();
        let mut reply = [0; 5];
        stream.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"reply");
        stream.shutdown().await.unwrap();
        assert_eq!(peer.read(&mut request).await.unwrap(), 0);
        assert!(connect_stream(host, 5434).await.is_err());
        drop(listener);
        assert!(connect_stream(host, 5433).await.is_err());
    }

    #[cfg(not(unix))]
    #[tokio::test]
    async fn unix_endpoint_is_rejected_on_unsupported_platforms() {
        assert!(connect_stream("/var/run/postgresql", 5432).await.is_err());
    }
}
