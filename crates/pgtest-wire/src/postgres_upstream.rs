use std::{collections::BTreeMap, io};

use bytes::BytesMut;
use pgwire::{
    error::PgWireError,
    messages::{
        DecodeContext, Message,
        startup::{Authentication, Startup},
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::postgres_upstream::upstream_stream::UpstreamStream;

pub(crate) mod upstream_stream;

#[derive(Debug, thiserror::Error)]
pub(crate) enum UpstreamError {
    #[error("upstream I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("invalid upstream protocol message: {0}")]
    Protocol(#[from] PgWireError),
    #[error("upstream requires authentication; only trust authentication is supported")]
    UnsupportedAuthentication,
    #[error("upstream rejected the startup request")]
    StartupRejected,
    #[error("unexpected upstream authentication message tag {0:#x}")]
    UnexpectedMessage(u8),
    #[error("invalid upstream message length {0}")]
    InvalidFrameLength(i32),
}

pub struct RawBytes(BytesMut);
impl RawBytes {
    pub fn bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
impl From<BytesMut> for RawBytes {
    fn from(b: BytesMut) -> Self {
        RawBytes(b)
    }
}

pub(crate) struct UpstreamSession {
    pub(crate) stream: UpstreamStream,
    pub(crate) session_burst: RawBytes,
}

#[hotpath::measure]
pub(crate) async fn connect(
    target: &pgtest::backend::PgTarget,
    client_params: &BTreeMap<String, String>,
) -> Result<UpstreamSession, UpstreamError> {
    let (host, port) = match &target.endpoint {
        pgtest::backend::PgEndpoint::Tcp { host, port } => (host.clone(), *port),
        pgtest::backend::PgEndpoint::Unix { directory, port } => {
            (directory.to_string_lossy().into_owned(), *port)
        }
    };
    let mut stream = connect_stream(&host, port).await?;
    let mut decode_buffer = authenticate(&mut stream, &target.database, client_params).await?;

    let session_burst = wait_for_ready_for_query(&mut stream, &mut decode_buffer).await?;

    Ok(UpstreamSession { stream, session_burst })
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

// Preserve bytes after AuthenticationOk for the next stage.
#[hotpath::measure]
async fn authenticate(
    stream: &mut UpstreamStream,
    db_name: &str,
    client_params: &BTreeMap<String, String>,
) -> Result<BytesMut, UpstreamError> {
    send_startup(stream, db_name, client_params).await?;

    let mut decode_buffer = BytesMut::with_capacity(1024);
    let (tag, frame_end) =
        read_frame(stream, &mut decode_buffer, 0, Authentication::max_message_length()).await?;
    match tag {
        b'R' => {
            // Authentication::decode assumes the code and MD5 salt are present.
            // Validate those lengths before handing it a complete, isolated
            // frame.
            let length = (frame_end - 1) as i32;
            if length < 8 {
                return Err(UpstreamError::InvalidFrameLength(length));
            }
            let code = i32::from_be_bytes(decode_buffer[5..9].try_into().unwrap());
            if code == 5 && length < 12 {
                return Err(UpstreamError::InvalidFrameLength(length));
            }
            let mut frame = decode_buffer.split_to(frame_end);
            let authentication =
                hotpath::measure_block!("postgres_upstream::decode_authentication", {
                    Authentication::decode(&mut frame, &DecodeContext::default())
                })?;
            match authentication {
                Some(Authentication::Ok) => Ok(decode_buffer),
                Some(_) => Err(UpstreamError::UnsupportedAuthentication),
                None => Err(UpstreamError::InvalidFrameLength(length)),
            }
        }
        b'E' => Err(UpstreamError::StartupRejected),
        tag => Err(UpstreamError::UnexpectedMessage(tag)),
    }
}

#[hotpath::measure(future = true)]
async fn send_startup(
    stream: &mut UpstreamStream,
    db_name: &str,
    client_params: &BTreeMap<String, String>,
) -> Result<(), UpstreamError> {
    let mut upstream_startup = Startup::new();
    upstream_startup.parameters = forwardable(client_params);
    upstream_startup.parameters.insert("database".into(), db_name.to_owned());

    let mut out = BytesMut::with_capacity(256);
    hotpath::measure_block!(
        "postgres_upstream::encode_startup",
        upstream_startup.encode(&mut out)
    )?;
    hotpath::future!(stream.write_all(&out), label = "postgres_upstream::write_startup").await?;
    Ok(())
}

#[hotpath::measure]
fn forwardable(client_params: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    const OWNED: [&str; 2] = ["database", "replication"];
    client_params
        .iter()
        .filter(|(k, _)| !OWNED.contains(&k.as_str()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

#[hotpath::measure]
async fn wait_for_ready_for_query(
    stream: &mut UpstreamStream,
    buf: &mut BytesMut,
) -> Result<RawBytes, UpstreamError> {
    let mut cursor = 0;
    loop {
        let (tag, frame_end) = read_frame(stream, buf, cursor, i32::MAX as usize).await?;
        cursor = frame_end;
        if matches!(tag, b'Z' | b'E') {
            return Ok(RawBytes::from(buf.split_to(cursor)));
        }
    }
}

#[hotpath::measure(future = true)]
async fn read_frame(
    stream: &mut UpstreamStream,
    buf: &mut BytesMut,
    cursor: usize,
    max_length: usize,
) -> Result<(u8, usize), UpstreamError> {
    hotpath::future!(
        read_until(stream, buf, cursor + 5),
        label = "postgres_upstream::read_frame_header"
    )
    .await?;
    let length = i32::from_be_bytes(buf[cursor + 1..cursor + 5].try_into().unwrap());
    if length < 4 {
        return Err(UpstreamError::InvalidFrameLength(length));
    }
    if length as usize > max_length {
        return Err(PgWireError::MessageTooLarge(max_length, length as usize).into());
    }
    let frame_end =
        cursor.checked_add(1 + length as usize).ok_or(UpstreamError::InvalidFrameLength(length))?;
    hotpath::future!(
        read_until(stream, buf, frame_end),
        label = "postgres_upstream::read_frame_body"
    )
    .await?;
    Ok((buf[cursor], frame_end))
}

#[hotpath::measure]
async fn read_until(
    stream: &mut UpstreamStream,
    buf: &mut BytesMut,
    length: usize,
) -> io::Result<()> {
    while buf.len() < length {
        if hotpath::future!(stream.read_buf(buf), label = "postgres_upstream::read_socket").await?
            == 0
        {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "upstream closed during startup",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn connect_with_reply(reply: Vec<u8>) -> Result<UpstreamSession, UpstreamError> {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let backend = async {
                let (mut stream, _) = listener.accept().await.unwrap();
                let length = stream.read_i32().await.unwrap();
                let mut startup = vec![0; length as usize - 4];
                stream.read_exact(&mut startup).await.unwrap();
                // Exercise reads across partial headers and bodies.
                for chunk in reply.chunks(2) {
                    if stream.write_all(chunk).await.is_err() {
                        break; // The client may reject a header before the body arrives.
                    }
                    tokio::task::yield_now().await;
                }
            };
            let params = BTreeMap::from([("user".to_owned(), "postgres".to_owned())]);
            let target = pgtest::backend::PgTarget {
                database: "test".into(),
                endpoint: pgtest::backend::PgEndpoint::Tcp { host: "127.0.0.1".into(), port },
            };
            let (result, ()) = tokio::join!(connect(&target, &params), backend);
            result
        })
        .await
        .expect("mock upstream must finish")
    }

    #[tokio::test]
    async fn oversized_startup_returns_the_encoding_error() {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let mut stream =
            connect_stream("127.0.0.1", listener.local_addr().unwrap().port()).await.unwrap();
        let params = BTreeMap::from([(
            "application_name".to_owned(),
            "a".repeat(Startup::max_message_length()),
        )]);
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            authenticate(&mut stream, "test", &params),
        )
        .await
        .expect("encoding must fail before waiting for an upstream reply");
        assert!(matches!(result, Err(UpstreamError::Protocol(PgWireError::MessageTooLarge(_, _)))));
    }

    #[tokio::test]
    async fn upstream_failures_keep_their_error_kind() {
        assert!(matches!(connect_with_reply(vec![]).await,
            Err(UpstreamError::Io(error)) if error.kind() == io::ErrorKind::UnexpectedEof));
        assert!(matches!(
            connect_with_reply(b"X\0\0\0\x04".to_vec()).await,
            Err(UpstreamError::UnexpectedMessage(b'X'))
        ));
        assert!(matches!(
            connect_with_reply(b"E\0\0\0\x04".to_vec()).await,
            Err(UpstreamError::StartupRejected)
        ));
        assert!(matches!(
            connect_with_reply(b"R\0\0\0\x08\0\0\0\x03".to_vec()).await,
            Err(UpstreamError::UnsupportedAuthentication)
        ));
        assert!(matches!(
            connect_with_reply(b"R\0\0\0\x08\0\0\0\x63".to_vec()).await,
            Err(UpstreamError::Protocol(PgWireError::InvalidAuthenticationMessageCode(99)))
        ));
    }

    #[tokio::test]
    async fn malformed_upstream_lengths_return_errors_without_panicking() {
        for reply in [
            b"R\0\0\0\x04".to_vec(),
            b"R\0\0\0\x08\0\0\0\x05".to_vec(), // MD5 without its salt.
            b"R\0\0\0\x03".to_vec(),
            b"R\xff\xff\xff\xff".to_vec(),
            b"R\0\0\0\x08\0\0\0\0Z\0\0\0\x03".to_vec(),
        ] {
            assert!(matches!(
                connect_with_reply(reply).await,
                Err(UpstreamError::InvalidFrameLength(_))
            ));
        }
        let mut oversized = vec![b'R'];
        oversized
            .extend_from_slice(&((Authentication::max_message_length() + 1) as i32).to_be_bytes());
        assert!(matches!(
            connect_with_reply(oversized).await,
            Err(UpstreamError::Protocol(PgWireError::MessageTooLarge(_, _)))
        ));
    }

    #[tokio::test]
    async fn startup_preserves_the_opaque_burst_after_authentication() {
        for burst in [b"S\0\0\0\x08a\0b\0Z\0\0\0\x05I".as_slice(), b"E\0\0\0\x04".as_slice()] {
            let mut reply = b"R\0\0\0\x08\0\0\0\0".to_vec();
            reply.extend_from_slice(burst);
            let session = connect_with_reply(reply).await.unwrap();
            assert_eq!(session.session_burst.bytes(), burst);
        }
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
