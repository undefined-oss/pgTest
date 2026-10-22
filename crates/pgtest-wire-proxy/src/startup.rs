//! Drive startup negotiation using pgwire's codec on any Tokio-compatible I/O.

use std::{io, net::SocketAddr};

use futures::{SinkExt, StreamExt};
use pgwire::{
    api::{ClientInfo, DefaultClient, PgWireConnectionState},
    error::{ErrorInfo, PgWireError, PgWireResult},
    messages::{
        PgWireBackendMessage, PgWireFrontendMessage, SslNegotiationMetaMessage,
        response::{GssEncResponse, SslResponse},
        startup::Startup,
    },
    tokio::server::PgWireMessageServerCodec,
};
use pingora_core::protocols::Stream;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;

use crate::control_panel::PgTestQueryTypeControlStatement;

/// The codec owns framing and keeps any bytes read beyond the startup message.
pub(crate) type ClientConnection<S> =
    Framed<S, PgWireMessageServerCodec<PgTestQueryTypeControlStatement>>;

/// Adapt an accepted Pingora TCP or Unix stream to pgwire's client metadata.
pub(crate) fn accepted(stream: Stream) -> io::Result<ClientConnection<Stream>> {
    let digest = stream.get_socket_digest().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "accepted stream has no socket metadata")
    })?;
    if let Some(peer) = digest.peer_addr().and_then(|address| address.as_inet()) {
        return Ok(connection(stream, *peer));
    }
    #[cfg(unix)]
    if digest.local_addr().and_then(|address| address.as_unix()).is_some() {
        // Unix clients commonly have unnamed peers. Pgwire requires an IP even
        // for Unix transports, so use a loopback placeholder.
        // This is codec metadata only, never an authentication/routing
        // identity.
        return Ok(connection(stream, ([127, 0, 0, 1], 0).into()));
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "accepted stream has no supported socket address",
    ))
}

/// Wrap an accepted plaintext stream with pgwire's server codec.
pub(crate) fn connection<S>(stream: S, peer: SocketAddr) -> ClientConnection<S> {
    let client = DefaultClient::new(peer, false);
    Framed::new(stream, PgWireMessageServerCodec::new(client))
}

/// Send and flush a PostgreSQL startup failure before the caller closes I/O.
pub(crate) async fn reject<S>(
    connection: &mut ClientConnection<S>,
    code: &str,
    message: &str,
) -> PgWireResult<()>
where
    S: AsyncRead + AsyncWrite + Unpin + 'static,
{
    let error = ErrorInfo::new("FATAL".into(), code.into(), message.into());
    connection.send(PgWireBackendMessage::ErrorResponse(error.into())).await?;
    Ok(())
}

/// Decline SSL/GSS requests with `N`, allowing clients to proceed in plaintext.
/// The caller applies one deadline to the entire negotiation and startup read.
/// Keep the framed connection after this returns; `into_parts()` preserves its
/// read buffer when we later switch to byte forwarding.
/// TLS upgrades, direct TLS, and PostgreSQL CancelRequest handling are not
/// implemented yet.
#[hotpath::measure(future = true)]
pub(crate) async fn read<S>(connection: &mut ClientConnection<S>) -> PgWireResult<Startup>
where
    S: AsyncRead + AsyncWrite + Unpin + 'static,
{
    let mut ssl_seen = false;
    let mut gss_seen = false;
    while let Some(message) = connection.next().await {
        match message? {
            PgWireFrontendMessage::SslNegotiation(SslNegotiationMetaMessage::PostgresSsl(_))
                if !ssl_seen =>
            {
                ssl_seen = true;
                connection.send(PgWireBackendMessage::SslResponse(SslResponse::Refuse)).await?;
            }
            PgWireFrontendMessage::SslNegotiation(SslNegotiationMetaMessage::PostgresGss(_))
                if !gss_seen =>
            {
                gss_seen = true;
                connection
                    .send(PgWireBackendMessage::GssEncResponse(GssEncResponse::Refuse))
                    .await?;
            }
            PgWireFrontendMessage::SslNegotiation(SslNegotiationMetaMessage::None) => {
                // The codec has identified a non-encryption packet without
                // consuming it. Decode it as startup on the next iteration.
                connection.set_state(PgWireConnectionState::AwaitingStartup);
            }
            PgWireFrontendMessage::Startup(startup) => return Ok(startup),
            _ => return Err(PgWireError::InvalidStartupMessage),
        }
    }
    Err(io::Error::new(io::ErrorKind::UnexpectedEof, "connection closed before startup").into())
}

#[cfg(test)]
mod tests {
    use std::io;

    use pgwire::error::PgWireError;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

    /// Test connection metadata for in-memory transports.
    fn connection<S>(stream: S) -> super::ClientConnection<S> {
        super::connection(stream, ([127, 0, 0, 1], 54321).into())
    }

    /// Reconstruct the byte stream when leaving the framed protocol layer.
    async fn assert_following_bytes<S: AsyncRead + Unpin>(framed: super::ClientConnection<S>) {
        let parts = framed.into_parts();
        let mut stream = io::Cursor::new(parts.read_buf).chain(parts.io);
        let mut following = [0; 4];
        stream.read_exact(&mut following).await.unwrap();
        assert_eq!(&following, b"next");
    }

    /// Independent wire fixture: protocol 3.0, user alice, database
    /// template/demo.
    const STARTUP: &[u8] =
        b"\x00\x00\x00\x2b\x00\x03\x00\x00user\x00alice\x00database\x00template/demo\x00\x00";

    #[tokio::test]
    async fn fragmented_startup_preserves_parameters_and_following_bytes() {
        // Capacity one forces partial reads, including inside the length
        // prefix.
        let (mut client, server) = tokio::io::duplex(1);
        let mut server = connection(server);
        let sender = async {
            client.write_all(STARTUP).await.unwrap();
            client.write_all(b"next").await.unwrap();
        };
        let receiver = async {
            let startup = super::read(&mut server).await.unwrap();
            assert_eq!(startup.protocol_number_major, 3);
            assert_eq!(startup.protocol_number_minor, 0);
            assert_eq!(startup.parameters.get("user").unwrap(), "alice");
            assert_eq!(startup.parameters.get("database").unwrap(), "template/demo");
            assert_following_bytes(server).await;
        };
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            tokio::join!(sender, receiver);
        })
        .await
        .expect("startup reader must preserve bytes after the packet");
    }

    #[tokio::test]
    async fn invalid_startup_lengths_are_rejected_by_pgwire() {
        for length in [0_u32, 3, 7, 8, 1_000_000, u32::MAX] {
            // Pgwire identifies the initial message using its first eight
            // bytes.
            let mut packet = length.to_be_bytes().to_vec();
            packet.extend_from_slice(&[0, 3, 0, 0]);
            let mut server = connection(io::Cursor::new(packet));
            let error = super::read(&mut server).await.unwrap_err();
            assert!(matches!(
                error,
                PgWireError::InvalidStartupMessage | PgWireError::MessageTooLarge(..)
            ));
        }
    }

    #[tokio::test]
    async fn truncated_packets_and_unsupported_protocols_are_rejected() {
        for length in [0, 3, 4, STARTUP.len() - 1] {
            let mut server = connection(io::Cursor::new(STARTUP[..length].to_vec()));
            assert!(super::read(&mut server).await.is_err());
        }
        let mut packet = STARTUP.to_vec();
        packet[5] = 2; // Unsupported protocol major version 2.
        let mut server = connection(io::Cursor::new(packet));
        let error = super::read(&mut server).await.unwrap_err();
        assert!(matches!(error, PgWireError::InvalidStartupMessage));
    }

    #[tokio::test]
    async fn coalesced_startup_keeps_following_bytes_in_the_framed_buffer() {
        let mut packet = STARTUP.to_vec();
        packet.extend_from_slice(b"next");
        let mut server = connection(io::Cursor::new(packet));
        let startup = super::read(&mut server).await.unwrap();
        assert_eq!(startup.parameters.get("database").unwrap(), "template/demo");
        assert_eq!(server.read_buffer().as_ref(), b"next");
        assert_following_bytes(server).await;
    }

    /// PostgreSQL's eight-byte SSLRequest and GSSENCRequest packets.
    const SSL_REQUEST: &[u8] = b"\x00\x00\x00\x08\x04\xd2\x16\x2f";
    const GSS_REQUEST: &[u8] = b"\x00\x00\x00\x08\x04\xd2\x16\x30";

    #[tokio::test]
    async fn startup_rejection_is_flushed_before_connection_closes() {
        let (mut client, server) = tokio::io::duplex(1);
        let mut server = connection(tokio::io::BufStream::new(server));
        let sender = async {
            client.write_all(STARTUP).await.unwrap();
            // Independent PostgreSQL wire fixture: E, length, severity,
            // SQLSTATE, message, and the terminating zero byte.
            let expected = b"E\x00\x00\x00\x1eSFATAL\x00C0A000\x00Mnot ready\x00\x00";
            let mut response = vec![0; expected.len()];
            client.read_exact(&mut response).await.unwrap();
            assert_eq!(response, expected);
        };
        let receiver = async {
            super::read(&mut server).await.unwrap();
            super::reject(&mut server, "0A000", "not ready").await.unwrap();
        };
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            tokio::join!(sender, receiver);
        })
        .await
        .expect("the client must receive the error while the server stream remains open");
    }

    #[tokio::test]
    async fn encryption_refusals_allow_startup_in_either_order() {
        for requests in [
            vec![SSL_REQUEST],
            vec![GSS_REQUEST],
            vec![SSL_REQUEST, GSS_REQUEST],
            vec![GSS_REQUEST, SSL_REQUEST],
        ] {
            let (mut client, server) = tokio::io::duplex(1);
            // Buffer writes to verify that each refusal is explicitly flushed.
            let mut server = connection(tokio::io::BufStream::new(server));
            let sender = async {
                for request in requests {
                    client.write_all(request).await.unwrap();
                    assert_eq!(client.read_u8().await.unwrap(), b'N');
                }
                client.write_all(STARTUP).await.unwrap();
                client.write_all(b"next").await.unwrap();
            };
            let receiver = async {
                let startup = super::read(&mut server).await.unwrap();
                assert_eq!(startup.parameters.get("database").unwrap(), "template/demo");
                assert_following_bytes(server).await;
            };
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                tokio::join!(sender, receiver);
            })
            .await
            .expect("client must receive refusal before sending the next packet");
        }
    }

    #[tokio::test]
    async fn repeated_encryption_requests_are_rejected() {
        for request in [SSL_REQUEST, GSS_REQUEST] {
            let (mut client, server) = tokio::io::duplex(64);
            let mut server = connection(server);
            let sender = async {
                client.write_all(request).await.unwrap();
                assert_eq!(client.read_u8().await.unwrap(), b'N');
                client.write_all(request).await.unwrap();
            };
            let receiver = async {
                let error = super::read(&mut server).await.unwrap_err();
                assert!(matches!(error, PgWireError::InvalidStartupMessage));
            };
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                tokio::join!(sender, receiver);
            })
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn truncated_packet_after_encryption_refusal_is_rejected() {
        for request in [SSL_REQUEST, GSS_REQUEST] {
            let (mut client, server) = tokio::io::duplex(64);
            let mut server = connection(server);
            let sender = async {
                client.write_all(request).await.unwrap();
                assert_eq!(client.read_u8().await.unwrap(), b'N');
                client.write_all(&[0]).await.unwrap();
                client.shutdown().await.unwrap();
            };
            let receiver = async {
                assert!(super::read(&mut server).await.is_err());
            };
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                tokio::join!(sender, receiver);
            })
            .await
            .unwrap();
        }
    }
}

#[cfg(all(test, unix))]
mod accepted_tests {
    use std::{
        os::unix::{fs::PermissionsExt, io::AsRawFd},
        sync::Arc,
        time::Duration,
    };

    use async_trait::async_trait;
    use pgwire::api::ClientInfo;
    use pingora_core::{
        apps::ServerApp,
        protocols::{GetSocketDigest, SocketDigest, Stream, l4::stream::Stream as L4Stream},
        server::ShutdownWatch,
        services::{Service as ServiceTrait, listening::Service},
    };
    use tokio::{
        net::{TcpListener, TcpStream, UnixStream},
        sync::watch,
    };
    use tokio_postgres::{Config, NoTls};

    struct StartupProbe;

    #[async_trait]
    impl ServerApp for StartupProbe {
        async fn process_new(
            self: &Arc<Self>,
            stream: Stream,
            _: &ShutdownWatch,
        ) -> Option<Stream> {
            // Exercise the same adapter used by WireProxy::process_new, with
            // the socket metadata populated by a real Pingora Unix
            // listener.
            let digest = stream.get_socket_digest().unwrap();
            assert!(digest.local_addr().unwrap().as_unix().is_some());
            let mut connection = super::accepted(stream).unwrap();
            assert_eq!(connection.socket_addr(), ([127, 0, 0, 1], 0).into());
            let startup = super::read(&mut connection).await.unwrap();
            assert_eq!(startup.parameters["database"], "template/demo");
            assert_eq!(startup.parameters["user"], "alice");
            super::reject(&mut connection, "0A000", "Unix startup received").await.unwrap();
            None
        }
    }

    #[tokio::test]
    async fn pingora_unix_listener_accepts_unnamed_postgres_clients() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let directory =
                tempfile::Builder::new().prefix("pgtest-wire-").tempdir_in("/tmp").unwrap();
            let path = directory.path().join(".s.PGSQL.6433");
            let mut service = Service::new("unix-test".into(), StartupProbe);
            service.add_uds(path.to_str().unwrap(), Some(std::fs::Permissions::from_mode(0o600)));
            let (shutdown, watch) = watch::channel(false);
            let task = tokio::spawn(async move { service.start_service(None, watch, 1).await });
            while !path.exists() {
                assert!(!task.is_finished(), "listener stopped before binding");
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            let mut config = Config::new();
            config.host_path(directory.path()).port(6433).user("alice").dbname("template/demo");
            let error = match config.connect(NoTls).await {
                Err(error) => error,
                Ok(_) => panic!("the probe only accepts startup"),
            };
            let error = error.as_db_error().expect("Unix client must receive the pgwire response");
            assert_eq!(error.code().code(), "0A000");
            assert_eq!(error.message(), "Unix startup received");
            assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
            shutdown.send(true).unwrap();
            task.await.unwrap();
        })
        .await
        .expect("Unix startup and listener shutdown must finish");
    }

    #[tokio::test]
    async fn tcp_metadata_is_preserved_and_unknown_transports_are_not_assumed_unix() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let _client = TcpStream::connect(listener.local_addr().unwrap()).await.unwrap();
        let (socket, peer) = listener.accept().await.unwrap();
        let digest = SocketDigest::from_raw_fd(socket.as_raw_fd());
        let mut stream = L4Stream::from(socket);
        stream.set_socket_digest(digest);
        let connection = super::accepted(Box::new(stream)).unwrap();
        assert_eq!(connection.socket_addr(), peer);

        let (socket, _peer) = UnixStream::pair().unwrap();
        // A raw wrapper has no digest until the listener populates it. The
        // adapter must not silently treat any missing TCP address as a
        // local Unix client.
        assert!(super::accepted(Box::new(L4Stream::from(socket))).is_err());
    }
}
