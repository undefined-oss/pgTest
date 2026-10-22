//! Open a fresh Pingora stream and forward startup using pgwire's encoder.

use std::{io, time::Duration};

use bytes::BytesMut;
use pgtest_engine_backend::{PgEndpoint, PgTarget};
use pgwire::{
    error::PgWireResult,
    messages::{Message, startup::Startup},
};
use pingora_core::{
    connectors::TransportConnector,
    protocols::Stream,
    upstreams::peer::{BasicPeer, PeerOptions},
};
use tokio::io::{AsyncWrite, AsyncWriteExt};

/// The caller bounds DNS, connect, and startup writes with a deadline and the
/// lease cancellation token. This does not wait for a PostgreSQL response.
#[hotpath::measure(future = true)]
pub(crate) async fn connect(
    connector: &TransportConnector,
    target: &PgTarget,
    startup: Startup,
) -> PgWireResult<Stream> {
    let mut stream = connect_stream(connector, &target.endpoint).await?;
    send_startup(&mut stream, &target.database, startup).await?;
    Ok(stream)
}

#[hotpath::measure(future = true)]
async fn connect_stream(
    connector: &TransportConnector,
    endpoint: &PgEndpoint,
) -> io::Result<Stream> {
    match endpoint {
        PgEndpoint::Tcp { host, port } => {
            // BasicPeer expects a socket address, so resolve asynchronously and
            // try each result (e.g. IPv6 then IPv4 for localhost).
            let addresses = tokio::net::lookup_host((host.as_str(), *port)).await?;
            let mut last_error =
                io::Error::new(io::ErrorKind::NotFound, "hostname has no addresses");
            for address in addresses {
                let mut peer = BasicPeer {
                    _address: address.into(),
                    sni: String::new(),
                    options: PeerOptions::new(),
                };
                // PostgreSQL sessions cannot use an unreset generic idle pool.
                peer.options.idle_timeout = Some(Duration::ZERO);
                match connector.new_stream(&peer).await {
                    Ok(stream) => return Ok(stream),
                    Err(error) => last_error = io::Error::other(error),
                }
            }
            Err(last_error)
        }
        PgEndpoint::Unix { directory, port } => {
            #[cfg(unix)]
            {
                let mut peer = BasicPeer::new_uds(directory.join(format!(".s.PGSQL.{port}")))
                    .map_err(io::Error::other)?;
                // PostgreSQL sessions cannot use an unreset generic idle pool.
                peer.options.idle_timeout = Some(Duration::ZERO);
                connector.new_stream(&peer).await.map_err(io::Error::other)
            }
            #[cfg(not(unix))]
            {
                let _ = (directory, port);
                Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "Unix upstream sockets are unsupported",
                ))
            }
        }
    }
}

#[hotpath::measure(future = true)]
async fn send_startup<S>(stream: &mut S, database: &str, mut startup: Startup) -> PgWireResult<()>
where
    S: AsyncWrite + Unpin,
{
    startup.parameters.insert("database".into(), database.to_owned());
    // Pgwire owns length checks, framing, and field encoding. No backend reads
    // occur here, so its first authentication response stays on the stream.
    let mut packet = BytesMut::new();
    startup.encode(&mut packet)?;
    stream.write_all(&packet).await?;
    stream.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use pgwire::{error::PgWireError, messages::Message};
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

    use super::*;
    use crate::startup;

    fn client_startup() -> Startup {
        let mut startup = Startup::new();
        startup.protocol_number_minor = 2;
        startup.parameters.extend([
            ("database".into(), "template/demo".into()),
            ("user".into(), "alice".into()),
            ("application_name".into(), "learning-session".into()),
            ("options".into(), "-c search_path=public".into()),
            ("_pq_.custom".into(), "preserved".into()),
        ]);
        startup
    }

    async fn read_startup<S: AsyncRead + AsyncWrite + Unpin + 'static>(
        peer: S,
    ) -> startup::ClientConnection<S> {
        let mut framed = startup::connection(peer, ([127, 0, 0, 1], 5432).into());
        let forwarded = startup::read(&mut framed).await.unwrap();
        let mut expected = client_startup();
        expected.parameters.insert("database".into(), "allocated_database".into());
        assert_eq!(forwarded, expected);
        framed
    }

    #[tokio::test]
    async fn startup_rewrites_only_database_and_flushes_fragmented_writes() {
        let (sender, receiver) = tokio::io::duplex(1);
        let mut sender = tokio::io::BufWriter::new(sender);
        tokio::time::timeout(Duration::from_secs(3), async {
            let (sent, _) = tokio::join!(
                send_startup(&mut sender, "allocated_database", client_startup()),
                read_startup(receiver),
            );
            sent.unwrap();
        })
        .await
        .expect("the startup packet must be flushed without closing the stream");
    }

    #[tokio::test]
    async fn oversized_startup_fails_before_writing() {
        let mut startup = client_startup();
        startup
            .parameters
            .insert("application_name".into(), "a".repeat(Startup::max_message_length()));
        let mut output = Vec::new();
        let error = send_startup(&mut output, "allocated_database", startup).await.unwrap_err();
        assert!(matches!(error, PgWireError::MessageTooLarge(..)));
        assert!(output.is_empty());
    }

    #[tokio::test]
    async fn tcp_hostname_opens_fresh_streams_and_does_not_wait_for_backend_reply() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
            let target = PgTarget {
                database: "allocated_database".into(),
                endpoint: PgEndpoint::Tcp {
                    host: "localhost".into(),
                    port: listener.local_addr().unwrap().port(),
                },
            };
            let connector = TransportConnector::new(None);
            // Each connection must deliver its own startup packet, even when
            // the connector and target are the same. No backend
            // response is needed.
            for _ in 0..2 {
                let mut stream = connect(&connector, &target, client_startup()).await.unwrap();
                let (peer, _) = listener.accept().await.unwrap();
                let mut peer = read_startup(peer).await;
                let challenge = b"R\0\0\0\x08\0\0\0\x03";
                peer.get_mut().write_all(challenge).await.unwrap();
                let mut received = [0; 9];
                stream.read_exact(&mut received).await.unwrap();
                assert_eq!(&received, challenge, "connect must leave backend responses unread");
            }
        })
        .await
        .expect("connect must finish without authentication or ReadyForQuery");
    }

    #[tokio::test]
    async fn refused_tcp_connection_returns_an_error() {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let endpoint = PgEndpoint::Tcp {
            host: "127.0.0.1".into(),
            port: listener.local_addr().unwrap().port(),
        };
        drop(listener);
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            connect_stream(&TransportConnector::new(None), &endpoint),
        )
        .await
        .unwrap();
        assert!(result.is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unix_uses_postgres_socket_name_and_reports_missing_socket() {
        tokio::time::timeout(Duration::from_secs(5), async {
            // Keep the path short enough for macOS's Unix socket path limit.
            let directory = tempfile::tempdir_in("/tmp").unwrap();
            let path = directory.path().join(".s.PGSQL.5433");
            let listener = tokio::net::UnixListener::bind(&path).unwrap();
            let target = PgTarget {
                database: "allocated_database".into(),
                endpoint: PgEndpoint::Unix { directory: directory.path().into(), port: 5433 },
            };
            let connector = TransportConnector::new(None);
            let _stream = connect(&connector, &target, client_startup()).await.unwrap();
            let (peer, _) = listener.accept().await.unwrap();
            read_startup(peer).await;
            let missing = PgEndpoint::Unix { directory: directory.path().into(), port: 5434 };
            assert!(connect_stream(&connector, &missing).await.is_err());
        })
        .await
        .unwrap();
    }
}
