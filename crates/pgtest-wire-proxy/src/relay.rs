//! Relay opaque PostgreSQL bytes after startup routing, including
//! authentication.

use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::BytesMut;
use futures::SinkExt;
use pingora_core::server::ShutdownWatch;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf, copy_bidirectional_with_sizes};
use tokio_util::sync::CancellationToken;

use crate::startup::ClientConnection;

const RELAY_BUFFER_SIZE: usize = 64 * 1024;

/// Replay pgwire's read-ahead bytes while writes go directly to the client.
/// Keeping both directions in one stream lets Tokio relay without split locks.
struct PrefixedStream<S> {
    prefix: io::Cursor<BytesMut>,
    stream: S,
}

impl<S: AsyncRead + Unpin> AsyncRead for PrefixedStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.prefix.position() < this.prefix.get_ref().len() as u64 {
            Pin::new(&mut this.prefix).poll_read(cx, buf)
        } else {
            Pin::new(&mut this.stream).poll_read(cx, buf)
        }
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for PrefixedStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().stream).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().stream).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.stream.is_write_vectored()
    }
}

/// Own both streams so cancellation or an I/O error closes the entire session.
#[hotpath::measure(future = true)]
pub(crate) async fn run<C, U>(
    mut client: ClientConnection<C>,
    upstream: U,
    cancellation: CancellationToken,
    mut shutdown: ShutdownWatch,
) -> io::Result<()>
where
    C: AsyncRead + AsyncWrite + Unpin + 'static,
    U: AsyncRead + AsyncWrite + Unpin,
{
    let relay = async {
        // All negotiation responses must reach the client before raw relay.
        client.flush().await?;
        let parts = client.into_parts();
        let mut upstream = upstream;
        // Deliver pgwire's read-ahead bytes first, exactly once. Both
        // directions run concurrently even while this prefix is blocked
        // by backpressure.
        let mut client =
            PrefixedStream { prefix: io::Cursor::new(parts.read_buf), stream: parts.io };
        // Tokio handles half-close in each direction while continuing to
        // copy the other direction, with 64 KiB buffers in both directions.
        copy_bidirectional_with_sizes(
            &mut client,
            &mut upstream,
            RELAY_BUFFER_SIZE,
            RELAY_BUFFER_SIZE,
        )
        .await?;
        Ok(())
    };
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => Ok(()),
        _ = shutdown.wait_for(|stopping| *stopping) => Ok(()),
        result = relay => result,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::BytesMut;
    use pgwire::messages::{Message, startup::Startup};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt, DuplexStream},
        sync::watch,
    };

    use super::*;
    use crate::startup;

    fn framed(stream: DuplexStream) -> ClientConnection<DuplexStream> {
        startup::connection(stream, ([127, 0, 0, 1], 54321).into())
    }

    #[tokio::test]
    async fn buffered_bytes_authentication_and_backend_errors_flow_in_order() {
        tokio::time::timeout(Duration::from_secs(3), async {
            let (mut client, proxy_client) = tokio::io::duplex(1024);
            let (proxy_upstream, mut backend) = tokio::io::duplex(1024);
            let mut connection = framed(proxy_client);
            let mut packet = BytesMut::new();
            Startup::new().encode(&mut packet).unwrap();
            packet.extend_from_slice(b"buffered");
            client.write_all(&packet).await.unwrap();
            startup::read(&mut connection).await.unwrap();
            assert_eq!(connection.read_buffer().as_ref(), b"buffered");
            let (_shutdown, watch) = watch::channel(false);
            let relay =
                tokio::spawn(run(connection, proxy_upstream, CancellationToken::new(), watch));
            client.write_all(b"later").await.unwrap();
            let mut received = [0; 13];
            backend.read_exact(&mut received).await.unwrap();
            assert_eq!(&received, b"bufferedlater");

            // Authentication must reach the client before ReadyForQuery exists.
            let challenge = b"R\0\0\0\x08\0\0\0\x03";
            for chunk in challenge.chunks(2) {
                backend.write_all(chunk).await.unwrap();
                let mut received = vec![0; chunk.len()];
                client.read_exact(&mut received).await.unwrap();
                assert_eq!(received, chunk);
            }
            let password = b"p\0\0\0\x0bsecret\0";
            client.write_all(password).await.unwrap();
            let mut received = vec![0; password.len()];
            backend.read_exact(&mut received).await.unwrap();
            assert_eq!(received, password);

            let error = b"E\0\0\0\x21SFATAL\0C28P01\0Mbad password\0\0";
            backend.write_all(error).await.unwrap();
            backend.shutdown().await.unwrap();
            let mut received = Vec::new();
            client.read_to_end(&mut received).await.unwrap();
            assert_eq!(received, error);
            client.shutdown().await.unwrap();
            relay.await.unwrap().unwrap();
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn buffered_backpressure_does_not_block_reverse_traffic_or_half_close() {
        tokio::time::timeout(Duration::from_secs(3), async {
            let (mut client, proxy_client) = tokio::io::duplex(1);
            let (proxy_upstream, mut backend) = tokio::io::duplex(1);
            let mut connection = framed(proxy_client);
            // Exceed the copy buffer so the prefix itself needs multiple
            // reads while the reverse direction is making progress.
            let prefix = b"buffered".repeat(RELAY_BUFFER_SIZE / 8 + 1);
            connection.read_buffer_mut().extend_from_slice(&prefix);
            let (_shutdown, watch) = watch::channel(false);
            let relay =
                tokio::spawn(run(connection, proxy_upstream, CancellationToken::new(), watch));
            let backend = async {
                // Finish this write before reading the prefix. A sequential
                // prefix write in the relay would deadlock
                // because both buffers are full.
                backend.write_all(b"challenge").await.unwrap();
                let mut received = Vec::new();
                backend.read_to_end(&mut received).await.unwrap();
                let mut expected = prefix;
                expected.extend_from_slice(b"request");
                assert_eq!(received, expected);
                // The client's write EOF must still allow the backend to
                // respond.
                backend.write_all(b"response").await.unwrap();
                backend.shutdown().await.unwrap();
            };
            let client = async {
                let mut challenge = [0; 9];
                client.read_exact(&mut challenge).await.unwrap();
                assert_eq!(&challenge, b"challenge");
                client.write_all(b"request").await.unwrap();
                client.shutdown().await.unwrap();
                let mut response = Vec::new();
                client.read_to_end(&mut response).await.unwrap();
                assert_eq!(response, b"response");
            };
            tokio::join!(backend, client);
            relay.await.unwrap().unwrap();
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn lease_cancellation_and_shutdown_close_both_streams_during_authentication() {
        for cancel_lease in [true, false] {
            tokio::time::timeout(Duration::from_secs(3), async {
                let (mut client, proxy_client) = tokio::io::duplex(64);
                let (proxy_upstream, mut backend) = tokio::io::duplex(64);
                let cancellation = CancellationToken::new();
                let (shutdown, watch) = watch::channel(false);
                let relay = tokio::spawn(run(
                    framed(proxy_client),
                    proxy_upstream,
                    cancellation.clone(),
                    watch,
                ));
                backend.write_all(b"challenge").await.unwrap();
                let mut challenge = [0; 9];
                client.read_exact(&mut challenge).await.unwrap();
                if cancel_lease {
                    cancellation.cancel();
                } else {
                    shutdown.send(true).unwrap();
                }
                relay.await.unwrap().unwrap();
                assert_eq!(client.read(&mut [0]).await.unwrap(), 0);
                assert_eq!(backend.read(&mut [0]).await.unwrap(), 0);
            })
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn backend_write_error_closes_client() {
        tokio::time::timeout(Duration::from_secs(3), async {
            let (mut client, proxy_client) = tokio::io::duplex(64);
            let (proxy_upstream, backend) = tokio::io::duplex(1);
            drop(backend);
            let mut connection = framed(proxy_client);
            connection.read_buffer_mut().extend_from_slice(b"pending");
            let (_shutdown, watch) = watch::channel(false);
            assert!(
                run(connection, proxy_upstream, CancellationToken::new(), watch).await.is_err()
            );
            assert_eq!(client.read(&mut [0]).await.unwrap(), 0);
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn postgres_client_authenticates_and_queries_through_pingora_streams() {
        use futures::StreamExt;
        use pgtest_engine_backend::{PgEndpoint, PgTarget};
        use pgwire::{
            api::{ClientInfo, PgWireConnectionState},
            messages::{
                PgWireBackendMessage as B, PgWireFrontendMessage as F,
                data::{DataRow, FieldDescription, RowDescription},
                response::{CommandComplete, ReadyForQuery, TransactionStatus},
                startup::Authentication,
            },
        };
        use pingora_core::connectors::TransportConnector;
        use tokio::net::TcpListener;
        use tokio_postgres::{Config, NoTls, SimpleQueryMessage, config::SslMode};

        tokio::time::timeout(Duration::from_secs(5), async {
            let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
            let backend_listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
            let target = PgTarget {
                database: "allocated_database".into(),
                endpoint: PgEndpoint::Tcp {
                    host: "127.0.0.1".into(),
                    port: backend_listener.local_addr().unwrap().port(),
                },
            };
            let mut config = Config::new();
            config
                .host("127.0.0.1")
                .port(listener.local_addr().unwrap().port())
                .user("alice")
                .password("secret")
                .dbname("template/demo")
                .ssl_mode(SslMode::Disable);
            let proxy = async {
                let (socket, peer) = listener.accept().await.unwrap();
                let stream: pingora_core::protocols::Stream =
                    Box::new(pingora_core::protocols::l4::stream::Stream::from(socket));
                let mut connection = startup::connection(stream, peer);
                let initial = startup::read(&mut connection).await.unwrap();
                let upstream =
                    crate::upstream::connect(&TransportConnector::new(None), &target, initial)
                        .await
                        .unwrap();
                let (_shutdown, watch) = watch::channel(false);
                run(connection, upstream, CancellationToken::new(), watch).await.unwrap();
            };
            let backend = async {
                let (socket, peer) = backend_listener.accept().await.unwrap();
                let mut backend = startup::connection(socket, peer);
                let initial = startup::read(&mut backend).await.unwrap();
                assert_eq!(initial.parameters["database"], "allocated_database");
                assert_eq!(initial.parameters["user"], "alice");
                backend.set_state(PgWireConnectionState::AuthenticationInProgress);
                backend.send(B::Authentication(Authentication::CleartextPassword)).await.unwrap();
                let F::PasswordMessageFamily(password) = backend.next().await.unwrap().unwrap()
                else {
                    panic!("expected password response through the relay");
                };
                assert_eq!(password.into_password().unwrap().password, "secret");
                backend.send(B::Authentication(Authentication::Ok)).await.unwrap();
                backend
                    .send(B::ReadyForQuery(ReadyForQuery::new(TransactionStatus::Idle)))
                    .await
                    .unwrap();
                backend.set_state(PgWireConnectionState::ReadyForQuery);
                let F::Query(query) = backend.next().await.unwrap().unwrap() else {
                    panic!("expected query after authentication");
                };
                assert_eq!(query.query, "SELECT 42");
                backend
                    .send(B::RowDescription(RowDescription::new(vec![FieldDescription::new(
                        "answer".into(),
                        0,
                        0,
                        23,
                        4,
                        -1,
                        0,
                    )])))
                    .await
                    .unwrap();
                backend
                    .send(B::DataRow(DataRow::new(BytesMut::from(&b"\0\0\0\x0242"[..]), 1)))
                    .await
                    .unwrap();
                backend
                    .send(B::CommandComplete(CommandComplete::new("SELECT 1".into())))
                    .await
                    .unwrap();
                backend
                    .send(B::ReadyForQuery(ReadyForQuery::new(TransactionStatus::Idle)))
                    .await
                    .unwrap();
                assert!(matches!(backend.next().await.unwrap().unwrap(), F::Terminate(_)));
            };
            let client = async {
                let (client, driver) = config.connect(NoTls).await.unwrap();
                let driver = tokio::spawn(driver);
                let rows = client.simple_query("SELECT 42").await.unwrap();
                let row = rows
                    .iter()
                    .find_map(|message| match message {
                        SimpleQueryMessage::Row(row) => Some(row),
                        _ => None,
                    })
                    .unwrap();
                assert_eq!(row.get(0), Some("42"));
                drop(client);
                driver.await.unwrap().unwrap();
            };
            tokio::join!(proxy, backend, client);
        })
        .await
        .expect("real TCP authentication and query traffic must finish through Pingora");
    }
}
