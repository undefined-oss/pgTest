use bytes::BytesMut;
use pgwire::tokio::server::MaybeTls;
use tokio::io::{AsyncWriteExt, copy_bidirectional_with_sizes};
use tokio_util::sync::CancellationToken;

use crate::postgres_upstream::upstream_stream::UpstreamStream;

const RELAY_BUF: usize = 64 * 1024;

#[hotpath::measure]
pub(crate) async fn run(
    upstream_client: MaybeTls,
    upstream_stream: UpstreamStream,
    remaining_stream: BytesMut,
    cancellation: CancellationToken,
) -> std::io::Result<()> {
    let mut upstream_client = hotpath::io!(upstream_client, label = "client-relay");
    let mut upstream_stream = hotpath::io!(upstream_stream, label = "postgres-relay");
    let relay = async {
        if !remaining_stream.is_empty() {
            upstream_stream.write_all(&remaining_stream).await?;
        }
        copy_bidirectional_with_sizes(
            &mut upstream_client,
            &mut upstream_stream,
            RELAY_BUF,
            RELAY_BUF,
        )
        .await?;
        Ok::<(), std::io::Error>(())
    };
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => Ok(()),
        result = relay => result,
    }
}

#[cfg(test)]
mod tests {
    use tokio::{io::AsyncReadExt, net::TcpStream};

    use super::*;

    async fn socket_pair() -> (TcpStream, TcpStream) {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap()).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        (client, server)
    }

    #[tokio::test]
    async fn relays_authentication_buffered_bytes_and_startup_errors() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let (mut client, proxy_client) = socket_pair().await;
            let (proxy_upstream, mut upstream) = socket_pair().await;
            let cancellation = CancellationToken::new();
            let relay = tokio::spawn(run(
                MaybeTls::Plain(proxy_client),
                UpstreamStream::Tcp(proxy_upstream),
                BytesMut::from(&b"already-buffered"[..]),
                cancellation.clone(),
            ));
            let mut buffered = [0; 16];
            upstream.read_exact(&mut buffered).await.unwrap();
            assert_eq!(&buffered, b"already-buffered");

            // A challenge must arrive before AuthenticationOk or ReadyForQuery.
            let challenge = b"R\0\0\0\x08\0\0\0\x03";
            for chunk in challenge.chunks(2) {
                upstream.write_all(chunk).await.unwrap();
                let mut received = vec![0; chunk.len()];
                client.read_exact(&mut received).await.unwrap();
                assert_eq!(received, chunk);
            }
            let password = b"p\0\0\0\x0bsecret\0";
            client.write_all(password).await.unwrap();
            let mut received = vec![0; password.len()];
            upstream.read_exact(&mut received).await.unwrap();
            assert_eq!(received, password);

            // Upstream startup errors are forwarded verbatim, without wrapping.
            let error = b"E\0\0\0\x21SFATAL\0C28P01\0Mbad password\0\0";
            upstream.write_all(error).await.unwrap();
            let mut received = vec![0; error.len()];
            client.read_exact(&mut received).await.unwrap();
            assert_eq!(received, error);
            upstream.shutdown().await.unwrap();
            assert_eq!(client.read(&mut [0]).await.unwrap(), 0);
            client.shutdown().await.unwrap();
            relay.await.unwrap().unwrap();
        })
        .await
        .expect("authentication must flow before startup completes");
    }

    #[tokio::test]
    async fn cancellation_closes_both_sockets_during_authentication() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let (mut client, proxy_client) = socket_pair().await;
            let (proxy_upstream, mut upstream) = socket_pair().await;
            let cancellation = CancellationToken::new();
            let relay = tokio::spawn(run(
                MaybeTls::Plain(proxy_client),
                UpstreamStream::Tcp(proxy_upstream),
                BytesMut::new(),
                cancellation.clone(),
            ));
            let challenge = b"R\0\0\0\x08\0\0\0\x03";
            upstream.write_all(challenge).await.unwrap();
            let mut received = [0; 9];
            client.read_exact(&mut received).await.unwrap();
            assert_eq!(&received, challenge);
            cancellation.cancel();
            relay.await.unwrap().unwrap();
            assert_eq!(client.read(&mut [0]).await.unwrap(), 0);
            assert_eq!(upstream.read(&mut [0]).await.unwrap(), 0);
        })
        .await
        .unwrap();
    }
}
