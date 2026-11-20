use std::{net::SocketAddr, time::Duration};

use tokio::{io::AsyncWriteExt, net::TcpStream};

use crate::{
    config::{BUFFER_SIZE, CONNECTION_TIMEOUT_SECS, READ_TIMEOUT_SECS},
    consumable_buffer::ConsumableBuffer,
    error::{ProxyError, handle_err},
    header_util, stream_util,
};

pub async fn handle_stream(mut stream: TcpStream, addr: SocketAddr) {
    println!("Receiving request from {}", addr);
    let (mut client_reader, mut client_writer) = stream.split();
    let mut client_read_buf = ConsumableBuffer::<BUFFER_SIZE>::new();
    let header_res = tokio::time::timeout(
        Duration::from_secs(READ_TIMEOUT_SECS),
        header_util::extract_header(&mut client_reader, &mut client_read_buf),
    )
    .await;
    let header = match header_res {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            handle_err(e, &mut client_writer).await;
            return;
        }
        Err(_) => {
            handle_err(
                ProxyError::ReadTimeoutHeader("Timeout during reading header."),
                &mut client_writer,
            )
            .await;
            return;
        }
    };
    let addr = match header_util::parse_header(&header) {
        Ok(s) => s,
        Err(e) => {
            handle_err(e, &mut client_writer).await;
            return;
        }
    };
    let mut target_stream = match connect_to_target(addr).await {
        Ok(s) => s,
        Err(e) => {
            handle_err(e, &mut client_writer).await;
            return;
        }
    };
    let (mut target_reader, mut target_writer) = target_stream.split();
    if let Err(e) = client_writer
        .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        .await
    {
        handle_err(
            ProxyError::SendingToClientFailed(
                "Error when sending proxy success message to client",
                e,
            ),
            &mut client_writer,
        )
        .await;
        return;
    }
    let mut target_read_buf = ConsumableBuffer::<BUFFER_SIZE>::new();
    if let Err(e) = stream_util::redirect_streams(
        &mut client_reader,
        &mut client_read_buf,
        &mut client_writer,
        &mut target_reader,
        &mut target_read_buf,
        &mut target_writer,
    )
    .await
    {
        handle_err(e, &mut client_writer).await;
        return;
    }
}

async fn connect_to_target(addr: &str) -> Result<TcpStream, ProxyError> {
    match tokio::time::timeout(
        Duration::from_secs(CONNECTION_TIMEOUT_SECS),
        TcpStream::connect(addr),
    )
    .await
    {
        Ok(Ok(s)) => Ok(s),
        Ok(Err(e)) => Err(ProxyError::TargetConnectionFailed(
            "Cannot connect to target address.",
            e,
        )),
        Err(e) => Err(ProxyError::TargetConnectionFailed(
            "Cannot connect to target address.",
            std::io::Error::new(std::io::ErrorKind::TimedOut, e),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{io::AsyncReadExt, net::TcpListener};

    #[tokio::test]
    async fn test_connect_to_target_success() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();

        let accept_task = tokio::spawn(async move {
            let (_sock, _) = listener.accept().await.unwrap();
        });

        let connected = connect_to_target(&addr).await;
        assert!(connected.is_ok());

        accept_task.await.unwrap();
    }

    #[tokio::test]
    async fn test_connect_to_target_failure() {
        let err = connect_to_target("127.0.0.1:0").await.unwrap_err();
        assert!(matches!(err, ProxyError::TargetConnectionFailed(_, _)));
    }

    #[tokio::test]
    async fn test_handle_stream_connect_and_relay() {
        // 1. Start a mock target server that echoes back whatever it receives.
        let target_listener = TcpListener::bind("127.0.0.1:9000").await.unwrap();
        let target_addr = target_listener.local_addr().unwrap();

        let target_task = tokio::spawn(async move {
            let (mut target_sock, _) = target_listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let n = target_sock.read(&mut buf).await.unwrap();
            if n > 0 {
                target_sock.write_all(&buf[..n]).await.unwrap();
            }
        });

        // 2. Start a listener to get a client stream connected to handle_stream.
        let proxy_listener = TcpListener::bind("127.0.0.1:9001").await.unwrap();
        let proxy_addr = proxy_listener.local_addr().unwrap();

        let proxy_task = tokio::spawn(async move {
            let (client_stream, peer_addr) = proxy_listener.accept().await.unwrap();
            handle_stream(client_stream, peer_addr).await;
        });

        // 3. Act as the client connecting to the proxy.
        let mut client = TcpStream::connect(proxy_addr).await.unwrap();
        let connect_req = format!("CONNECT {target_addr} HTTP/1.1\r\nHost: {target_addr}\r\n\r\n");
        client.write_all(connect_req.as_bytes()).await.unwrap();

        // Read the 200 response from the proxy.
        let mut resp_buf = [0u8; 128];
        let n = client.read(&mut resp_buf).await.unwrap();
        let resp = std::str::from_utf8(&resp_buf[..n]).unwrap();
        assert!(resp.starts_with("HTTP/1.1 200 Connection Established"));

        // Send payload through the tunnel and verify echo.
        client.write_all(b"ping-through-tunnel").await.unwrap();
        let mut echo_buf = [0u8; 64];
        let m = client.read(&mut echo_buf).await.unwrap();
        assert_eq!(&echo_buf[..m], b"ping-through-tunnel");

        drop(client);
        proxy_task.await.unwrap();
        target_task.await.unwrap();
    }

    #[tokio::test]
    async fn test_handle_stream_rejects_non_connect_method() {
        let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = proxy_listener.local_addr().unwrap();

        let proxy_task = tokio::spawn(async move {
            let (client_stream, peer_addr) = proxy_listener.accept().await.unwrap();
            handle_stream(client_stream, peer_addr).await;
        });

        let mut client = TcpStream::connect(proxy_addr).await.unwrap();
        client
            .write_all(b"GET example.com:80 HTTP/1.1\r\nHost: example.com:80\r\n\r\n")
            .await
            .unwrap();

        let mut buf = [0u8; 512];
        let n = client.read(&mut buf).await.unwrap();
        let out = String::from_utf8_lossy(&buf[..n]);
        assert!(!out.contains("200 Connection Established"));
        proxy_task.await.unwrap();
    }

    #[tokio::test]
    async fn test_handle_stream_malformed_header() {
        let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = proxy_listener.local_addr().unwrap();

        let proxy_task = tokio::spawn(async move {
            let (client_stream, peer_addr) = proxy_listener.accept().await.unwrap();
            handle_stream(client_stream, peer_addr).await;
        });

        let mut client = TcpStream::connect(proxy_addr).await.unwrap();
        client.write_all(b"INVALID_REQUEST\r\n\r\n").await.unwrap();

        let mut buf = [0u8; 512];
        let n = client.read(&mut buf).await.unwrap();
        let out = String::from_utf8_lossy(&buf[..n]);
        assert!(!out.contains("200 Connection Established"));

        proxy_task.await.unwrap();
    }
}
