use tokio::net::{TcpListener, ToSocketAddrs};

use crate::proxy;

/// This is a basic http-proxy implementation. It does not intend to be production ready nor does it
/// implement the full protocol specified in the related RFC.
///
/// It only offers support for proxy requests initiated by the CONNECT method.
// The implementation avoids using BytesMut because it has no baken in size restriction.
pub async fn http_proxy<A>(addr: A) -> Result<(), std::io::Error>
where
    A: ToSocketAddrs,
{
    let listener = TcpListener::bind(addr).await?;
    loop {
        let (stream, addr) = listener.accept().await?;
        tokio::spawn(proxy::handle_stream(stream, addr));
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };

    #[tokio::test]
    async fn test_proxy_connect_success() {
        let proxy_port = 18083;
        let target_port = 18084;

        // 1. Spawn a mock target server that echoes back any data it receives
        let target_listener = TcpListener::bind(("127.0.0.1", target_port)).await.unwrap();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = target_listener.accept().await {
                let mut buf = [0; 1024];
                if let Ok(n) = stream.read(&mut buf).await
                    && n > 0
                {
                    // Echo the data back to prove the tunnel works end-to-end
                    stream.write_all(&buf[..n]).await.unwrap();
                }
            }
        });

        // 2. Spawn your proxy server in the background
        tokio::spawn(http_proxy(format!("0.0.0.0:{proxy_port}")));

        // Give listeners a moment to bind
        tokio::time::sleep(Duration::from_millis(50)).await;

        // 3. Connect as a client to the proxy
        let mut client = TcpStream::connect(("127.0.0.1", proxy_port)).await.unwrap();

        // 4. Send the HTTP CONNECT request targeting our mock server
        let connect_msg = format!(
            "CONNECT 127.0.0.1:{} HTTP/1.1\r\nHost: 127.0.0.1:{}\r\n\r\n",
            target_port, target_port
        );
        client.write_all(connect_msg.as_bytes()).await.unwrap();

        // 5. Read the proxy's response and verify connection establishment
        let mut buf = [0; 1024];
        let n = client.read(&mut buf).await.unwrap();
        let response = String::from_utf8_lossy(&buf[..n]);
        assert!(
            response.contains("200 Connection Established"),
            "Expected 200 Connection Established, got: {}",
            response
        );

        //  payload through the established tunnel
        client.write_all(b"Hello through tunnel!").await.unwrap();

        // 7. Read the response echoed back from the target server through the proxy
        let n = client.read(&mut buf).await.unwrap();
        let echoed = String::from_utf8_lossy(&buf[..n]);
        assert_eq!(echoed, "Hello through tunnel!");
    }
}
