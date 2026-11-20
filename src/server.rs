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
