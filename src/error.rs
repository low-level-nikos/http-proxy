use http::StatusCode;
use thiserror::Error;
use tokio::io::{AsyncWrite, AsyncWriteExt};

#[derive(Error, Debug)]
pub enum ProxyError {
    #[error("{0}: {1}")]
    HandshakeError(&'static str, std::io::Error),
    #[error("{0}")]
    PrematureConClose(&'static str),
    #[error("{0}")]
    TooManyHeaderBytes(&'static str),
    #[error("{0}")]
    HeaderNotValidUtf8(&'static str),
    #[error("{0}")]
    ReadTimeoutHeader(&'static str),
    #[error("{0}")]
    WrongMethod(&'static str),
    #[error("{0}")]
    HeaderFormatError(&'static str),
    #[error("{0}: {1}")]
    TargetConnectionFailed(&'static str, std::io::Error),
    #[error("{0}: {1}")]
    SendingToClientFailed(&'static str, std::io::Error),
    #[error("{0}: {1}")]
    SendingToTargetFailed(&'static str, std::io::Error),
    #[error("{0}: {1}")]
    UnexpectedError(&'static str, Box<dyn std::error::Error + Send>),
    #[error("{0}")]
    UnexpectedInternalError(&'static str),
    #[error("{0}")]
    ReadTimeout(&'static str),
    #[error("{0}")]
    WriteTimeout(&'static str),
}

pub(crate) async fn handle_err<W>(pe: ProxyError, writer: &mut W)
where
    W: AsyncWrite + Unpin,
{
    match pe {
        ProxyError::HandshakeError(msg, e)
        | ProxyError::TargetConnectionFailed(msg, e)
        | ProxyError::SendingToClientFailed(msg, e)
        | ProxyError::SendingToTargetFailed(msg, e) => {
            eprintln!("{e}");
            send_error(StatusCode::INTERNAL_SERVER_ERROR, msg, writer).await;
        }
        ProxyError::TooManyHeaderBytes(msg)
        | ProxyError::HeaderNotValidUtf8(msg)
        | ProxyError::WrongMethod(msg)
        | ProxyError::HeaderFormatError(msg) => {
            send_error(StatusCode::BAD_REQUEST, msg, writer).await
        }
        ProxyError::PrematureConClose(msg)
        | ProxyError::ReadTimeoutHeader(msg)
        | ProxyError::ReadTimeout(msg)
        | ProxyError::WriteTimeout(msg) => {
            send_error(StatusCode::INTERNAL_SERVER_ERROR, msg, writer).await
        }
        ProxyError::UnexpectedInternalError(msg) => {
            eprintln!("{msg}");
            send_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Some internal error happened.",
                writer,
            )
            .await;
        }
        ProxyError::UnexpectedError(msg, e) => {
            eprintln!("{e}");
            send_error(StatusCode::INTERNAL_SERVER_ERROR, msg, writer).await;
        }
    }
}

async fn send_error<W>(status: StatusCode, msg: &str, stream: &mut W)
where
    W: AsyncWriteExt + Unpin,
{
    let len = msg.len();
    if let Err(e) = stream
        .write_all(
            format!(
                "\
                    HTTP/1.1 {status}\r\n\
                     Content-Type: text/plain\r\n\
                     Content-Length: {len}\r\n\
                     Connection: close\r\n\
                     \r\n\
                     {msg}",
            )
            .as_bytes(),
        )
        .await
    {
        eprintln!("Error when sending error to client {e}");
    }
    if let Err(e) = stream.flush().await {
        eprintln!("Error flushing error response to client {e}");
    }
}
