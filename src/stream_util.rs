use std::time::Duration;

use crate::{
    config::WRITE_TIMEOUT_SECS,
    consumable_buffer::{BufferRead, ConsumableBuffer},
    error::ProxyError,
};

pub async fn forward<const N: usize, F, T>(
    reader: &mut F,
    read_buf: &mut ConsumableBuffer<N>,
    writer: &mut T,
    read_timeout: Duration,
) -> Result<(), ProxyError>
where
    F: tokio::io::AsyncRead + Unpin,
    T: tokio::io::AsyncWrite + Unpin,
{
    let mut reader_is_eof = false;
    loop {
        match tokio::time::timeout(read_timeout, read_buf.fill(reader)).await {
            Ok(Ok(BufferRead::Eof)) => reader_is_eof = true,
            Ok(Ok(BufferRead::Buffered) | Ok(BufferRead::BufferFull)) => {}
            Ok(Err(e)) => {
                break Err(ProxyError::UnexpectedError(
                    "Error during read from client.",
                    Box::new(e),
                ));
            }
            Err(_) => break Err(ProxyError::ReadTimeout("Timeout during read from client.")),
        };
        match tokio::time::timeout(
            Duration::from_secs(WRITE_TIMEOUT_SECS),
            read_buf.write_to(writer),
        )
        .await
        {
            Ok(Err(e)) => {
                break Err(ProxyError::UnexpectedError(
                    "Error during write to target.",
                    Box::new(e),
                ));
            }
            Err(_) => break Err(ProxyError::WriteTimeout("Timeout during write to target.")),
            _ => {}
        }
        if reader_is_eof {
            break Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, duplex};

    use super::*;

    /// An AsyncRead that never resolves (useful for triggering read_timeout cleanly).
    struct PendingReader;

    impl AsyncRead for PendingReader {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }

    /// An AsyncWrite that accepts writes once (to fill the buffer) and then stays Pending
    /// so `write_to` hits `WRITE_TIMEOUT_SECS`.
    struct BlockAfterFirstWrite {
        accepted_once: bool,
    }

    impl AsyncWrite for BlockAfterFirstWrite {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            if !self.accepted_once {
                self.accepted_once = true;
                Poll::Ready(Ok(buf.len()))
            } else {
                Poll::Pending
            }
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn forwards_data_until_eof() {
        let (mut client_read, mut reader_writer) = duplex(1024);
        let (mut writer_reader, mut target_write) = duplex(1024);
        let mut read_buf = ConsumableBuffer::<128>::new();

        tokio::spawn(async move {
            reader_writer.write_all(b"hello proxy").await.unwrap();
        });

        let res = forward(
            &mut client_read,
            &mut read_buf,
            &mut target_write,
            Duration::from_secs(5),
        )
        .await;

        assert!(res.is_ok(), "expected clean EOF exit, got: {res:?}");
        drop(target_write);

        let mut received = Vec::new();
        writer_reader.read_to_end(&mut received).await.unwrap();
        assert_eq!(received, b"hello proxy");
    }

    #[tokio::test]
    async fn returns_read_timeout_when_reader_stalls() {
        let mut reader = PendingReader;
        let (mut _sink_read, mut writer) = duplex(64);
        let mut read_buf = ConsumableBuffer::<64>::new();

        let timeout_dur = Duration::from_secs(2);
        let err = forward(&mut reader, &mut read_buf, &mut writer, timeout_dur)
            .await
            .expect_err("expected ReadTimeout");
        assert!(matches!(err, ProxyError::ReadTimeout(_)));
    }

    #[tokio::test]
    async fn returns_write_timeout_when_writer_stalls() {
        // Feed two chunks so the second iteration hits `write_to` while `BlockAfterFirstWrite` is Pending.
        let data = b"first-chunk-second-chunk";
        let mut reader = io::Cursor::new(data.as_slice());
        let mut writer = BlockAfterFirstWrite {
            accepted_once: false,
        };
        let mut read_buf = ConsumableBuffer::<4>::new();

        let err = forward(
            &mut reader,
            &mut read_buf,
            &mut writer,
            Duration::from_secs(5),
        )
        .await
        .expect_err("expected WriteTimeout");
        assert!(matches!(err, ProxyError::WriteTimeout(_)));
    }
}
