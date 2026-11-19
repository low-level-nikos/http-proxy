use tokio::io::AsyncReadExt;

use crate::{
    config::MAX_HEADER_BYTE_COUNT,
    consumable_buffer::{BufferRead, ConsumableBuffer},
    error::ProxyError,
};

// Reads the http header from the given reader into the given buffer.
// The header part will be consumed from the buffer and returns as result.
// The implementation protects against Header Flooding by responding with error if header size
// surpasses a given threshold.
pub async fn extract_header<const N: usize, R>(
    reader: &mut R,
    read_buf: &mut ConsumableBuffer<N>,
) -> Result<String, ProxyError>
where
    R: AsyncReadExt + Unpin,
{
    let mut header_buf: Vec<u8> = vec![];
    let mut received_bytes_count = 0;
    let mut expected_header_end_char = b"\r"[0];
    let mut header_end_match_count = 0;
    'main: loop {
        // reading into the buffer
        match read_buf.fill(reader).await {
            Err(e) => break Err(ProxyError::HandshakeError("Error during handshake.", e)),
            Ok(BufferRead::Eof) => {
                break Err(ProxyError::PrematureConClose(
                    "Connection prematurely closed by client during handshake.",
                ));
            }
            Ok(BufferRead::BufferFull) => {
                break Err(ProxyError::UnexpectedInternalError(
                    "Buffer full during header read. Consumption is not triggered right?",
                ));
            }
            Ok(BufferRead::Buffered) => {}
        }

        // consuming up to header end token or loop if not yet found
        while let Some(byte) = read_buf.consume() {
            received_bytes_count += 1usize;
            if received_bytes_count > MAX_HEADER_BYTE_COUNT {
                break 'main Err(ProxyError::TooManyHeaderBytes(
                    "Too many bytes received in header.",
                ));
            }
            header_buf.push(byte);
            if byte == expected_header_end_char {
                header_end_match_count += 1;
                expected_header_end_char = if expected_header_end_char == b"\r"[0] {
                    b"\n"[0]
                } else {
                    b"\r"[0]
                };
            } else {
                // reset match
                header_end_match_count = 0;
                expected_header_end_char = b"\r"[0];
            }
            // either this happens or the header size breaks the limit
            if header_end_match_count == 4 {
                match String::from_utf8(header_buf[0..].to_vec()) {
                    Ok(s) => break 'main Ok(s),
                    Err(_) => {
                        break 'main Err(ProxyError::HeaderNotValidUtf8("Header not valid utf8"));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[tokio::test]
    async fn extracts_header_and_leaves_body_in_buffer() {
        let raw = b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n{\"hello\":\"world\"}";
        let mut reader = Cursor::new(raw.as_slice());
        let mut buf = ConsumableBuffer::<64>::new();

        let header = extract_header(&mut reader, &mut buf)
            .await
            .expect("expected valid header");

        assert_eq!(header, "GET / HTTP/1.1\r\nHost: example.com\r\n\r\n");

        // Verify trailing body bytes were not consumed as part of the header.
        let mut remaining = Vec::new();
        while let Some(b) = buf.consume() {
            remaining.push(b);
        }
        assert_eq!(remaining, b"{\"hello\":\"world\"}");
    }

    #[tokio::test]
    async fn returns_premature_close_on_eof_before_header_end() {
        let raw = b"GET / HTTP/1.1\r\nHost: example.com\r\n"; // missing final \r\n
        let mut reader = Cursor::new(raw.as_slice());
        let mut buf = ConsumableBuffer::<64>::new();

        let err = extract_header(&mut reader, &mut buf)
            .await
            .expect_err("expected PrematureConClose");

        assert!(matches!(err, ProxyError::PrematureConClose(_)));
    }

    #[tokio::test]
    async fn returns_too_many_header_bytes_when_exceeding_limit() {
        let mut raw = vec![b'A'; MAX_HEADER_BYTE_COUNT + 1];
        raw.extend_from_slice(b"\r\n\r\n");

        let mut reader = Cursor::new(raw);
        let mut buf = ConsumableBuffer::<128>::new();

        let err = extract_header(&mut reader, &mut buf)
            .await
            .expect_err("expected TooManyHeaderBytes");

        assert!(matches!(err, ProxyError::TooManyHeaderBytes(_)));
    }

    #[tokio::test]
    async fn returns_invalid_utf8_on_non_utf8_header() {
        let raw = b"GET /\xff HTTP/1.1\r\n\r\n";
        let mut reader = Cursor::new(raw.as_slice());
        let mut buf = ConsumableBuffer::<64>::new();

        let err = extract_header(&mut reader, &mut buf)
            .await
            .expect_err("expected HeaderNotValidUtf8");

        assert!(matches!(err, ProxyError::HeaderNotValidUtf8(_)));
    }
}
