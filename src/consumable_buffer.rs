use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(PartialEq, Debug)]
pub enum BufferRead {
    Buffered,
    BufferFull,
    Eof,
}

pub struct ConsumableBuffer<const N: usize> {
    buf: [u8; N],
    write_pos: usize,
    consume_pos: usize,
}

impl<const N: usize> ConsumableBuffer<N> {
    pub fn new() -> Self {
        ConsumableBuffer {
            buf: [0; N],
            write_pos: 0,
            consume_pos: 0,
        }
    }

    fn is_full(&self) -> bool {
        self.write_pos == N && self.consume_pos == 0
    }

    /// Consumes a byte, if available, from the read data.
    pub fn consume(&mut self) -> Option<u8> {
        if self.consume_pos < self.write_pos {
            let byte = self.buf[self.consume_pos];
            self.consume_pos += 1;
            Some(byte)
        } else {
            None
        }
    }

    /// Writes all not yet consumed data into the writer and consumes these data.
    pub async fn write_to<W>(&mut self, writer: &mut W) -> Result<(), std::io::Error>
    where
        W: AsyncWriteExt + Unpin,
    {
        if self.consume_pos < self.write_pos {
            match writer
                .write_all(&self.buf[self.consume_pos..self.write_pos])
                .await
            {
                Ok(_) => {
                    self.consume_pos = self.write_pos;
                    Ok(())
                }
                Err(e) => Err(e),
            }
        } else {
            Ok(())
        }
    }

    /// Reads bytes from the given stream into the internal buffer from later consumption.
    pub async fn fill<R>(&mut self, reader: &mut R) -> Result<BufferRead, std::io::Error>
    where
        R: AsyncReadExt + Unpin,
    {
        if self.is_full() {
            return Ok(BufferRead::BufferFull);
        }
        if self.consume_pos == self.write_pos {
            self.write_pos = 0;
        } else if self.consume_pos > 0 {
            self.buf.copy_within(self.consume_pos..self.write_pos, 0);
            self.write_pos -= self.consume_pos;
        }
        self.consume_pos = 0;
        match reader.read(&mut self.buf[self.write_pos..]).await {
            Ok(0) => Ok(BufferRead::Eof),
            Ok(n) => {
                self.write_pos += n;
                Ok(BufferRead::Buffered)
            }
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::consumable_buffer::{BufferRead, ConsumableBuffer};

    #[tokio::test]
    async fn test() -> Result<(), std::io::Error> {
        let mut buf = ConsumableBuffer::<3>::new();
        let mut reader = &b"hello world"[..];
        assert_eq!(buf.fill(&mut reader).await?, BufferRead::Buffered);
        assert_eq!(buf.fill(&mut reader).await?, BufferRead::BufferFull);
        assert_eq!(buf.consume(), Some(b'h'));
        assert_eq!(buf.consume(), Some(b'e'));
        assert_eq!(buf.fill(&mut reader).await?, BufferRead::Buffered);
        assert_eq!(buf.consume(), Some(b'l'));
        assert_eq!(buf.consume(), Some(b'l'));
        assert_eq!(buf.consume(), Some(b'o'));
        assert_eq!(buf.consume(), None);
        assert_eq!(buf.fill(&mut reader).await?, BufferRead::Buffered);
        assert_eq!(buf.consume(), Some(b' '));

        let mut writer = Vec::<u8>::new();
        buf.write_to(&mut writer).await?;
        assert_eq!(buf.consume(), None);
        buf.write_to(&mut writer).await?;
        assert_eq!(writer, b"wo".to_vec());
        assert_eq!(buf.fill(&mut reader).await?, BufferRead::Buffered);
        buf.write_to(&mut writer).await?;
        assert_eq!(writer, b"world".to_vec());
        assert_eq!(buf.fill(&mut reader).await?, BufferRead::Eof);
        Ok(())
    }
}
