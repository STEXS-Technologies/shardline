//! Cancellation between source reads without retryable Interrupted errors.

use std::io::{Read, Result, Seek, SeekFrom};

pub(super) struct CheckpointReader<'request, R> {
    pub(super) reader: &'request mut R,
    pub(super) checkpoint: &'request dyn Fn() -> Result<()>,
}

impl<R> CheckpointReader<'_, R> {
    fn check(&self) -> Result<()> {
        // read_exact retries Interrupted. Cancellation must instead terminate
        // parsing, rather than spinning forever after the request disappears.
        (self.checkpoint)()
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::ConnectionAborted, error))
    }
}

impl<R: Read> Read for CheckpointReader<'_, R> {
    fn read(&mut self, bytes: &mut [u8]) -> Result<usize> {
        self.check()?;
        self.reader.read(bytes)
    }
}

impl<R: Seek> Seek for CheckpointReader<'_, R> {
    fn seek(&mut self, position: SeekFrom) -> Result<u64> {
        self.check()?;
        self.reader.seek(position)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancelled_exact_read_and_seek_fail_without_retrying_or_consuming_source() {
        let mut source = std::io::Cursor::new(b"verified source");
        let cancelled = || Err(std::io::Error::from(std::io::ErrorKind::Interrupted));
        let mut reader = CheckpointReader {
            reader: &mut source,
            checkpoint: &cancelled,
        };
        let error = reader.read_exact(&mut [0_u8; 4]).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::ConnectionAborted);
        assert_eq!(
            reader.seek(SeekFrom::Start(4)).unwrap_err().kind(),
            std::io::ErrorKind::ConnectionAborted
        );
        assert_eq!(source.position(), 0);
    }
}
