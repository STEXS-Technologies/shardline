//! Stop consuming a streamed object after its owning request is cancelled.
use std::io::{self, Write};

pub(super) struct CheckpointWriter<'request, W> {
    pub(super) output: &'request mut W,
    pub(super) checkpoint: &'request (dyn Fn() -> io::Result<()> + Sync),
}

impl<W: Write> Write for CheckpointWriter<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        (self.checkpoint)().map_err(non_retryable)?;
        self.output.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        (self.checkpoint)().map_err(non_retryable)?;
        self.output.flush()
    }
}

fn non_retryable(error: io::Error) -> io::Error {
    if error.kind() == io::ErrorKind::Interrupted {
        // write_all retries Interrupted, which would loop on cancellation.
        io::Error::new(
            io::ErrorKind::ConnectionAborted,
            "object materialization cancelled",
        )
    } else {
        error
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn cancelled_writer_keeps_prior_bytes_and_does_not_retry_interrupted() {
        let cancelled = AtomicBool::new(false);
        let checkpoint = || {
            if cancelled.load(Ordering::SeqCst) {
                Err(io::Error::from(io::ErrorKind::Interrupted))
            } else {
                Ok(())
            }
        };
        let mut bytes = Vec::new();
        let mut writer = CheckpointWriter {
            output: &mut bytes,
            checkpoint: &checkpoint,
        };
        writer.write_all(b"first frame").unwrap();
        cancelled.store(true, Ordering::SeqCst);
        assert_eq!(
            writer.write_all(b"discarded frame").unwrap_err().kind(),
            io::ErrorKind::ConnectionAborted
        );
        assert_eq!(
            writer.flush().unwrap_err().kind(),
            io::ErrorKind::ConnectionAborted
        );
        assert_eq!(bytes, b"first frame");
    }
}
