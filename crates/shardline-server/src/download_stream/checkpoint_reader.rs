//! Stop container verification between bounded reads when its request disappears.
use std::io::{self, Read, Seek, SeekFrom};

pub(crate) struct CheckpointReader<'request, R> {
    pub(crate) reader: &'request mut R,
    pub(crate) checkpoint: &'request dyn Fn() -> io::Result<()>,
}

impl<R> CheckpointReader<'_, R> {
    fn check(&self) -> io::Result<()> {
        // read_exact retries Interrupted; cancellation must terminate it.
        (self.checkpoint)().map_err(|error| io::Error::new(io::ErrorKind::ConnectionAborted, error))
    }
}

impl<R: Read> Read for CheckpointReader<'_, R> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.check()?;
        self.reader.read(bytes)
    }
}

impl<R: Seek> Seek for CheckpointReader<'_, R> {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.check()?;
        self.reader.seek(position)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        num::NonZeroUsize,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    #[test]
    fn cancellation_does_not_retry_exact_reads_or_seek_the_source() {
        let mut source = io::Cursor::new(b"unchanged");
        let mut reader = CheckpointReader {
            reader: &mut source,
            checkpoint: &|| Err(io::Error::from(io::ErrorKind::Interrupted)),
        };
        assert_eq!(
            reader.read_exact(&mut [0; 4]).unwrap_err().kind(),
            io::ErrorKind::ConnectionAborted
        );
        assert_eq!(
            reader.seek(SeekFrom::Start(4)).unwrap_err().kind(),
            io::ErrorKind::ConnectionAborted
        );
        assert_eq!(source.position(), 0);
    }

    struct PausedSource {
        source: io::Cursor<Vec<u8>>,
        reads: Arc<AtomicUsize>,
        started: Option<tokio::sync::oneshot::Sender<()>>,
        resume: std::sync::mpsc::Receiver<()>,
    }

    impl Read for PausedSource {
        fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            let count = self.source.read(bytes)?;
            if let Some(started) = self.started.take() {
                let _sent = started.send(());
                self.resume
                    .recv_timeout(Duration::from_secs(5))
                    .map_err(io::Error::other)?;
            }
            Ok(count)
        }
    }

    impl Seek for PausedSource {
        fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
            self.source.seek(position)
        }
    }

    #[tokio::test]
    async fn dropped_download_stops_container_validation_before_the_next_source_read() {
        let (bytes, hash) = crate::test_fixtures::single_chunk_xorb(b"verified payload");
        let hash = shardline_index::parse_xet_hash_hex(&hash).unwrap();
        let pool = crate::admission::BoundedPool::new(NonZeroUsize::new(1).unwrap());
        let worker_pool = pool.clone();
        let reads = Arc::new(AtomicUsize::new(0));
        let (started, running) = tokio::sync::oneshot::channel();
        let (resume, wait) = std::sync::mpsc::channel();
        let (finished, observed) = tokio::sync::oneshot::channel();
        let mut source = PausedSource {
            source: io::Cursor::new(bytes.to_vec()),
            reads: reads.clone(),
            started: Some(started),
            resume: wait,
        };
        let request = tokio::spawn(async move {
            crate::admission::cancellable_work_with_setup(
                &worker_pool,
                || (),
                move |(), cancel| {
                    let result = crate::xet_adapter::validate_serialized_xorb(
                        &mut CheckpointReader {
                            reader: &mut source,
                            checkpoint: &|| cancel.check_io(),
                        },
                        hash,
                    );
                    let _sent = finished.send(result.is_err());
                    cancel.check()?;
                    result.map(|_| ()).map_err(Into::into)
                },
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(5), running)
            .await
            .unwrap()
            .unwrap();
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        assert!(
            pool.try_acquire().is_none(),
            "running validation must remain charged"
        );
        resume.send(()).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(5), observed)
                .await
                .unwrap()
                .unwrap()
        );
        let _permit = tokio::time::timeout(Duration::from_secs(5), pool.acquire())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            reads.load(Ordering::SeqCst),
            1,
            "cancelled verification read more source data"
        );
    }
}
