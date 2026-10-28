//! Optional SHA worker charged to the caller's existing blocking admission.
use sha2::{Digest, Sha256};
use std::{sync::mpsc, thread};

enum Message {
    Bytes(Box<[u8]>),
    Reset(Sha256),
    Snapshot(mpsc::SyncSender<Sha256>),
}

pub(super) struct DigestWorker {
    send: Option<mpsc::SyncSender<Message>>,
    job: Option<thread::JoinHandle<Sha256>>,
    failed: bool,
}

impl DigestWorker {
    pub(super) fn start() -> Option<Self> {
        let (send, receive) = mpsc::sync_channel(2);
        let job = thread::Builder::new()
            .name("shardline-file-sha256".into())
            .stack_size(256 * 1024)
            .spawn(move || {
                let mut state = Sha256::new();
                while let Ok(message) = receive.recv() {
                    match message {
                        Message::Bytes(bytes) => state.update(bytes),
                        Message::Reset(replacement) => state = replacement,
                        Message::Snapshot(reply) => {
                            let _ignored = reply.send(state.clone());
                        }
                    }
                }
                state
            })
            .ok()?;
        Some(Self {
            send: Some(send),
            job: Some(job),
            failed: false,
        })
    }

    fn submit(&mut self, message: Message) {
        if self
            .send
            .as_ref()
            .is_none_or(|send| send.send(message).is_err())
        {
            self.failed = true;
        }
    }

    pub(super) fn update(&mut self, bytes: &[u8]) {
        // At most two queued blocks, one worker block and one producer block.
        for block in bytes.chunks(64 * 1024) {
            self.submit(Message::Bytes(block.into()));
            if self.failed {
                break;
            }
        }
    }

    pub(super) fn reset(&mut self, state: Sha256) {
        self.submit(Message::Reset(state));
    }

    pub(super) fn snapshot(&mut self) -> Option<Sha256> {
        let (send, receive) = mpsc::sync_channel(0);
        self.submit(Message::Snapshot(send));
        if self.failed {
            return None;
        }
        let result = receive.recv().ok();
        self.failed |= result.is_none();
        result
    }

    pub(super) fn finish(mut self) -> Option<Sha256> {
        self.send.take();
        let result = self.job.take()?.join().ok();
        if self.failed { None } else { result }
    }
}

impl Drop for DigestWorker {
    fn drop(&mut self) {
        self.send.take();
        if let Some(job) = self.job.take() {
            let _ignored = job.join();
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn closed_worker_cannot_return_a_successful_identity() {
        let mut worker = DigestWorker::start().unwrap();
        worker.send.take();
        worker.update(b"must not disappear");
        assert!(worker.finish().is_none());
    }

    #[test]
    fn dropping_a_backlogged_worker_drains_and_joins() {
        let mut worker = DigestWorker::start().unwrap();
        worker.update(&vec![7; 1024 * 1024]);
        drop(worker);
        // A second independently owned worker remains usable after cancellation.
        let mut retry = DigestWorker::start().unwrap();
        retry.update(b"retry");
        assert_eq!(retry.finish().unwrap().finalize(), Sha256::digest(b"retry"));
    }
}
