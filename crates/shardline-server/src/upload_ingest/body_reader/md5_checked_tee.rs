//! Validate Content-MD5 and compute an ETag with one streamed checksum.
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use futures_util::Stream;
use md5::{Digest, Md5};

use super::{BodyChunkResult, BoxedBodyStream, Md5Tee};
use crate::ServerError;

pub(super) struct Md5CheckedTee {
    tee: Md5Tee,
    expected: [u8; 16],
    terminal: bool,
}

impl Md5CheckedTee {
    pub(super) fn new(inner: BoxedBodyStream, hasher: Arc<Mutex<Md5>>, expected: [u8; 16]) -> Self {
        Self {
            tee: Md5Tee { inner, hasher },
            expected,
            terminal: false,
        }
    }
}

impl Stream for Md5CheckedTee {
    type Item = BodyChunkResult;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.terminal {
            return Poll::Ready(None);
        }
        match Pin::new(&mut self.tee).poll_next(cx) {
            Poll::Ready(None) => {
                self.terminal = true;
                // Preserve the caller's completed ETag state. Cloning a digest
                // state does not hash the body again.
                let actual: [u8; 16] = self
                    .tee
                    .hasher
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone()
                    .finalize()
                    .into();
                if actual == self.expected {
                    Poll::Ready(None)
                } else {
                    Poll::Ready(Some(Err(ServerError::RequestBodyMd5Mismatch)))
                }
            }
            Poll::Ready(Some(Err(error))) => {
                self.terminal = true;
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(Some(Ok(bytes))) => Poll::Ready(Some(Ok(bytes))),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests;
