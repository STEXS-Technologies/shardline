//! Bound stalled socket writes so transport-owned response buffers are released.

use axum::serve::Listener;
use std::{
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    time::Sleep,
};

pub(super) struct WriteIdleListener<L> {
    inner: L,
    idle: Duration,
}

impl<L> WriteIdleListener<L> {
    pub(super) const fn new(inner: L, idle: Duration) -> Self {
        Self { inner, idle }
    }
}

impl<L: Listener> Listener for WriteIdleListener<L> {
    type Io = IdleWrite<L::Io>;
    type Addr = L::Addr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        // Delegate accept errors/backoff and addresses to the original listener.
        let (inner, address) = self.inner.accept().await;
        (IdleWrite::new(inner, self.idle), address)
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.inner.local_addr()
    }
}

pub(super) struct IdleWrite<T> {
    inner: T,
    idle: Duration,
    stalled: Option<Pin<Box<Sleep>>>,
    pending_write: bool,
    failed: bool,
}

impl<T> IdleWrite<T> {
    const fn new(inner: T, idle: Duration) -> Self {
        Self {
            inner,
            idle,
            stalled: None,
            pending_write: false,
            failed: false,
        }
    }

    fn check_deadline(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        if self.failed
            || self
                .stalled
                .as_mut()
                .is_some_and(|timer| timer.as_mut().poll(cx).is_ready())
        {
            self.failed = true;
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "socket write made no progress before its idle deadline",
            ));
        }
        Ok(())
    }

    fn pending<R>(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<R>> {
        if self.stalled.is_none() {
            self.stalled = Some(Box::pin(tokio::time::sleep(self.idle)));
        }
        self.check_deadline(cx)?;
        Poll::Pending
    }

    fn wrote(&mut self, bytes: usize) {
        if bytes > 0 {
            self.pending_write = false;
            self.stalled = None;
        }
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for IdleWrite<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        // Incoming requests must not keep a blocked response write alive.
        this.check_deadline(cx)?;
        Pin::new(&mut this.inner).poll_read(cx, buf)
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for IdleWrite<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.check_deadline(cx)?;
        match Pin::new(&mut this.inner).poll_write(cx, buf) {
            Poll::Ready(Ok(bytes)) => {
                this.wrote(bytes);
                Poll::Ready(Ok(bytes))
            }
            Poll::Pending if !buf.is_empty() => {
                this.pending_write = true;
                this.pending(cx)
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.check_deadline(cx)?;
        match Pin::new(&mut this.inner).poll_write_vectored(cx, bufs) {
            Poll::Ready(Ok(bytes)) => {
                this.wrote(bytes);
                Poll::Ready(Ok(bytes))
            }
            Poll::Pending if bufs.iter().any(|buf| !buf.is_empty()) => {
                this.pending_write = true;
                this.pending(cx)
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.check_deadline(cx)?;
        match Pin::new(&mut this.inner).poll_flush(cx) {
            Poll::Ready(Ok(())) => {
                // TcpStream::flush is a no-op; it cannot erase a pending write's
                // deadline without any bytes having reached the socket.
                if !this.pending_write {
                    this.stalled = None;
                }
                Poll::Ready(Ok(()))
            }
            Poll::Pending => this.pending(cx),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.check_deadline(cx)?;
        match Pin::new(&mut this.inner).poll_shutdown(cx) {
            Poll::Ready(Ok(())) => {
                this.pending_write = false;
                this.stalled = None;
                Poll::Ready(Ok(()))
            }
            Poll::Pending => this.pending(cx),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
        }
    }
}

#[cfg(test)]
mod tests;
