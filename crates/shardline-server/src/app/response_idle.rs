//! Release response producers after an idle interval, including when transport
//! backpressure stops polling the body. Transport-owned bytes retain their guards.

use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use axum::{
    body::{Body, Bytes, HttpBody},
    response::Response,
};
use futures_util::task::AtomicWaker;
use http_body::{Frame, SizeHint};
use tokio::time::Instant;

pub(super) fn wrap(response: Response, idle: Duration) -> Response {
    let (parts, body) = response.into_parts();
    let body = if body.is_end_stream() {
        body
    } else {
        Body::new(IdleBody::new(body, idle))
    };
    Response::from_parts(parts, body)
}

struct State {
    body: Option<Body>,
    deadline: Instant,
    expired: bool,
}

struct Shared {
    state: Mutex<State>,
    waker: AtomicWaker,
}

struct IdleBody {
    shared: Arc<Shared>,
    watchdog: tokio::task::JoinHandle<()>,
    idle: Duration,
    reported: bool,
}

fn deadline_after(idle: Duration) -> Instant {
    let now = Instant::now();
    // An unrepresentable interval must fail closed, rather than panic or
    // accidentally disable cleanup. The production interval is five minutes.
    now.checked_add(idle).unwrap_or(now)
}

impl IdleBody {
    fn new(body: Body, idle: Duration) -> Self {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                body: Some(body),
                deadline: deadline_after(idle),
                expired: false,
            }),
            waker: AtomicWaker::new(),
        });
        // Never retain the body across a timer wait: dropping the response must
        // immediately cancel its producer, rather than wait for this deadline.
        let weak = Arc::downgrade(&shared);
        let watchdog = tokio::spawn(async move {
            loop {
                let deadline = {
                    let Some(owner) = weak.upgrade() else { return };
                    let state = owner
                        .state
                        .lock()
                        .unwrap_or_else(|error| error.into_inner());
                    if state.body.is_none() {
                        return;
                    }
                    state.deadline
                };
                tokio::time::sleep_until(deadline).await;
                let Some(expired_owner) = weak.upgrade() else {
                    return;
                };
                let expired = {
                    let mut state = expired_owner
                        .state
                        .lock()
                        .unwrap_or_else(|error| error.into_inner());
                    if Instant::now() < state.deadline {
                        continue;
                    }
                    if state.body.is_some() {
                        state.expired = true;
                    }
                    state.body.take()
                };
                // Producer destruction can run cleanup; do it outside the lock.
                drop(expired);
                expired_owner.waker.wake();
                return;
            }
        });
        Self {
            shared,
            watchdog,
            idle,
            reported: false,
        }
    }
}

impl Drop for IdleBody {
    fn drop(&mut self) {
        self.watchdog.abort();
    }
}

impl HttpBody for IdleBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let this = self.get_mut();
        this.shared.waker.register(cx.waker());
        let mut state = this
            .shared
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.body.is_some() && Instant::now() >= state.deadline {
            state.expired = true;
            let expired = state.body.take();
            drop(state);
            drop(expired);
            state = this
                .shared
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
        }
        if state.expired && !this.reported {
            this.reported = true;
            return Poll::Ready(Some(Err(axum::Error::new(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "response body made no progress before its idle deadline",
            )))));
        }
        let Some(body) = state.body.as_mut() else {
            return Poll::Ready(None);
        };
        let frame = Pin::new(body).poll_frame(cx);
        if let Poll::Ready(Some(Ok(frame))) = &frame
            && frame.data_ref().is_some_and(|bytes| !bytes.is_empty())
        {
            state.deadline = deadline_after(this.idle);
        }
        if matches!(&frame, Poll::Ready(None | Some(Err(_)))) {
            let finished = state.body.take();
            drop(state);
            drop(finished);
            this.watchdog.abort();
        }
        frame
    }

    fn is_end_stream(&self) -> bool {
        let state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.expired {
            return self.reported;
        }
        state.body.as_ref().is_none_or(HttpBody::is_end_stream)
    }

    fn size_hint(&self) -> SizeHint {
        let state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state
            .body
            .as_ref()
            .map_or_else(SizeHint::default, HttpBody::size_hint)
    }
}

#[cfg(test)]
mod tests;
