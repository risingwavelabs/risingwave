use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::Stream;
use pin_project_lite::pin_project;

pin_project! {
    #[derive(Debug)]
    #[must_use = "streams do nothing unless polled"]
    pub struct Pausable<St>
        where St: Stream
    {
        #[pin]
        stream: St,
        paused: Arc<AtomicBool>,
    }
}

#[derive(Clone)]
pub struct Valve {
    paused: Arc<AtomicBool>,
}

impl Valve {
    /// Pause the stream controlled by the valve.
    pub fn pause(&self) {
        self.paused.store(true, Ordering::Relaxed)
    }

    /// Resume the stream controlled by the valve.
    pub fn resume(&self) {
        self.paused.store(false, Ordering::Relaxed);
    }
}

impl<St> Pausable<St>
where
    St: Stream,
{
    pub(crate) fn new(stream: St) -> (Self, Valve) {
        let paused = Arc::new(AtomicBool::new(false));
        (
            Pausable {
                stream,
                paused: paused.clone(),
            },
            Valve { paused },
        )
    }
}

impl<St> Stream for Pausable<St>
where
    St: Stream,
{
    type Item = St::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        if this.paused.load(Ordering::Relaxed) {
            Poll::Pending
        } else {
            this.stream.poll_next(cx)
        }
    }
}
