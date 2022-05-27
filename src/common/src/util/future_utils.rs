use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{Stream, StreamExt};

pub struct MergeStream<S: Stream + Unpin> {
    sources: Vec<S>,
    last_base: usize,
}

impl<S: Stream + Unpin> MergeStream<S> {
    fn new(sources: Vec<S>) -> Self {
        Self {
            sources,
            last_base: 0,
        }
    }
}

impl<S: Stream + Unpin> Stream for MergeStream<S> {
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut poll_count = 0;
        while poll_count < self.sources.len() {
            let idx = (poll_count + self.last_base) % self.sources.len();
            match self.sources[idx].poll_next_unpin(cx) {
                Poll::Ready(Some(item)) => {
                    self.last_base = (idx + 1) % self.sources.len();
                    return Poll::Ready(Some(item));
                }
                Poll::Ready(None) => {
                    let _ = self.sources.swap_remove(idx);
                    // read from the front or we may miss the stream just moved from the back.
                    poll_count = 0;
                    continue;
                }
                Poll::Pending => {
                    poll_count += 1;
                    continue;
                }
            }
        }
        if !self.sources.is_empty() {
            Poll::Pending
        } else {
            Poll::Ready(None)
        }
    }
}

pub fn select_all<S: Stream + Unpin>(streams: Vec<S>) -> MergeStream<S> {
    let set = MergeStream::new(streams);
    set
}
