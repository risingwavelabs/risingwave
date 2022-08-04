#![allow(clippy::declare_interior_mutable_const)]

use std::borrow::Cow;
use std::cell::{RefCell, RefMut};
use std::collections::{BTreeMap, HashSet};
use std::fmt::{Debug, Write};
use std::hash::Hash;
use std::pin::Pin;
use std::rc::Rc;
use std::task::Poll;
use std::time::{Duration, Instant};

use futures::future::Fuse;
use futures::{Future, FutureExt};
use itertools::Itertools;
use pin_project::{pin_project, pinned_drop};
use tokio::sync::watch;

pub type SpanValue = Cow<'static, str>;

#[derive(Clone)]
pub struct StackTreeNode {
    inner: Rc<RefCell<StackTreeNodeInner>>,
}

// SAFETY: we'll never clone the `Rc` in multiple threads.
unsafe impl Send for StackTreeNode {}

impl PartialEq for StackTreeNode {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.inner, &other.inner)
    }
}
impl Eq for StackTreeNode {}

impl Hash for StackTreeNode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.inner.as_ptr().hash(state);
    }
}

impl From<StackTreeNodeInner> for StackTreeNode {
    fn from(inner: StackTreeNodeInner) -> Self {
        Self {
            inner: Rc::new(RefCell::new(inner)),
        }
    }
}

struct StackTreeNodeInner {
    parent: Option<StackTreeNode>,
    children: HashSet<StackTreeNode>,
    start_time: Instant,
    value: SpanValue,
}

impl Debug for StackTreeNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.borrow().fmt(f)
    }
}

impl Debug for StackTreeNodeInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("children", &self.children)
            .field("value", &self.value)
            .finish_non_exhaustive()
    }
}

impl StackTreeNode {
    fn new(parent: StackTreeNode, value: SpanValue) -> Self {
        StackTreeNodeInner {
            parent: Some(parent),
            children: Default::default(),
            start_time: Instant::now(),
            value,
        }
        .into()
    }

    fn root(value: SpanValue) -> Self {
        StackTreeNodeInner {
            parent: None,
            children: Default::default(),
            start_time: Instant::now(),
            value,
        }
        .into()
    }

    fn add_child(&self, value: SpanValue) -> Self {
        let child = Self::new(self.clone(), value);
        self.mount_child(child.clone());
        child
    }

    fn mount_child(&self, child: Self) {
        let mut inner = self.inner.borrow_mut();
        assert!(inner.children.insert(child), "child already mounted");
    }

    fn delete_from_parent(&self) {
        assert!(self.delete_from_parent_unchecked(), "child not exists");
    }

    fn delete_from_parent_unchecked(&self) -> bool {
        let parent = self.parent();
        let mut parent_inner = parent.inner.borrow_mut();
        parent_inner.children.remove(self)
    }

    fn parent(&self) -> Self {
        self.inner.borrow().parent.clone().unwrap()
    }

    fn set_parent(&self, parent: Self) {
        let mut inner = self.inner.borrow_mut();
        inner.parent = Some(parent);
    }

    fn has_child(&self, child: &StackTreeNode) -> bool {
        self.inner.borrow().children.contains(child)
    }

    fn clear_children(&self) {
        self.inner.borrow_mut().children.clear();
    }
}

#[derive(Debug, Clone)]
pub struct TraceReport {
    pub report: String,
    pub time: Instant,
}

impl Default for TraceReport {
    fn default() -> Self {
        Self {
            report: "<not reported>".to_string(),
            time: Instant::now(),
        }
    }
}

impl std::fmt::Display for TraceReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[captured {:?} ago]\n{}",
            self.time.elapsed(),
            self.report
        )
    }
}

// TODO: may use a better name
#[derive(Debug)]
pub struct TraceContext {
    pub root: StackTreeNode,

    current: StackTreeNode,
}

impl std::fmt::Display for TraceContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn fmt_node(
            f: &mut std::fmt::Formatter<'_>,
            node: &StackTreeNode,
            depth: usize,
        ) -> std::fmt::Result {
            f.write_str(&" ".repeat(depth * 2))?;

            let inner = node.inner.borrow();
            f.write_str(inner.value.as_ref())?;

            let elapsed = inner.start_time.elapsed();
            f.write_fmt(format_args!(
                " [{}{:?}]",
                if depth > 0 && elapsed.as_secs() >= 1 {
                    "!!! "
                } else {
                    ""
                },
                elapsed
            ))?;

            f.write_char('\n')?;
            for child in inner
                .children
                .iter()
                .sorted_by(|a, b| a.inner.borrow().value.cmp(&b.inner.borrow().value))
            {
                assert_eq!(&child.parent(), node);
                fmt_node(f, child, depth + 1)?;
            }

            Ok(())
        }

        fmt_node(f, &self.root, 0)
    }
}

impl TraceContext {
    fn new(root_span: SpanValue) -> Self {
        let root = StackTreeNode::root(root_span);

        Self {
            root: root.clone(),
            current: root,
        }
    }

    fn to_report(&self) -> TraceReport {
        let report = format!("{}", self);
        TraceReport {
            report,
            time: Instant::now(),
        }
    }

    fn push(&mut self, span: SpanValue) -> StackTreeNode {
        let new_current_node = self.current.add_child(span);
        self.current = new_current_node.clone();
        new_current_node
    }

    fn step_in(&mut self, child: &StackTreeNode) {
        if !self.current.has_child(child) {
            child.delete_from_parent_unchecked();
            child.set_parent(self.current.clone());
            self.current.mount_child(child.clone());
        }
        assert!(self.current.has_child(child));
        self.current = child.clone();
    }

    fn pop(&mut self, child: &StackTreeNode) {
        child.delete_from_parent();
        self.current = child.parent();
    }

    fn step_out(&mut self) {
        self.current = self.current.parent();
    }
}

tokio::task_local! {
    pub static TRACE_CONTEXT: RefCell<TraceContext>
}

fn with_write_context<F, R>(f: F) -> R
where
    F: FnOnce(RefMut<TraceContext>) -> R,
{
    TRACE_CONTEXT.with(|trace_context| {
        let trace_context = trace_context.borrow_mut();
        f(trace_context)
    })
}

fn context_exists() -> bool {
    TRACE_CONTEXT.try_with(|_| {}).is_ok()
}

pub type TraceSender = watch::Sender<TraceReport>;
pub type TraceReceiver = watch::Receiver<TraceReport>;

#[derive(Default, Debug)]
pub struct TraceContextManager<K: Ord> {
    rxs: BTreeMap<K, TraceReceiver>,
}

impl<K: Ord + std::fmt::Debug> TraceContextManager<K> {
    pub fn register(&mut self, key: K) -> TraceSender {
        let (tx, rx) = watch::channel(Default::default());
        self.rxs.try_insert(key, rx).unwrap();
        tx
    }

    pub fn get_all(&mut self) -> impl Iterator<Item = (&K, watch::Ref<TraceReport>)> {
        self.rxs.retain(|_, rx| rx.has_changed().is_ok());
        self.rxs.iter_mut().map(|(k, v)| (k, v.borrow_and_update()))
    }
}

#[pin_project(PinnedDrop)]
pub struct StackTraced<F: Future> {
    #[pin]
    inner: F,

    span: Option<SpanValue>,

    this_node: Option<StackTreeNode>,
}

impl<F: Future> StackTraced<F> {
    fn new(inner: F, span: impl Into<SpanValue>) -> Self {
        Self {
            inner,
            span: Some(span.into()),
            this_node: None,
        }
    }
}

impl<F: Future> Future for StackTraced<F> {
    type Output = F::Output;

    // TODO: may disable on cfg(not(debug_assertions))
    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        if !context_exists() {
            return this.inner.poll(cx);
        }

        let old_current = with_write_context(|c| c.current.clone());

        let this_node = with_write_context(|mut c| match this.this_node {
            Some(this_node) => {
                c.step_in(this_node);
                this_node
            }
            None => this
                .this_node
                .insert(c.push(this.span.take().expect("node should only be created once"))),
        });

        let r = match this.inner.poll(cx) {
            Poll::Ready(r) => {
                with_write_context(|mut c| c.pop(this_node));
                *this.this_node = None;
                Poll::Ready(r)
            }
            Poll::Pending => {
                with_write_context(|mut c| c.step_out());
                Poll::Pending
            }
        };

        assert_eq!(old_current, with_write_context(|c| c.current.clone()));

        r
    }
}

#[pinned_drop]
impl<F: Future> PinnedDrop for StackTraced<F> {
    fn drop(self: Pin<&mut Self>) {
        // TODO: check we have correctly handle future cancellation here
        // TODO: may use `Weak` here
        let this = self.project();

        match this.this_node {
            Some(this_node) => {
                this_node.delete_from_parent();
                this_node.clear_children();
            }
            None => {} // not polled or ready
        }
    }
}

impl<T> StackTrace for T where T: Future {}

pub trait StackTrace: Future + Sized {
    fn stack_trace(self, span: impl Into<SpanValue>) -> Fuse<StackTraced<Self>> {
        StackTraced::new(self, span).fuse()
    }
}

pub async fn monitored<F: Future>(
    f: F,
    root_span: impl Into<SpanValue>,
    trace_sender: TraceSender,
    interval: Duration,
) -> F::Output {
    TRACE_CONTEXT
        .scope(
            RefCell::new(TraceContext::new(root_span.into())),
            async move {
                let monitor = async move {
                    let mut interval = tokio::time::interval(interval);
                    loop {
                        interval.tick().await;
                        let new_trace = TRACE_CONTEXT.with(|c| c.borrow().to_report());
                        match trace_sender.send(new_trace) {
                            Ok(_) => {}
                            Err(e) => {
                                tracing::error!("Trace monitor error: failed to send trace: {}", e);
                                futures::future::pending().await
                            }
                        }
                    }
                };

                tokio::select! {
                    output = f => output,
                    _ = monitor => unreachable!()
                }
            },
        )
        .await
}

#[cfg(test)]
mod tests {
    use futures::future::{join_all, select_all};
    use futures::StreamExt;
    use futures_async_stream::stream;
    use tokio::sync::watch;

    use super::*;

    async fn sleep(time: u64) {
        tokio::time::sleep(std::time::Duration::from_millis(time)).await;
        println!("slept {time}ms");
    }

    async fn sleep_nested() {
        join_all([
            sleep(1500).stack_trace("sleep nested 1500"),
            sleep(2500).stack_trace("sleep nested 2500"),
        ])
        .await;
    }

    async fn multi_sleep() {
        sleep(400).await;

        sleep(800).stack_trace("sleep another in multi slepp").await;
    }

    #[stream(item = ())]
    async fn stream1() {
        loop {
            sleep(150).await;
            yield;
        }
    }

    #[stream(item = ())]
    async fn stream2() {
        sleep(200).await;
        yield;
        join_all([
            sleep(400).stack_trace("sleep nested 400"),
            sleep(600).stack_trace("sleep nested 600"),
        ])
        .stack_trace("sleep nested another in stream 2")
        .await;
        yield;
    }

    async fn hello() {
        async move {
            // Join
            join_all([
                sleep(1000).boxed().stack_trace(format!("sleep {}", 1000)),
                sleep(2000).boxed().stack_trace("sleep 2000"),
                sleep_nested().boxed().stack_trace("sleep nested"),
                multi_sleep().boxed().stack_trace("multi sleep"),
            ])
            .await;

            // Join another
            join_all([
                sleep(1200).stack_trace("sleep 1200"),
                sleep(2200).stack_trace("sleep 2200"),
            ])
            .await;

            // Cancel
            select_all([
                sleep(666).boxed().stack_trace("sleep 666"),
                sleep_nested()
                    .boxed()
                    .stack_trace("sleep nested (should be cancelled)"),
            ])
            .await;

            // Check whether cleaned up
            sleep(233).stack_trace("sleep 233").await;

            // Check stream next drop
            {
                let mut stream1 = stream1().fuse().boxed();
                let mut stream2 = stream2().fuse().boxed();
                let mut count = 0;

                'outer: loop {
                    tokio::select! {
                        _ = stream1.next().stack_trace(format!("stream1 next {count}")) => {},
                        r = stream2.next().stack_trace(format!("stream2 next {count}")) => {
                            if r.is_none() { break 'outer }
                        },
                    }
                    count += 1;
                }
            }

            // Check whether cleaned up
            sleep(233).stack_trace("sleep 233").await;
        }
        .stack_trace("hello")
        .await
    }

    #[tokio::test]
    async fn test_stack_trace() {
        let (watch_tx, mut watch_rx) = watch::channel(Default::default());

        let collector = tokio::spawn(async move {
            while watch_rx.changed().await.is_ok() {
                println!("{}", &*watch_rx.borrow());
            }
        });

        monitored(hello(), "actor 233", watch_tx, Duration::from_millis(1000)).await;

        collector.await.unwrap();
    }
}
