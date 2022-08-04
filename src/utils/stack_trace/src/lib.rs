#![allow(clippy::declare_interior_mutable_const)]
#![feature(generators)]
#![feature(map_try_insert)]

use std::borrow::Cow;
use std::cell::{RefCell, RefMut};
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Write};
use std::hash::Hash;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::Poll;
use std::time::{Duration, Instant};

use futures::future::Fuse;
use futures::{Future, FutureExt};
use itertools::Itertools;
use pin_project::{pin_project, pinned_drop};
use tokio::sync::watch;

pub type SpanValue = Cow<'static, str>;

#[derive(Clone)]
struct StackTreeNode {
    inner: Rc<RefCell<StackTreeNodeInner>>,
}

// SAFETY: by checking the id from the context against the id in the future, we ensure that this
// struct won't be touched if it's moved to another thread.
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
    // TODO: may use a more efficient timing mechanism
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
    /// Create a new node with the given parent and value.
    fn new(parent: StackTreeNode, value: SpanValue) -> Self {
        StackTreeNodeInner {
            parent: Some(parent),
            children: Default::default(),
            start_time: Instant::now(),
            value,
        }
        .into()
    }

    /// Create a new root node with the given value.
    fn root(value: SpanValue) -> Self {
        StackTreeNodeInner {
            parent: None,
            children: Default::default(),
            start_time: Instant::now(),
            value,
        }
        .into()
    }

    /// Build a new node with the given value and mount it as a child of the current node.
    fn add_child(&self, value: SpanValue) -> Self {
        let child = Self::new(self.clone(), value);
        self.mount_child(child.clone());
        child
    }

    /// Mount the given child as a child of the current node.
    fn mount_child(&self, child: Self) {
        let mut inner = self.inner.borrow_mut();
        assert!(inner.children.insert(child), "child already mounted");
    }

    /// Unmount the current node from its parent. Panics if the parent does not have this child.
    fn unmount_from_parent(&self) {
        assert!(self.unmount_from_parent_unchecked(), "child not exists");
    }

    /// Unmount the current node from its parent. Returns whether this child exists.
    fn unmount_from_parent_unchecked(&self) -> bool {
        let parent = self.parent();
        let mut parent_inner = parent.inner.borrow_mut();
        parent_inner.children.remove(self)
    }

    /// Returns the parent node of this node. Panics when called on the root node.
    fn parent(&self) -> Self {
        self.inner.borrow().parent.clone().unwrap()
    }

    /// Set the parent node of this node.
    fn set_parent(&self, parent: Self) {
        let mut inner = self.inner.borrow_mut();
        inner.parent = Some(parent);
    }

    /// Returns whether this node has the given child.
    fn has_child(&self, child: &StackTreeNode) -> bool {
        self.inner.borrow().children.contains(child)
    }

    /// Clears the references of the children of this node.
    fn clear_children(&self) {
        self.inner.borrow_mut().children.clear();
    }
}

/// The report of a stack trace.
#[derive(Debug, Clone)]
pub struct StackTraceReport {
    pub report: String,
    pub capture_time: Instant,
}

impl Default for StackTraceReport {
    fn default() -> Self {
        Self {
            report: "<not reported>".to_string(),
            capture_time: Instant::now(),
        }
    }
}

impl std::fmt::Display for StackTraceReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[captured {:?} ago]\n{}",
            self.capture_time.elapsed(),
            self.report
        )
    }
}

#[derive(Debug)]
struct TraceContext {
    id: u64,
    root: StackTreeNode,
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
    /// Create a new stack trace context with the given root span.
    fn new(root_span: SpanValue) -> Self {
        static ID: AtomicU64 = AtomicU64::new(0);

        let root = StackTreeNode::root(root_span);

        Self {
            id: ID.fetch_add(1, Ordering::SeqCst),
            root: root.clone(),
            current: root,
        }
    }

    /// Get the report of the current state of the stack trace.
    fn to_report(&self) -> StackTraceReport {
        let report = format!("{}", self);
        StackTraceReport {
            report,
            capture_time: Instant::now(),
        }
    }

    /// Push a new span as a child of current span. Returns the new current span.
    fn push(&mut self, span: SpanValue) -> StackTreeNode {
        let new_current_node = self.current.add_child(span);
        self.current = new_current_node.clone();
        new_current_node
    }

    /// Step in the current span to the given child.
    ///
    /// If the child is not actually a child of the current span, it means we are using a new future
    /// to poll it, so we need to unmount it from the previous parent, and mount it to the current
    /// span.
    fn step_in(&mut self, child: &StackTreeNode) {
        if !self.current.has_child(child) {
            // The previous parent might be already dropped, so uncheck here.
            child.unmount_from_parent_unchecked();
            child.set_parent(self.current.clone());
            self.current.mount_child(child.clone());
        }
        assert!(self.current.has_child(child));
        self.current = child.clone();
    }

    /// Pop the current span to the parent.
    fn pop(&mut self) {
        let parent = self.current.parent();
        self.current.unmount_from_parent();
        self.current = parent;
    }

    /// Step out the current span to the parent.
    fn step_out(&mut self) {
        self.current = self.current.parent();
    }
}

tokio::task_local! {
    static TRACE_CONTEXT: RefCell<TraceContext>
}

fn with_mut_context<F, R>(f: F) -> R
where
    F: FnOnce(RefMut<TraceContext>) -> R,
{
    TRACE_CONTEXT.with(|trace_context| {
        let trace_context = trace_context.borrow_mut();
        f(trace_context)
    })
}

#[pin_project(PinnedDrop)]
pub struct StackTraced<F: Future> {
    #[pin]
    inner: F,

    /// The span of this traced future, will be taken after first poll.
    span: Option<SpanValue>,

    /// The node associated with this future, will be set after first poll.
    this_node: Option<StackTreeNode>,

    /// The id of the context where this future is first polled.
    this_context_id: Option<u64>,
}

impl<F: Future> StackTraced<F> {
    fn new(inner: F, span: impl Into<SpanValue>) -> Self {
        Self {
            inner,
            span: Some(span.into()),
            this_node: None,
            this_context_id: None,
        }
    }
}

impl<F: Future> Future for StackTraced<F> {
    type Output = F::Output;

    // TODO: may disable based on features
    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        let current_context_id = TRACE_CONTEXT.try_with(|c| c.borrow().id);
        match (current_context_id, &this.this_context_id) {
            // First polled
            (Ok(id), None) => *this.this_context_id = Some(id),
            // Context correct
            (Ok(current_id), Some(this_id)) if current_id == *this_id => {}
            // Context changed
            (Ok(_), Some(_)) => {
                tracing::warn!("stack traced future is not the same context as it was first polled, are we sent to another traced task?");
                return this.inner.poll(cx);
            }
            // Out of context
            (Err(_), Some(_)) => {
                tracing::warn!("stack traced future is not in a traced context, while it was when first polled, are we sent to another task?");
                return this.inner.poll(cx);
            }
            // Not in a context ever
            (Err(_), None) => return this.inner.poll(cx),
        };

        // For assertion.
        let old_current = with_mut_context(|c| c.current.clone());

        let this_node = with_mut_context(|mut c| match this.this_node {
            // First polled, push a new span to the context.
            None => this
                .this_node
                .insert(c.push(this.span.take().expect("node should only be created once"))),
            // Polled before, just step in.
            Some(this_node) => {
                c.step_in(this_node);
                this_node
            }
        });

        let r = match this.inner.poll(cx) {
            // The future is ready, clean-up this span by popping from the context.
            Poll::Ready(output) => {
                assert_eq!(this_node, &with_mut_context(|c| c.current.clone()));
                with_mut_context(|mut c| c.pop());
                // Show that the state is clean when dropping this future.
                *this.this_node = None;
                Poll::Ready(output)
            }
            // Still pending, just step out.
            Poll::Pending => {
                with_mut_context(|mut c| c.step_out());
                Poll::Pending
            }
        };

        // The current node must be the same as we started with.
        assert_eq!(old_current, with_mut_context(|c| c.current.clone()));

        r
    }
}

#[pinned_drop]
impl<F: Future> PinnedDrop for StackTraced<F> {
    fn drop(self: Pin<&mut Self>) {
        let this = self.project();

        match this.this_node {
            Some(this_node) => {
                // Our parent may have been dropped already, so uncheck here.
                this_node.unmount_from_parent_unchecked();
                // Clear the references of the children to avoid cyclic references.
                // TODO: can we use `Weak` here?
                this_node.clear_children();
            }
            None => {} // not polled or ready
        }
    }
}

impl<T> StackTrace for T where T: Future {}

pub trait StackTrace: Future + Sized {
    /// Wrap this future, so that we're able to check the stack trace and find where and why this
    /// future is pending, with [`StackTraceReport`] and [`StackTraceManager`].
    fn stack_trace(self, span: impl Into<SpanValue>) -> Fuse<StackTraced<Self>> {
        StackTraced::new(self, span).fuse()
    }
}

pub type TraceSender = watch::Sender<StackTraceReport>;
pub type TraceReceiver = watch::Receiver<StackTraceReport>;

/// Manages the stack traces of multiple tasks.
#[derive(Default, Debug)]
pub struct StackTraceManager<K> {
    rxs: HashMap<K, TraceReceiver>,
}

impl<K> StackTraceManager<K>
where
    K: Hash + Eq + std::fmt::Debug,
{
    /// Register with given key. Returns a sender that should be provided to [`stack_traced`].
    pub fn register(&mut self, key: K) -> TraceSender {
        let (tx, rx) = watch::channel(Default::default());
        self.rxs.try_insert(key, rx).unwrap();
        tx
    }

    /// Get all trace reports registered in this manager.
    ///
    /// Note that the reports might not be updated if the traced task is doing some computation
    /// heavy work and never yields, one may see the captured time to check this.
    pub fn get_all(&mut self) -> impl Iterator<Item = (&K, watch::Ref<StackTraceReport>)> {
        self.rxs.retain(|_, rx| rx.has_changed().is_ok());
        self.rxs.iter_mut().map(|(k, v)| (k, v.borrow_and_update()))
    }
}

/// Provide a stack tracing context with the `root_span` for the given future `f`. A reporter will
/// be started in the current task and update the captured stack trace report through the given
/// `trace_sender` every `interval` time.
pub async fn stack_traced<F: Future>(
    f: F,
    root_span: impl Into<SpanValue>,
    trace_sender: TraceSender,
    interval: Duration,
) -> F::Output {
    TRACE_CONTEXT
        .scope(
            RefCell::new(TraceContext::new(root_span.into())),
            async move {
                let reporter = async move {
                    let mut interval = tokio::time::interval(interval);
                    loop {
                        interval.tick().await;
                        let new_trace = TRACE_CONTEXT.with(|c| c.borrow().to_report());
                        match trace_sender.send(new_trace) {
                            Ok(_) => {}
                            Err(e) => {
                                tracing::error!("Trace report error: failed to send trace: {}", e);
                                futures::future::pending().await
                            }
                        }
                    }
                };

                tokio::select! {
                    output = f => output,
                    _ = reporter => unreachable!()
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

            // TODO: add tests on sending the future to another task or context.
        }
        .stack_trace("hello")
        .await
    }

    #[tokio::test]
    async fn test_stack_trace_display() {
        let (watch_tx, mut watch_rx) = watch::channel(Default::default());

        let collector = tokio::spawn(async move {
            while watch_rx.changed().await.is_ok() {
                println!("{}", &*watch_rx.borrow());
            }
        });

        stack_traced(hello(), "actor 233", watch_tx, Duration::from_millis(1000)).await;

        collector.await.unwrap();
    }
}
