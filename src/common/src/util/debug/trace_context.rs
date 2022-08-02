use std::borrow::Cow;
use std::cell::{RefCell, RefMut};
use std::collections::HashMap;
use std::fmt::{Debug, Write};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{Arc, RwLock, RwLockWriteGuard, Weak};
use std::task::Poll;

use futures::future::Fuse;
use futures::{Future, FutureExt};
use pin_project::{pin_project, pinned_drop};
use tokio::sync::watch;

pub type SpanValue = Cow<'static, str>;

#[derive(Clone)]
pub struct StackTreeNode {
    inner: Rc<RefCell<StackTreeNodeInner>>,
}

// SAFETY: we'll never clone the `Rc` in multiple threads.
unsafe impl Send for StackTreeNode {}

impl From<StackTreeNodeInner> for StackTreeNode {
    fn from(inner: StackTreeNodeInner) -> Self {
        Self {
            inner: Rc::new(RefCell::new(inner)),
        }
    }
}

struct StackTreeNodeInner {
    parent: Option<StackTreeNode>,
    children: Vec<StackTreeNode>,
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
            children: Vec::new(),
            value,
        }
        .into()
    }

    fn root(value: SpanValue) -> Self {
        StackTreeNodeInner {
            parent: None,
            children: Vec::new(),
            value,
        }
        .into()
    }

    fn add_child(&self, value: SpanValue) -> Self {
        let child = Self::new(self.clone(), value);
        let mut inner = self.inner.borrow_mut();
        inner.children.push(child.clone());
        child
    }

    fn delete(&self) {
        let parent = self.parent();
        assert!(parent.has_child(self));

        let mut parent_inner = parent.inner.borrow_mut();
        parent_inner
            .children
            .retain(|x| !Rc::ptr_eq(&x.inner, &self.inner));
    }

    fn delete_unchecked(&self) {
        let parent = self.parent();

        let mut parent_inner = parent.inner.borrow_mut();
        parent_inner
            .children
            .retain(|x| !Rc::ptr_eq(&x.inner, &self.inner));
    }

    fn parent(&self) -> Self {
        self.inner.borrow().parent.clone().unwrap()
    }

    fn has_child(&self, child: &StackTreeNode) -> bool {
        self.inner
            .borrow()
            .children
            .iter()
            .any(|x| Rc::ptr_eq(&x.inner, &child.inner))
    }

    fn clear_children(&self) {
        self.inner.borrow_mut().children.clear();
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
            f.write_char('\n')?;
            for child in inner.children.iter() {
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

    fn push(&mut self, span: SpanValue) -> StackTreeNode {
        let current_node = self.current.clone();
        let new_current_node = current_node.add_child(span);
        self.current = new_current_node.clone();
        new_current_node
    }

    fn step_in(&mut self, child: &StackTreeNode) {
        assert!(self.current.has_child(child));
        self.current = child.clone();
    }

    fn pop(&mut self, child: &StackTreeNode) {
        child.delete();
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

pub type TraceSender = watch::Sender<String>;
pub type TraceReceiver = watch::Receiver<String>;

#[derive(Default, Debug)]
pub struct TraceContextManager {
    rxs: RwLock<HashMap<String, TraceReceiver>>,
}

impl TraceContextManager {
    pub fn register(&self, key: String) -> TraceSender {
        let (tx, rx) = watch::channel("<not reported>".to_owned());
        self.rxs.write().unwrap().try_insert(key, rx).unwrap();
        tx
    }
}

#[pin_project(PinnedDrop)]
pub struct StackTraced<F: Future> {
    #[pin]
    inner: Fuse<F>,

    span: SpanValue,

    this_node: Option<StackTreeNode>,
}

impl<F: Future> StackTraced<F> {
    pub fn new(inner: F, span: impl Into<SpanValue>) -> Self {
        Self {
            inner: inner.fuse(),
            span: span.into(),
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

        let this_node = with_write_context(|mut c| match this.this_node {
            Some(this_node) => {
                c.step_in(this_node);
                this_node
            }
            None => this.this_node.insert(c.push(std::mem::take(this.span))),
        });

        match this.inner.poll(cx) {
            Poll::Ready(r) => {
                with_write_context(|mut c| c.pop(this_node));
                *this.this_node = None;
                Poll::Ready(r)
            }
            Poll::Pending => {
                with_write_context(|mut c| c.step_out());
                Poll::Pending
            }
        }
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
                this_node.delete_unchecked();
                this_node.clear_children();
            }
            None => {} // not polled or ready
        }
    }
}

impl<T> StackTrace for T where T: Future {}

pub trait StackTrace: Future + Sized {
    fn stack_trace(self, span: impl Into<SpanValue>) -> StackTraced<Self> {
        StackTraced::new(self, span)
    }
}

#[cfg(test)]
mod tests {
    use futures::future::{join_all, select_all};
    use tokio::sync::{oneshot, watch};

    use super::*;

    async fn sleep(time: u64) {
        tokio::time::sleep(std::time::Duration::from_millis(time)).await;
        println!("slept {time}ms");
    }

    async fn sleep_nested() {
        join_all([
            StackTraced::new(sleep(1500), "sleep nested 1500"),
            StackTraced::new(sleep(2500), "sleep nested 2500"),
        ])
        .await;
    }

    async fn multi_sleep() {
        sleep(400).await;

        StackTraced::new(sleep(800), "sleep another in multi sleep").await;
    }

    async fn hello() {
        StackTraced::new(
            async move {
                // Join
                join_all([
                    StackTraced::new(sleep(1000).boxed(), format!("sleep {}", 1000)),
                    StackTraced::new(sleep(2000).boxed(), "sleep 2000"),
                    StackTraced::new(sleep_nested().boxed(), "sleep nested"),
                    StackTraced::new(multi_sleep().boxed(), "multi sleep"),
                ])
                .await;

                // Join another
                join_all([
                    StackTraced::new(sleep(1200), "sleep 1200"),
                    StackTraced::new(sleep(2200), "sleep 2200"),
                ])
                .await;

                // Cancel
                select_all([
                    StackTraced::new(sleep(666).boxed(), "sleep 666"),
                    StackTraced::new(sleep_nested().boxed(), "sleep nested (should be cancelled)"),
                ])
                .await;

                // Check whether cleaned up
                StackTraced::new(sleep(233), "sleep 233").await;
            },
            "hello",
        )
        .await
    }

    #[tokio::test]
    async fn test_stack_trace() {
        let (watch_tx, mut watch_rx) = watch::channel(String::new());

        let collector = tokio::spawn(async move {
            while watch_rx.changed().await.is_ok() {
                println!("{}", &*watch_rx.borrow());
            }
        });

        TRACE_CONTEXT
            .scope(
                RefCell::new(TraceContext::new("actor 233".into())),
                async move {
                    let (tx, mut rx) = oneshot::channel();

                    let monitor = async move {
                        println!("Start monitor!");
                        while rx.try_recv().is_err() {
                            let new_report = TRACE_CONTEXT.with(|c| format!("{}", c.borrow()));
                            watch_tx.send_if_modified(|report| {
                                if report != &new_report {
                                    *report = new_report;
                                    true
                                } else {
                                    false
                                }
                            });
                            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        }
                    };

                    let work = async move {
                        hello().await;
                        tx.send(()).unwrap();
                    };

                    select_all([monitor.boxed(), work.boxed()]).await;
                },
            )
            .await;

        collector.await.unwrap();
    }
}
