use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::{Debug, Write};
use std::pin::Pin;
use std::sync::{Arc, RwLock, RwLockWriteGuard, Weak};
use std::task::Poll;

use futures::future::Fuse;
use futures::{Future, FutureExt};
use pin_project::pin_project;

pub type SpanValue = Cow<'static, str>;

#[derive(Clone)]
pub struct StackTreeNode {
    inner: Arc<RwLock<StackTreeNodeInner>>,
}

impl From<StackTreeNodeInner> for StackTreeNode {
    fn from(inner: StackTreeNodeInner) -> Self {
        Self {
            inner: Arc::new(RwLock::new(inner)),
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
        self.inner.read().unwrap().fmt(f)
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

    fn root() -> Self {
        StackTreeNodeInner {
            parent: None,
            children: Vec::new(),
            value: "root".into(),
        }
        .into()
    }

    fn add_child(&self, value: SpanValue) -> Self {
        let child = Self::new(self.clone(), value);
        let mut inner = self.inner.write().unwrap();
        inner.children.push(child.clone());
        child
    }

    fn delete(&self) {
        let parent = self.parent();
        assert!(parent.has_child(self));

        let mut parent_inner = parent.inner.write().unwrap();
        parent_inner
            .children
            .retain(|x| !Arc::ptr_eq(&x.inner, &self.inner));
    }

    fn parent(&self) -> Self {
        self.inner.read().unwrap().parent.clone().unwrap()
    }

    fn has_child(&self, child: &StackTreeNode) -> bool {
        self.inner
            .read()
            .unwrap()
            .children
            .iter()
            .any(|x| Arc::ptr_eq(&x.inner, &child.inner))
    }
}

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

            let inner = node.inner.read().unwrap();
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

impl Default for TraceContext {
    fn default() -> Self {
        let root = StackTreeNode::root();

        Self {
            root: root.clone(),
            current: root,
        }
    }
}

tokio::task_local! {
    pub static TRACE_CONTEXT: Arc<RwLock<TraceContext>>
}

fn with_write_context<F, R>(f: F) -> R
where
    F: FnOnce(RwLockWriteGuard<TraceContext>) -> R,
{
    TRACE_CONTEXT.with(|trace_context| {
        let trace_context = trace_context.write().unwrap();
        f(trace_context)
    })
}

fn context_exists() -> bool {
    TRACE_CONTEXT.try_with(|_| {}).is_ok()
}

#[derive(Default, Debug)]
pub struct TraceContextManager {
    contexts: RwLock<HashMap<String, Weak<RwLock<TraceContext>>>>,
}

lazy_static::lazy_static! {
    pub static ref TRACE_CONTEXT_MANAGER: TraceContextManager = Default::default();
}

impl TraceContextManager {
    pub fn register(&self, key: String) -> Arc<RwLock<TraceContext>> {
        let context = Arc::new(RwLock::new(TraceContext::default()));
        self.contexts
            .write()
            .unwrap()
            .try_insert(key, Arc::downgrade(&context))
            .unwrap();
        context
    }
}

#[pin_project]
pub struct StackTraced<F> {
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
                Poll::Ready(r)
            }
            Poll::Pending => {
                with_write_context(|mut c| c.step_out());
                Poll::Pending
            }
        }
    }
}

pub trait StackTraceExt: Future + Sized {
    fn stack_traced(self, span: impl Into<SpanValue>) -> StackTraced<Self> {
        StackTraced::new(self, span)
    }
}

#[cfg(test)]
mod tests {
    use futures::future::join_all;
    use tokio::sync::oneshot;

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
                join_all([
                    StackTraced::new(sleep(1000).boxed(), format!("sleep {}", 1000)),
                    StackTraced::new(sleep(2000).boxed(), "sleep 2000"),
                    StackTraced::new(sleep_nested().boxed(), "sleep nested"),
                    StackTraced::new(multi_sleep().boxed(), "multi sleep"),
                ])
                .await;

                join_all([
                    StackTraced::new(sleep(1200), "sleep 1200"),
                    StackTraced::new(sleep(2200), "sleep 2200"),
                ])
                .await;
            },
            "hello",
        )
        .await
    }

    #[tokio::test]
    async fn test_stack_trace() {
        let manager = Box::leak(Box::new(TraceContextManager::default()));
        let (tx, mut rx) = oneshot::channel();

        let _handle = {
            let manager = &*manager;
            tokio::spawn(async move {
                TRACE_CONTEXT
                    .scope(manager.register("actor 233".to_string()), hello())
                    .await;
                tx.send(()).unwrap();
            })
        };

        while rx.try_recv().is_err() {
            for (key, context) in manager.contexts.read().unwrap().iter() {
                if let Some(context) = context.upgrade() {
                    let context = context.read().unwrap();
                    println!("{}\n{}", key, context);
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    }
}
