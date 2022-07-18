use enum_as_inner::EnumAsInner;

/// The context used for debugging. Use [`with_debug_context`] to access the context.
#[derive(Debug, Clone, EnumAsInner)]
pub enum DebugContext {
    StreamExecutor {
        actor_id: u32,
        executor_id: u32,
        // TODO: use `Arc<str>`.
        identity: String,
    },

    // TODO: add compaction task info here.
    Compaction,

    // TODO: add stage info here.
    BatchQuery,

    Unknown,
}

tokio::task_local! {
    pub static DEBUG_CONTEXT: DebugContext
}

/// Access the debug context to check which task or executor is currently running.
pub fn with_debug_context<F, R>(f: F) -> R
where
    F: Fn(&DebugContext) -> R,
{
    DEBUG_CONTEXT
        .try_with(&f)
        .unwrap_or_else(|_| f(&DebugContext::Unknown))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_debug_context() {
        DEBUG_CONTEXT
            .scope(
                DebugContext::StreamExecutor {
                    actor_id: 1,
                    executor_id: 2,
                    identity: "Test".to_string(),
                },
                async {
                    with_debug_context(|d| {
                        println!("{:?}", d);
                        assert!(d.as_stream_executor().is_some())
                    });
                },
            )
            .await;

        with_debug_context(|d| d.is_unknown());
    }
}
