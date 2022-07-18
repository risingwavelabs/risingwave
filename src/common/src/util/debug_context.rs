use enum_as_inner::EnumAsInner;

#[derive(Clone, EnumAsInner)]
pub enum DebugContext {
    StreamExecutor {
        actor_id: u32,
        executor_id: u64,
        identity: String,
    },

    BatchQuery,

    Unknown,
}

impl std::fmt::Debug for DebugContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StreamExecutor {
                actor_id,
                executor_id,
                identity,
            } => f
                .debug_struct("StreamExecutor")
                .field("actor_id", actor_id)
                .field("executor_id", &(*executor_id as u32))
                .field("unique_executor_id", executor_id)
                .field("identity", identity)
                .finish(),
            Self::BatchQuery => write!(f, "BatchQuery"),
            Self::Unknown => write!(f, "Unknown"),
        }
    }
}

tokio::task_local! {
    pub static DEBUG_CONTEXT: DebugContext
}

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
        DEBUG_CONTEXT.scope(
            DebugContext::StreamExecutor {
                actor_id: 1,
                executor_id: 2,
                identity: "Test".to_string(),
            },
            async {
                with_debug_context(|d| println!("{:?}", d));
            },
        ).await;
    }
}
