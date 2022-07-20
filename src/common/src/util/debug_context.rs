// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// FIXME: This is a false-positive clippy test, remove this while bumping toolchain.
// https://github.com/tokio-rs/tokio/issues/4836
// https://github.com/rust-lang/rust-clippy/issues/8493
#![expect(clippy::declare_interior_mutable_const)]

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
