// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod batch_posix_fs_list;
pub use batch_posix_fs_list::*;
mod batch_posix_fs_fetch;
pub use batch_posix_fs_fetch::*;
mod batch_iceberg_list;
pub use batch_iceberg_list::*;
mod batch_iceberg_fetch;
pub use batch_iceberg_fetch::*;

/// Define a stream executor module that is gated by a feature.
///
/// This is similar to `feature_gated_source_mod` in the connector crate, allowing heavy or
/// unpopular source implementations (and their dependencies) to be disabled at compile time
/// to decrease compilation time and binary size.
///
/// When the feature is disabled, this macro generates a dummy executor implementation that
/// returns an error indicating the feature is not enabled.
///
/// # Example
/// ```ignore
/// feature_gated_executor_mod!(
///     batch_adbc_snowflake_list,
///     BatchAdbcSnowflakeListExecutor<S: StateStore>,
///     "adbc_snowflake",
///     (
///         _actor_ctx: ActorContextRef,
///         _stream_source_core: StreamSourceCore<S>,
///         _metrics: Arc<StreamingMetrics>,
///         _barrier_receiver: UnboundedReceiver<Barrier>,
///         _barrier_manager: LocalBarrierManager,
///         _associated_table_id: Option<TableId>,
///     )
/// );
/// ```
macro_rules! feature_gated_executor_mod {
    (
        $mod_name:ident,
        $executor_name:ident <S: StateStore>,
        $source_name:literal,
        ( $( $param_name:ident : $param_type:ty ),* $(,)? )
    ) => {
        paste::paste! {
            #[cfg(feature = "source-" $source_name)]
            mod $mod_name;
            #[cfg(feature = "source-" $source_name)]
            pub use $mod_name::*;

            #[cfg(not(feature = "source-" $source_name))]
            #[doc = "Dummy implementation for executor when the feature `source-" $source_name "` is not enabled."]
            mod [<$mod_name _stub>] {
                #![allow(unused_imports)]
                use std::sync::Arc;

                use risingwave_common::id::TableId;
                use risingwave_storage::StateStore;
                use tokio::sync::mpsc::UnboundedReceiver;

                use crate::executor::prelude::*;
                use crate::executor::source::StreamSourceCore;
                use crate::task::LocalBarrierManager;

                fn err_feature_not_enabled() -> StreamExecutorError {
                    StreamExecutorError::from(anyhow::anyhow!(
                        "Feature `source-{}` is not enabled at compile time. \
                        Please enable it in `Cargo.toml` and rebuild.",
                        $source_name
                    ))
                }

                #[doc = "A dummy executor that returns an error, as the feature `source-" $source_name "` is currently not enabled."]
                pub struct $executor_name<S: StateStore> {
                    _marker: std::marker::PhantomData<S>,
                }

                impl<S: StateStore> $executor_name<S> {
                    #[allow(clippy::too_many_arguments)]
                    pub fn new( $( $param_name : $param_type ),* ) -> Self {
                        // Suppress unused variable warnings
                        $( let _ = $param_name; )*
                        Self {
                            _marker: std::marker::PhantomData,
                        }
                    }
                }

                impl<S: StateStore> Execute for $executor_name<S> {
                    fn execute(self: Box<Self>) -> BoxedMessageStream {
                        futures::stream::once(async { Err(err_feature_not_enabled()) }).boxed()
                    }
                }

                impl<S: StateStore> std::fmt::Debug for $executor_name<S> {
                    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        f.debug_struct(concat!(stringify!($executor_name), " (stub)"))
                            .finish()
                    }
                }
            }
            #[cfg(not(feature = "source-" $source_name))]
            pub use [<$mod_name _stub>]::*;
        }
    };
}

feature_gated_executor_mod!(
    batch_adbc_snowflake_list,
    BatchAdbcSnowflakeListExecutor<S: StateStore>,
    "adbc_snowflake",
    (
        _actor_ctx: ActorContextRef,
        _stream_source_core: StreamSourceCore<S>,
        _metrics: Arc<StreamingMetrics>,
        _barrier_receiver: UnboundedReceiver<Barrier>,
        _barrier_manager: LocalBarrierManager,
        _associated_table_id: Option<TableId>,
    )
);

feature_gated_executor_mod!(
    batch_adbc_snowflake_fetch,
    BatchAdbcSnowflakeFetchExecutor<S: StateStore>,
    "adbc_snowflake",
    (
        _actor_ctx: ActorContextRef,
        _stream_source_core: StreamSourceCore<S>,
        _upstream: Executor,
        _barrier_manager: LocalBarrierManager,
        _associated_table_id: Option<TableId>,
    )
);
