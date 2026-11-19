// Copyright 2026 RisingWave Labs
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

use std::sync::Arc;

use risingwave_common::id::UserId;
use risingwave_common::license::Feature;
use risingwave_expr::{ExprError, Result, capture_context, function};

use super::context::{AUTH_CONTEXT, META_CLIENT};
use crate::meta_client::FrontendMetaClient;

/// Warm up a table's caches and return the number of keys scanned across compute nodes.
/// Requires the `TableCacheWarmUp` license feature and table ownership or superuser privileges.
#[function("rw_warmup(int4) -> int8", volatile)]
async fn rw_warmup(table_id: i32) -> Result<i64> {
    let table_id = u32::try_from(table_id)
        .ok()
        .filter(|id| *id > 0)
        .ok_or_else(|| ExprError::InvalidParam {
            name: "table_id",
            reason: "must be a positive table ID".into(),
        })?;
    Feature::TableCacheWarmUp
        .check_available()
        .map_err(|error| ExprError::Internal(error.into()))?;
    let user_id = AUTH_CONTEXT::try_with(|auth_context| auth_context.user_id)?;
    rw_warmup_impl_captured(table_id, user_id).await
}

#[capture_context(META_CLIENT)]
async fn rw_warmup_impl(
    meta_client: &Arc<dyn FrontendMetaClient>,
    table_id: u32,
    user_id: UserId,
) -> Result<i64> {
    let (key_count, _) = meta_client
        .warm_up_table_cache(table_id.into(), 16, user_id)
        .await
        .map_err(|error| ExprError::Internal(error.into()))?;
    i64::try_from(key_count).map_err(|_| ExprError::NumericOutOfRange)
}

#[cfg(test)]
mod tests {
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::ScalarImpl;
    use thiserror_ext::AsReport;

    use crate::expr::{ExprImpl, ExprType, FunctionCall};
    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn warmup_is_runtime_and_respects_case() {
        let warmup: ExprImpl =
            FunctionCall::new(ExprType::RwWarmup, vec![ExprImpl::literal_int(-1)])
                .unwrap()
                .into();
        assert!(warmup.is_async());
        assert!(!warmup.is_pure());
        assert!(warmup.try_fold_const().is_none());
        assert!(warmup.eval_row(&OwnedRow::empty()).await.is_err());

        // Neither the invalid ID nor the missing frontend contexts may be evaluated here.
        let case: ExprImpl = FunctionCall::new(
            ExprType::Case,
            vec![
                ExprImpl::literal_bool(false),
                warmup,
                ExprImpl::literal_bigint(7),
            ],
        )
        .unwrap()
        .into();
        assert_eq!(
            case.eval_row(&OwnedRow::empty()).await.unwrap(),
            Some(ScalarImpl::Int64(7))
        );
    }

    #[tokio::test]
    async fn warmup_rejects_streaming_queries() {
        let frontend = LocalFrontend::new(Default::default()).await;
        let error = frontend
            .run_sql("CREATE MATERIALIZED VIEW warmup_mv AS SELECT rw_warmup(1)")
            .await
            .unwrap_err();
        assert!(
            error
                .to_report_string()
                .contains("rw_warmup is only supported in batch queries")
        );
    }
}
