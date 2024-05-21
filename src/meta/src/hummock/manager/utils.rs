// Copyright 2024 RisingWave Labs
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

/// Commit multiple `ValTransaction`s to state store and upon success update the local in-mem state
/// by the way
/// After called, the `ValTransaction` will be dropped.
macro_rules! commit_multi_var {
    ($meta_store:expr, $($val_txn:expr),*) => {
        {
            async {
                use crate::model::{InMemValTransaction, ValTransaction};
                match &$meta_store {
                    $crate::manager::MetaStoreImpl::Kv(meta_store) => {
                        use crate::storage::Transaction;
                        let mut trx = Transaction::default();
                        $(
                            $val_txn.apply_to_txn(&mut trx).await?;
                        )*
                        meta_store.txn(trx).await?;
                        $(
                            $val_txn.commit();
                        )*
                        Result::Ok(())
                    }
                    crate::manager::MetaStoreImpl::Sql(sql_meta_store) => {
                        use sea_orm::TransactionTrait;
                        use crate::model::MetadataModelError;
                        let mut trx = sql_meta_store.conn.begin().await.map_err(MetadataModelError::from)?;
                        $(
                            $val_txn.apply_to_txn(&mut trx).await?;
                        )*
                        trx.commit().await.map_err(MetadataModelError::from)?;
                        $(
                            $val_txn.commit();
                        )*
                        Result::Ok(())
                    }
                }
            }.await
        }
    };
}
pub(crate) use commit_multi_var;
