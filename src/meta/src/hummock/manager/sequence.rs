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

use std::collections::HashMap;
use std::fmt::Display;
use std::sync::LazyLock;

use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_meta_model::hummock_sequence;
use risingwave_meta_model::hummock_sequence::{
    COMPACTION_GROUP_ID, COMPACTION_TASK_ID, META_BACKUP_ID, SSTABLE_OBJECT_ID,
};
use risingwave_meta_model::prelude::HummockSequence;
use sea_orm::{ActiveModelTrait, ActiveValue, DatabaseConnection, EntityTrait, TransactionTrait};
use tokio::sync::Mutex;

use crate::hummock::error::Result;
use crate::manager::MetaSrvEnv;

static SEQ_INIT: LazyLock<HashMap<String, i64>> = LazyLock::new(|| {
    maplit::hashmap! {
        COMPACTION_TASK_ID.into() => 1,
        COMPACTION_GROUP_ID.into() => StaticCompactionGroupId::End as i64 + 1,
        SSTABLE_OBJECT_ID.into() => 1,
        META_BACKUP_ID.into() => 1,
    }
});

pub struct SequenceGenerator {
    db: Mutex<DatabaseConnection>,
}

impl SequenceGenerator {
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db: Mutex::new(db) }
    }

    /// Returns start, indicates range [start, start + num).
    ///
    /// Despite being a serial function, its infrequent invocation allows for acceptable performance.
    ///
    /// If num is 0, the next seq is returned just like num is 1, but caller must not use this seq.
    pub async fn next_interval(&self, ident: &str, num: u32) -> Result<u64> {
        // TODO: add pre-allocation if necessary
        let guard = self.db.lock().await;
        let txn = guard.begin().await?;
        let model: Option<hummock_sequence::Model> =
            hummock_sequence::Entity::find_by_id(ident.to_owned())
                .one(&txn)
                .await?;
        let start_seq = match model {
            None => {
                let init: u64 = SEQ_INIT
                    .get(ident)
                    .copied()
                    .unwrap_or_else(|| panic!("seq {ident} not found"))
                    as u64;
                let active_model = hummock_sequence::ActiveModel {
                    name: ActiveValue::set(ident.into()),
                    seq: ActiveValue::set(init.checked_add(num as _).unwrap().try_into().unwrap()),
                };
                HummockSequence::insert(active_model).exec(&txn).await?;
                init
            }
            Some(model) => {
                let start_seq: u64 = model.seq as u64;
                if num > 0 {
                    let mut active_model: hummock_sequence::ActiveModel = model.into();
                    active_model.seq = ActiveValue::set(
                        start_seq.checked_add(num as _).unwrap().try_into().unwrap(),
                    );
                    active_model.update(&txn).await?;
                }
                start_seq
            }
        };
        if num > 0 {
            txn.commit().await?;
        }
        Ok(start_seq)
    }
}

pub async fn next_compaction_task_id(env: &MetaSrvEnv) -> Result<u64> {
    env.hummock_seq.next_interval(COMPACTION_TASK_ID, 1).await
}

pub async fn next_meta_backup_id(env: &MetaSrvEnv) -> Result<u64> {
    env.hummock_seq.next_interval(META_BACKUP_ID, 1).await
}

pub async fn next_compaction_group_id(env: &MetaSrvEnv) -> Result<u64> {
    env.hummock_seq.next_interval(COMPACTION_GROUP_ID, 1).await
}

pub async fn next_sstable_object_id(
    env: &MetaSrvEnv,
    num: impl TryInto<u32> + Display + Copy,
) -> Result<u64> {
    let num: u32 = num
        .try_into()
        .unwrap_or_else(|_| panic!("fail to convert {num} into u32"));
    env.hummock_seq.next_interval(SSTABLE_OBJECT_ID, num).await
}

#[cfg(test)]
mod tests {
    use crate::controller::SqlMetaStore;
    use crate::hummock::manager::sequence::{COMPACTION_TASK_ID, SequenceGenerator};

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn test_seq_gen() {
        let store = SqlMetaStore::for_test().await;
        let conn = store.conn.clone();
        let s = SequenceGenerator::new(conn);
        assert_eq!(1, s.next_interval(COMPACTION_TASK_ID, 1).await.unwrap());
        assert_eq!(2, s.next_interval(COMPACTION_TASK_ID, 10).await.unwrap());
        assert_eq!(12, s.next_interval(COMPACTION_TASK_ID, 10).await.unwrap());
    }
}
