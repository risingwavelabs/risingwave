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

#![allow(dead_code)]

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::LazyLock;

use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_meta_model_v2::hummock_sequence;
use sea_orm::{ActiveModelTrait, ActiveValue, DatabaseConnection, EntityTrait, TransactionTrait};
use tokio::sync::Mutex;

use crate::MetaResult;

const COMPACTION_TASK_ID: &str = "compaction_task";
const COMPACTION_GROUP_ID: &str = "compaction_group";
const SSTABLE_OBJECT_ID: &str = "sstable_object";
const META_BACKUP_ID: &str = "meta_backup";

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
    pub async fn next_interval(&self, ident: &str, num: NonZeroU32) -> MetaResult<u64> {
        // TODO: add pre-allocation if necessary
        let guard = self.db.lock().await;
        let txn = guard.begin().await?;
        let model: Option<hummock_sequence::Model> =
            hummock_sequence::Entity::find_by_id(ident.to_string())
                .one(&txn)
                .await
                .unwrap();
        let start_seq = match model {
            None => {
                let init = SEQ_INIT
                    .get(ident)
                    .copied()
                    .unwrap_or_else(|| panic!("seq {ident} not found"));
                let active_model = hummock_sequence::ActiveModel {
                    name: ActiveValue::set(ident.into()),
                    seq: ActiveValue::set(init + num.get() as i64),
                };
                active_model.insert(&txn).await?;
                init
            }
            Some(model) => {
                let start_seq = model.seq;
                let mut active_model: hummock_sequence::ActiveModel = model.into();
                active_model.seq = ActiveValue::set(start_seq + num.get() as i64);
                active_model.update(&txn).await?;
                start_seq
            }
        };
        txn.commit().await?;
        Ok(u64::try_from(start_seq).unwrap_or_else(|_| panic!("seq {ident} overflow")))
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use crate::controller::SqlMetaStore;
    use crate::hummock::manager::sequence::{SequenceGenerator, COMPACTION_TASK_ID};

    #[cfg(not(madsim))]
    #[tokio::test]
    async fn test_seq_gen() {
        let store = SqlMetaStore::for_test().await;
        let conn = store.conn.clone();
        let s = SequenceGenerator::new(conn);
        assert_eq!(
            1,
            s.next_interval(COMPACTION_TASK_ID, NonZeroU32::new(1).unwrap())
                .await
                .unwrap()
        );
        assert_eq!(
            2,
            s.next_interval(COMPACTION_TASK_ID, NonZeroU32::new(10).unwrap())
                .await
                .unwrap()
        );
        assert_eq!(
            12,
            s.next_interval(COMPACTION_TASK_ID, NonZeroU32::new(10).unwrap())
                .await
                .unwrap()
        );
    }
}
