// Copyright 2023 RisingWave Labs
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

pub type Transaction = sea_orm::DatabaseTransaction;

#[cfg(not(madsim))]
#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use risingwave_pb::hummock::HummockPinnedVersion;
    use sea_orm::{EntityTrait, TransactionTrait};

    use crate::controller::SqlMetaStore;
    use crate::model::{BTreeMapTransaction, ValTransaction, VarTransaction};
    use crate::model_v2::hummock_pinned_version::Model as HummockPinnedVersionModel;
    use crate::model_v2::prelude::HummockPinnedVersion as HummockPinnedVersionEntity;
    use crate::model_v2::trx::Transaction;

    #[tokio::test]
    async fn test_simple_var_transaction_commit() {
        let store = SqlMetaStore::for_test().await;
        let db = &store.conn;
        let mut kv = HummockPinnedVersion {
            context_id: 1,
            min_pinned_id: 2,
        };
        let mut num_txn = VarTransaction::<'_, Transaction, _>::new(&mut kv);
        num_txn.min_pinned_id = 3;
        assert_eq!(num_txn.min_pinned_id, 3);
        let mut txn = db.begin().await.unwrap();
        num_txn.apply_to_txn(&mut txn).await.unwrap();
        txn.commit().await.unwrap();
        let db_val = HummockPinnedVersionEntity::find_by_id(1)
            .one(db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(db_val.min_pinned_id, 3);
        num_txn.commit();
        assert_eq!(kv.min_pinned_id, 3);
    }

    #[test]
    fn test_simple_var_transaction_abort() {
        let mut kv = HummockPinnedVersion {
            context_id: 1,
            min_pinned_id: 11,
        };
        let mut num_txn = VarTransaction::<'_, Transaction, _>::new(&mut kv);
        num_txn.min_pinned_id = 2;
        num_txn.abort();
        assert_eq!(11, kv.min_pinned_id);
    }

    #[tokio::test]
    async fn test_tree_map_transaction_commit() {
        let mut map: BTreeMap<u32, HummockPinnedVersion> = BTreeMap::new();
        // to remove
        map.insert(
            1,
            HummockPinnedVersion {
                context_id: 1,
                min_pinned_id: 11,
            },
        );
        // to-remove-after-modify
        map.insert(
            2,
            HummockPinnedVersion {
                context_id: 2,
                min_pinned_id: 22,
            },
        );
        // first
        map.insert(
            3,
            HummockPinnedVersion {
                context_id: 3,
                min_pinned_id: 33,
            },
        );

        let mut map_copy = map.clone();
        let mut map_txn = BTreeMapTransaction::new(&mut map);
        map_txn.remove(1);
        map_txn.insert(
            2,
            HummockPinnedVersion {
                context_id: 2,
                min_pinned_id: 0,
            },
        );
        map_txn.remove(2);
        // first
        map_txn.insert(
            3,
            HummockPinnedVersion {
                context_id: 3,
                min_pinned_id: 333,
            },
        );
        // second
        map_txn.insert(
            4,
            HummockPinnedVersion {
                context_id: 4,
                min_pinned_id: 44,
            },
        );
        assert_eq!(
            &HummockPinnedVersion {
                context_id: 4,
                min_pinned_id: 44
            },
            map_txn.get(&4).unwrap()
        );
        // third
        map_txn.insert(
            5,
            HummockPinnedVersion {
                context_id: 5,
                min_pinned_id: 55,
            },
        );
        assert_eq!(
            &HummockPinnedVersion {
                context_id: 5,
                min_pinned_id: 55
            },
            map_txn.get(&5).unwrap()
        );

        let mut third_entry = map_txn.get_mut(5).unwrap();
        third_entry.min_pinned_id = 555;
        assert_eq!(
            &HummockPinnedVersion {
                context_id: 5,
                min_pinned_id: 555
            },
            map_txn.get(&5).unwrap()
        );

        let store = SqlMetaStore::for_test().await;
        let db = &store.conn;
        let mut txn = db.begin().await.unwrap();
        map_txn.apply_to_txn(&mut txn).await.unwrap();
        txn.commit().await.unwrap();

        let db_rows: Vec<HummockPinnedVersionModel> =
            HummockPinnedVersionEntity::find().all(db).await.unwrap();
        assert_eq!(db_rows.len(), 3);
        assert_eq!(
            1,
            db_rows
                .iter()
                .filter(|m| m.context_id == 3 && m.min_pinned_id == 333)
                .count()
        );
        assert_eq!(
            1,
            db_rows
                .iter()
                .filter(|m| m.context_id == 4 && m.min_pinned_id == 44)
                .count()
        );
        assert_eq!(
            1,
            db_rows
                .iter()
                .filter(|m| m.context_id == 5 && m.min_pinned_id == 555)
                .count()
        );
        map_txn.commit();

        // replay the change to local copy and compare
        map_copy.remove(&1).unwrap();
        map_copy.insert(
            2,
            HummockPinnedVersion {
                context_id: 2,
                min_pinned_id: 22,
            },
        );
        map_copy.remove(&2).unwrap();
        map_copy.insert(
            3,
            HummockPinnedVersion {
                context_id: 3,
                min_pinned_id: 333,
            },
        );
        map_copy.insert(
            4,
            HummockPinnedVersion {
                context_id: 4,
                min_pinned_id: 44,
            },
        );
        map_copy.insert(
            5,
            HummockPinnedVersion {
                context_id: 5,
                min_pinned_id: 555,
            },
        );
        assert_eq!(map_copy, map);
    }

    #[tokio::test]
    async fn test_tree_map_entry_update_transaction_commit() {
        let mut map: BTreeMap<u32, HummockPinnedVersion> = BTreeMap::new();
        map.insert(
            1,
            HummockPinnedVersion {
                context_id: 1,
                min_pinned_id: 11,
            },
        );

        let mut map_txn = BTreeMapTransaction::new(&mut map);
        let mut first_entry_txn = map_txn.new_entry_txn(1).unwrap();
        first_entry_txn.min_pinned_id = 111;

        let store = SqlMetaStore::for_test().await;
        let db = &store.conn;
        let mut txn = db.begin().await.unwrap();
        first_entry_txn.apply_to_txn(&mut txn).await.unwrap();
        txn.commit().await.unwrap();
        first_entry_txn.commit();

        let db_rows: Vec<HummockPinnedVersionModel> =
            HummockPinnedVersionEntity::find().all(db).await.unwrap();
        assert_eq!(db_rows.len(), 1);
        assert_eq!(
            1,
            db_rows
                .iter()
                .filter(|m| m.context_id == 1 && m.min_pinned_id == 111)
                .count()
        );
        assert_eq!(111, map.get(&1).unwrap().min_pinned_id);
    }

    #[tokio::test]
    async fn test_tree_map_entry_insert_transaction_commit() {
        let mut map: BTreeMap<u32, HummockPinnedVersion> = BTreeMap::new();

        let mut map_txn = BTreeMapTransaction::new(&mut map);
        let first_entry_txn = map_txn.new_entry_insert_txn(
            1,
            HummockPinnedVersion {
                context_id: 1,
                min_pinned_id: 11,
            },
        );
        let store = SqlMetaStore::for_test().await;
        let db = &store.conn;
        let mut txn = db.begin().await.unwrap();
        first_entry_txn.apply_to_txn(&mut txn).await.unwrap();
        txn.commit().await.unwrap();
        first_entry_txn.commit();
        assert_eq!(11, map.get(&1).unwrap().min_pinned_id);
    }
}
