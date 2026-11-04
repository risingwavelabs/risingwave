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

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::time::Duration;

use bytes::Bytes;
use itertools::Itertools;
use risingwave_common::catalog::{TableId, TableOption};
use risingwave_common::util::epoch::{EpochExt, test_epoch};
use risingwave_hummock_sdk::full_text_index::FullTextIndex;
use risingwave_hummock_sdk::{HummockObjectId, HummockReadEpoch};
use risingwave_pb::catalog::PbFullTextIndexInfo;
use risingwave_rpc_client::HummockMetaClient;
use risingwave_storage::StateStore;
use risingwave_storage::hummock::HummockStorage;
use risingwave_storage::store::{
    NewFullTextWriterOptions, NewReadSnapshotOptions, SealCurrentEpochOptions,
    StateStoreReadFullText, StateStoreWriteEpochControl, StateStoreWriteFullText,
};

use crate::local_state_store_test_utils::LocalStateStoreTestExt;
use crate::test_utils::{HummockTestEnv, prepare_hummock_test_env};

const TEST_TABLE_ID: TableId = TableId::new(234);
/// Upper bound of epochs to drive while waiting for background maintenance to be committed.
const MAX_WAIT_EPOCHS: usize = 500;
/// Must match the writer's bound on pending delete files, plus the one of the epoch that is
/// committed while the writer waits.
const MAX_PENDING_DELETE_FILES: usize = 33;

fn key(i: usize) -> Bytes {
    Bytes::from(format!("key-{i:05}"))
}

fn tokens(text: &str) -> BTreeSet<String> {
    text.split_whitespace().map(str::to_owned).collect()
}

/// Expected state of the index, per committed epoch.
#[derive(Default)]
struct Model {
    /// Alive documents, including the uncommitted ones of the current epoch.
    docs: BTreeMap<Bytes, BTreeSet<String>>,
    /// Alive documents at the end of each committed epoch.
    committed: BTreeMap<u64, BTreeMap<Bytes, BTreeSet<String>>>,
}

#[expect(clippy::mutable_key_type)]
fn expected(docs: &BTreeMap<Bytes, BTreeSet<String>>, query: &BTreeSet<String>) -> Vec<Bytes> {
    docs.iter()
        .filter(|(_, doc)| query.is_subset(doc))
        .map(|(key, _)| key.clone())
        .collect()
}
struct FullTextTestEnv {
    env: HummockTestEnv,
    writer: <HummockStorage as StateStore>::FullTextWriter,
    /// The epoch being written.
    epoch: u64,
    table_ids: HashSet<TableId>,
    model: Model,
}

impl FullTextTestEnv {
    async fn new() -> Self {
        let base_epoch = test_epoch(233);
        let table_ids = HashSet::from_iter([TEST_TABLE_ID]);
        let env = prepare_hummock_test_env().await;
        env.register_full_text_index(
            TEST_TABLE_ID,
            base_epoch,
            PbFullTextIndexInfo {
                analyzer: "simple".into(),
                analyzer_version: 1,
                is_jsonb: false,
                format_version: 1,
            },
        )
        .await;
        let mut writer = env
            .storage
            .new_full_text_writer(NewFullTextWriterOptions {
                table_id: TEST_TABLE_ID,
            })
            .await;
        let epoch = base_epoch.next_epoch();
        env.storage.start_epoch(epoch, table_ids.clone());
        writer.init_for_test(epoch).await.unwrap();
        Self {
            env,
            writer,
            epoch,
            table_ids,
            model: Model::default(),
        }
    }

    fn insert(&mut self, key: Bytes, text: &str) {
        let doc = tokens(text);
        assert!(
            self.model.docs.insert(key.clone(), doc.clone()).is_none(),
            "a key must be deleted before it is inserted again"
        );
        self.writer
            .insert_document(key, &doc.into_iter().collect_vec())
            .unwrap();
    }

    fn delete(&mut self, key: Bytes) {
        assert!(
            self.model.docs.remove(&key).is_some(),
            "deleted key must be alive"
        );
        self.writer.delete_document(key).unwrap();
    }

    fn update(&mut self, key: Bytes, text: &str) {
        self.delete(key.clone());
        self.insert(key, text);
    }

    /// Seals the current epoch without flushing, like a stream executor at a non-checkpoint
    /// barrier.
    fn seal_epoch(&mut self) {
        let next_epoch = self.epoch.next_epoch();
        self.env
            .storage
            .start_epoch(next_epoch, self.table_ids.clone());
        self.writer
            .seal_current_epoch(next_epoch, SealCurrentEpochOptions::for_test());
        self.epoch = next_epoch;
    }

    /// Flushes and seals the current epoch like a stream executor at a checkpoint barrier, then
    /// syncs and commits it together with the epochs sealed before it. Returns the committed
    /// epoch.
    async fn commit_epoch(&mut self) -> u64 {
        let epoch = self.epoch;
        let next_epoch = epoch.next_epoch();
        self.env
            .storage
            .start_epoch(next_epoch, self.table_ids.clone());
        self.writer.flush().await.unwrap();
        self.writer
            .seal_current_epoch(next_epoch, SealCurrentEpochOptions::for_test());
        let res = self
            .env
            .storage
            .seal_and_sync_epoch(epoch, self.table_ids.clone())
            .await
            .unwrap();
        self.env.meta_client.commit_epoch(epoch, res).await.unwrap();
        self.env.wait_sync_committed_version().await;
        self.epoch = next_epoch;

        self.model.committed.insert(epoch, self.model.docs.clone());
        epoch
    }

    /// Commits empty epochs until `pred` holds for the committed index. Background maintenance
    /// only becomes visible in an epoch sealed after it finishes, so each round yields first.
    async fn commit_until(&mut self, desc: &str, pred: impl Fn(&FullTextIndex) -> bool) -> u64 {
        for _ in 0..MAX_WAIT_EPOCHS {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let epoch = self.commit_epoch().await;
            if pred(&self.committed_index().await) {
                return epoch;
            }
        }
        panic!(
            "{desc} not committed after {MAX_WAIT_EPOCHS} epochs: {:?}",
            self.committed_index().await
        );
    }

    async fn committed_index(&self) -> FullTextIndex {
        let version = self.env.manager.get_current_version().await;
        version
            .full_text_indexes
            .get(&TEST_TABLE_ID)
            .expect("index exists")
            .clone()
    }

    async fn search(&self, epoch: u64, query: &BTreeSet<String>) -> Vec<Bytes> {
        self.search_with_limit(epoch, query, None).await
    }

    async fn search_with_limit(
        &self,
        epoch: u64,
        query: &BTreeSet<String>,
        limit: Option<usize>,
    ) -> Vec<Bytes> {
        let snapshot = self
            .env
            .storage
            .new_read_snapshot(
                HummockReadEpoch::Committed(epoch),
                NewReadSnapshotOptions {
                    table_id: TEST_TABLE_ID,
                    table_option: TableOption::default(),
                },
            )
            .await
            .unwrap();
        snapshot
            .full_text_search(query.iter().cloned().collect(), limit)
            .await
            .unwrap()
    }

    async fn search_text(&self, epoch: u64, query: &str) -> Vec<Bytes> {
        self.search(epoch, &tokens(query)).await
    }

    /// Checks `epoch` and the epoch committed before it against the model. Results must be
    /// exact, also while delete files are pending.
    async fn check(&self, epoch: u64, queries: &[BTreeSet<String>]) {
        let index = self.committed_index().await;
        assert!(
            index.pending_delete_files.len() <= MAX_PENDING_DELETE_FILES,
            "too many pending delete files: {}",
            index.pending_delete_files.len()
        );
        let prev_epoch = self.model.committed.range(..epoch).next_back();
        let epochs = std::iter::once((&epoch, &self.model.committed[&epoch])).chain(prev_epoch);
        for (&epoch, docs) in epochs {
            for query in queries {
                let actual = self.search(epoch, query).await;
                let expected = expected(docs, query);
                assert_eq!(
                    actual,
                    expected,
                    "query {query:?} at epoch {epoch}, pending delete files: {}",
                    index.pending_delete_files.len()
                );
                for limit in [1, 2] {
                    let limited = self.search_with_limit(epoch, query, Some(limit)).await;
                    assert_eq!(limited.len(), limit.min(expected.len()));
                    assert!(
                        limited
                            .iter()
                            .all(|key| expected.binary_search(key).is_ok()),
                        "query {query:?} limit {limit} at epoch {epoch}: {limited:?}"
                    );
                }
            }
        }
        self.check_objects(&index).await;
    }

    /// Every object referenced by the index must have been uploaded.
    async fn check_objects(&self, index: &FullTextIndex) {
        let sstable_store = self.env.storage.sstable_store();
        let objects = index.get_objects().collect_vec();
        assert!(objects.iter().map(|(id, _)| *id).all_unique());
        for (object_id, file_size) in objects {
            let path = sstable_store.get_object_data_path(object_id);
            let metadata = sstable_store
                .store()
                .metadata(&path)
                .await
                .unwrap_or_else(|e| panic!("object {object_id:?} is not uploaded: {e:?}"));
            assert_eq!(metadata.total_size as u64, file_size, "{object_id:?}");
        }
    }
}

fn segment_ids(index: &FullTextIndex) -> HashSet<HummockObjectId> {
    index
        .segments
        .iter()
        .map(|segment| HummockObjectId::FullTextSegment(segment.object_id))
        .collect()
}

#[tokio::test]
async fn test_full_text_insert_delete_update() {
    let mut env = Box::pin(FullTextTestEnv::new()).await;

    env.insert(key(1), "apple banana");
    env.insert(key(2), "apple cherry");
    env.insert(key(3), "banana cherry");
    let epoch1 = env.commit_epoch().await;

    env.insert(key(4), "apple banana cherry");
    env.insert(key(5), "durian");
    let epoch2 = env.commit_epoch().await;

    env.insert(key(6), "apple");
    let epoch3 = env.commit_epoch().await;

    let index = env.committed_index().await;
    assert_eq!(index.segments.len(), 3);
    assert!(index.pending_delete_files.is_empty());

    assert_eq!(
        env.search_text(epoch3, "apple").await,
        vec![key(1), key(2), key(4), key(6)]
    );
    assert_eq!(
        env.search_text(epoch3, "apple banana").await,
        vec![key(1), key(4)]
    );
    assert_eq!(
        env.search_text(epoch3, "cherry banana").await,
        vec![key(3), key(4)]
    );
    assert_eq!(
        env.search_text(epoch3, "apple apple").await,
        vec![key(1), key(2), key(4), key(6)]
    );
    assert_eq!(
        env.search_text(epoch3, "missing").await,
        Vec::<Bytes>::new()
    );
    assert_eq!(
        env.search_text(epoch3, "apple missing").await,
        Vec::<Bytes>::new()
    );
    // Older committed epochs see older versions of the index.
    assert_eq!(env.search_text(epoch1, "apple").await, vec![key(1), key(2)]);
    assert_eq!(
        env.search_text(epoch2, "apple").await,
        vec![key(1), key(2), key(4)]
    );

    // Deletes and updates of documents in earlier epochs, together with documents that are
    // inserted and deleted or updated within the same epoch.
    env.insert(key(7), "apple grape");
    env.delete(key(7));
    env.insert(key(8), "grape");
    env.update(key(8), "kiwi");
    env.delete(key(2));
    env.update(key(3), "elder fig");
    // Deleted and re-inserted with the same tokens.
    env.update(key(6), "apple");
    let epoch4 = env.commit_epoch().await;

    // Changes within one epoch are applied before the segment is flushed.
    assert_eq!(env.search_text(epoch4, "grape").await, Vec::<Bytes>::new());
    assert_eq!(env.search_text(epoch4, "kiwi").await, vec![key(8)]);
    assert_eq!(env.search_text(epoch4, "elder").await, vec![key(3)]);
    let queries = [
        "apple",
        "banana",
        "cherry",
        "apple banana",
        "banana cherry",
        "fig elder",
    ]
    .map(tokens);
    env.check(epoch4, &queries).await;
    // The previous epoch is not affected.
    assert_eq!(
        env.search_text(epoch3, "apple").await,
        vec![key(1), key(2), key(4), key(6)]
    );
    assert_eq!(env.search_text(epoch3, "grape").await, Vec::<Bytes>::new());

    let index = env.committed_index().await;
    assert_eq!(index.pending_delete_files.len(), 1);
    assert_eq!(index.pending_delete_files[0].epoch, epoch4);
    assert_eq!(index.pending_delete_files[0].num_keys, 5);

    let epoch = env
        .commit_until("delete resolution", |index| {
            index.pending_delete_files.is_empty()
        })
        .await;
    env.check(epoch, &queries).await;
    assert_eq!(
        env.search_text(epoch, "apple").await,
        vec![key(1), key(4), key(6)]
    );
    assert_eq!(env.search_text(epoch, "cherry").await, vec![key(4)]);
    assert_eq!(env.search_text(epoch, "kiwi").await, vec![key(8)]);
    let index = env.committed_index().await;
    // Epoch 1 lost k2 and k3, epoch 3 lost the old k6. Epoch 2 is untouched.
    let deleted = index
        .segments
        .iter()
        .map(|segment| (segment.min_epoch, segment.num_deleted()))
        .collect_vec();
    assert!(deleted.contains(&(epoch1, 2)), "{deleted:?}");
    assert!(deleted.contains(&(epoch2, 0)), "{deleted:?}");

    // Deleting every document of a segment makes the segment empty, and it is removed by a
    // rewrite.
    let epoch2_segment = index
        .segments
        .iter()
        .find(|segment| segment.min_epoch == epoch2)
        .unwrap()
        .object_id;
    env.delete(key(4));
    env.delete(key(5));
    env.commit_epoch().await;
    let epoch = env
        .commit_until("removal of an all-deleted segment", |index| {
            index.pending_delete_files.is_empty()
                && index
                    .segments
                    .iter()
                    .all(|segment| segment.object_id != epoch2_segment)
        })
        .await;
    env.check(epoch, &queries).await;
    assert_eq!(env.search_text(epoch, "apple").await, vec![key(1), key(6)]);
    assert_eq!(env.search_text(epoch, "durian").await, Vec::<Bytes>::new());

    // A deleted key can be inserted again in a later epoch.
    env.insert(key(2), "apple durian");
    let epoch = env.commit_epoch().await;
    assert_eq!(
        env.search_text(epoch, "apple").await,
        vec![key(1), key(2), key(6)]
    );
    env.check(epoch, &queries).await;
}

#[tokio::test]
async fn test_full_text_merge() {
    let mut env = Box::pin(FullTextTestEnv::new()).await;
    let words = ["red", "green", "blue", "cyan"];
    let mut queries = words.map(tokens).to_vec();
    queries.push(tokens("red green"));
    queries.push(tokens("blue cyan red"));

    let mut next_key = 0;
    let mut epochs = vec![];
    let mut segments_seen = HashSet::new();
    for i in 0..12 {
        for j in 0..3 {
            let text = format!("{} {} common", words[(i + j) % 4], words[(i * j) % 4]);
            env.insert(key(next_key), &text);
            next_key += 1;
        }
        let epoch = env.commit_epoch().await;
        env.check(epoch, &queries).await;
        epochs.push(epoch);
        // A merge may already be committed while these epochs are written.
        segments_seen.extend(segment_ids(&env.committed_index().await));
    }
    let all_keys = (0..next_key).map(key).collect_vec();

    let epoch = env
        .commit_until("merge", |index| index.segments.len() < 8)
        .await;
    env.check(epoch, &queries).await;
    assert_eq!(env.search_text(epoch, "common").await, all_keys);
    let index = env.committed_index().await;
    let segments_after = segment_ids(&index);
    assert!(
        segments_seen.difference(&segments_after).count() >= 8,
        "seen {segments_seen:?}, after {segments_after:?}"
    );
    let referenced: HashSet<_> = index.get_objects().map(|(id, _)| id).collect();
    for removed in segments_seen.difference(&segments_after) {
        assert!(!referenced.contains(removed));
    }
    let merged = index
        .segments
        .iter()
        .max_by_key(|segment| segment.num_docs)
        .unwrap();
    assert!(merged.num_docs >= 24, "{index:?}");
    assert!(merged.min_epoch < merged.max_epoch);

    // Deletes of documents from different original epochs are resolved against the merged
    // segment by their own epochs.
    for i in (0..next_key).step_by(2) {
        env.delete(key(i));
    }
    // Reinsert one of them in the same epoch: the marker of this epoch must not delete it.
    env.insert(key(0), "red common");
    env.commit_epoch().await;
    let epoch = env
        .commit_until("delete resolution after merge", |index| {
            index.pending_delete_files.is_empty()
        })
        .await;
    env.check(epoch, &queries).await;
    let expected = (0..next_key)
        .filter(|i| i % 2 == 1 || *i == 0)
        .map(key)
        .collect_vec();
    assert_eq!(env.search_text(epoch, "common").await, expected);

    // Reads of epochs committed before the merge still work.
    let last_epoch_before_merge = *epochs.last().unwrap();
    assert_eq!(
        env.search_text(last_epoch_before_merge, "common").await,
        all_keys
    );
}

/// Documents are buffered across non-checkpoint epochs and flushed at the checkpoint.
#[tokio::test]
async fn test_full_text_non_checkpoint_epochs() {
    let mut env = Box::pin(FullTextTestEnv::new()).await;
    let queries = ["apple", "banana", "cherry", "apple cherry"].map(tokens);
    env.insert(key(1), "apple");
    env.insert(key(2), "banana");
    env.insert(key(3), "banana");
    env.commit_epoch().await;

    let buffer_start_epoch = env.epoch;
    env.delete(key(1));
    env.seal_epoch();
    // Re-inserted in a later epoch of the same buffer. The delete marker of the first epoch
    // must not delete it.
    env.insert(key(1), "apple cherry");
    env.delete(key(2));
    env.insert(key(4), "banana");
    env.seal_epoch();
    env.update(key(4), "cherry");
    env.insert(key(5), "apple");
    let epoch = env.commit_epoch().await;

    assert_eq!(env.search_text(epoch, "apple").await, vec![key(1), key(5)]);
    assert_eq!(env.search_text(epoch, "banana").await, vec![key(3)]);
    assert_eq!(env.search_text(epoch, "cherry").await, vec![key(1), key(4)]);
    env.check(epoch, &queries).await;
    let index = env.committed_index().await;
    assert_eq!(index.segments.len(), 2, "{index:?}");
    assert_eq!(index.pending_delete_files.len(), 1);
    assert_eq!(index.pending_delete_files[0].epoch, buffer_start_epoch);
    assert_eq!(index.pending_delete_files[0].num_keys, 3);

    let epoch = env
        .commit_until("delete resolution", |index| {
            index.pending_delete_files.is_empty()
        })
        .await;
    env.check(epoch, &queries).await;
    assert_eq!(env.search_text(epoch, "apple").await, vec![key(1), key(5)]);
}

/// Merges must still happen when every epoch carries some deletes.
#[tokio::test]
async fn test_full_text_merge_with_continuous_updates() {
    let mut env = Box::pin(FullTextTestEnv::new()).await;
    let queries = ["even", "odd", "doc"].map(tokens);
    for i in 0..4 {
        env.insert(key(i), if i % 2 == 0 { "doc even" } else { "doc odd" });
    }
    env.commit_epoch().await;
    let mut max_segments = 0;
    for round in 0..60 {
        let i = round % 4;
        let text = if round % 3 == 0 {
            "doc even"
        } else {
            "doc odd"
        };
        env.update(key(i), text);
        tokio::time::sleep(Duration::from_millis(10)).await;
        let epoch = env.commit_epoch().await;
        env.check(epoch, &queries).await;
        max_segments = max_segments.max(env.committed_index().await.segments.len());
    }
    assert!(
        max_segments <= 24,
        "segments are never merged when every epoch has deletes: {max_segments} segments"
    );
}

/// Pseudo-random workload checked against a model after every epoch.
#[tokio::test]
async fn test_full_text_random_workload() {
    const NUM_KEYS: u64 = 40;
    const WORDS: [&str; 6] = ["ant", "bee", "cat", "dog", "eel", "fox"];

    struct Lcg(u64);
    impl Lcg {
        fn below(&mut self, n: u64) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (self.0 >> 33) % n
        }
    }
    let mut rng = Lcg(20260415);
    let random_text = |rng: &mut Lcg| {
        let len = 1 + rng.below(3);
        (0..len)
            .map(|_| WORDS[rng.below(WORDS.len() as u64) as usize])
            .join(" ")
    };

    let mut queries = WORDS.map(tokens).to_vec();
    for i in 0..WORDS.len() {
        queries.push(tokens(&format!(
            "{} {}",
            WORDS[i],
            WORDS[(i + 1) % WORDS.len()]
        )));
    }
    queries.push(tokens("ant bee cat"));

    let mut env = Box::pin(FullTextTestEnv::new()).await;
    for round in 0..80 {
        let num_ops = rng.below(8);
        for _ in 0..num_ops {
            let k = key(rng.below(NUM_KEYS) as usize);
            if env.model.docs.contains_key(&k) {
                match rng.below(3) {
                    0 => env.delete(k),
                    _ => {
                        let text = random_text(&mut rng);
                        env.update(k, &text)
                    }
                }
            } else {
                let text = random_text(&mut rng);
                env.insert(k.clone(), &text);
                if rng.below(4) == 0 {
                    env.delete(k.clone());
                    if rng.below(2) == 0 {
                        let text = random_text(&mut rng);
                        env.insert(k, &text);
                    }
                }
            }
        }
        // Let background maintenance progress at different paces.
        if round % 5 == 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        if rng.below(3) == 0 {
            env.seal_epoch();
            continue;
        }
        let epoch = env.commit_epoch().await;
        env.check(epoch, &queries).await;
    }

    let epoch = env
        .commit_until("quiescence", |index| {
            index.pending_delete_files.is_empty() && index.segments.len() < 8
        })
        .await;
    env.check(epoch, &queries).await;
    let index = env.committed_index().await;
    let num_alive: u64 = index
        .segments
        .iter()
        .map(|segment| segment.num_alive())
        .sum();
    assert_eq!(num_alive, env.model.docs.len() as u64, "{index:?}");
}
