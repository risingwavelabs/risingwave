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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::config::Role;
use risingwave_common::config::streaming::CacheRefillPolicy;
use risingwave_common::hash::VirtualNode;
use risingwave_common::util::epoch::test_epoch;
use risingwave_hummock_sdk::compaction_group::group_split::split_sst_with_table_ids;
use risingwave_hummock_sdk::key::{FullKey, UserKey, prefix_slice_with_vnode};
use risingwave_hummock_sdk::sstable_info::{SstableInfo, SstableInfoInner};
use risingwave_hummock_sdk::{EpochWithGap, HummockSstableObjectId};
use risingwave_pb::id::TableId;
use tokio::sync::Semaphore;
use tokio::sync::mpsc::unbounded_channel;

use super::super::test_utils::{
    deleted_sst, gen_test_sst_with_object_id, mock_sstable_store_with_disk_cache,
    pinned_version_for_test, test_refill_config,
};
use super::super::{
    CacheRefillConfig, CacheRefiller, SpawnRefillTask, SstDeltaInfo, TableCacheRefillContext,
    TableCacheRefillContextMap,
};
use super::{
    CacheRefillContext, DataCacheRefillTask, DataCacheRefillTaskGenerator,
    table_cache_refill_contexts,
};
use crate::hummock::iterator::test_utils::{iterator_test_table_key_of, mock_sstable_store};
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::recent_filter::simple::SimpleRecentFilter;
use crate::hummock::table_cache_metadata::TableCacheMetadata;
use crate::hummock::test_utils::{default_builder_opt_for_test, gen_test_sstable_with_table_ids};
use crate::hummock::value::HummockValue;
use crate::hummock::{RecentFilter, RecentFilterTrait, SstableStoreRef, TableHolder};

struct DataRefillGeneratorTestFixture {
    table_id: TableId,
    sstable_store: SstableStoreRef,
    sst: TableHolder,
    sst_info: SstableInfo,
    deleted_sst_object_id: HummockSstableObjectId,
    _cache_dir: tempfile::TempDir,
}

impl DataRefillGeneratorTestFixture {
    async fn new(
        recent_filter: Option<Arc<RecentFilter<(HummockSstableObjectId, usize)>>>,
    ) -> Self {
        let table_id = TableId::from(233);
        let (sstable_store, cache_dir) = mock_sstable_store_with_disk_cache(recent_filter).await;
        let (sst, sst_info) = gen_test_sst_with_object_id(table_id, sstable_store.clone(), 1).await;
        Self {
            table_id,
            sstable_store,
            sst,
            sst_info,
            deleted_sst_object_id: 2330.into(),
            _cache_dir: cache_dir,
        }
    }

    fn context(
        &self,
        policy: CacheRefillPolicy,
        streaming_vnode_bitmap: Option<Bitmap>,
        serving_vnode_bitmap: Option<Bitmap>,
        configure: impl FnOnce(&mut CacheRefillConfig),
    ) -> CacheRefillContext {
        let mut config = test_refill_config(CacheRefillPolicy::Enabled);
        config.data_refill_levels.insert(0);
        configure(&mut config);
        CacheRefillContext {
            config: Arc::new(config),
            meta_refill_concurrency: None,
            concurrency: Arc::new(Semaphore::new(1)),
            sstable_store: self.sstable_store.clone(),
            table_cache_refill_context_map: HashMap::from([(
                self.table_id,
                TableCacheRefillContext {
                    streaming_vnode_bitmap,
                    serving_vnode_bitmap,
                    policy,
                },
            )]),
        }
    }

    fn normal_delta(
        &self,
        insert_sst_level: u32,
        deleted_sst_object_id: HummockSstableObjectId,
    ) -> SstDeltaInfo {
        SstDeltaInfo {
            insert_sst_infos: vec![self.sst_info.clone()],
            delete_sst_infos: vec![deleted_sst(deleted_sst_object_id)],
            insert_sst_level,
        }
    }

    fn normal_l0_delta(&self) -> SstDeltaInfo {
        self.normal_delta(0, self.deleted_sst_object_id)
    }

    async fn generate(
        &self,
        context: &CacheRefillContext,
        delta: &SstDeltaInfo,
    ) -> Vec<DataCacheRefillTask> {
        let generator = DataCacheRefillTaskGenerator {
            context,
            delta,
            ssts: std::slice::from_ref(&self.sst),
        };
        let tasks = generator.generate_unfiltered_tasks();
        generator.filter_by_inheritance_if_needed(tasks).await
    }
}

#[test]
fn test_table_cache_refill_contexts_by_role_and_policy() {
    struct Case {
        name: &'static str,
        role: Role,
        default_policy: CacheRefillPolicy,
        policy: Option<CacheRefillPolicy>,
        has_streaming_vnodes: bool,
        has_serving_vnodes: bool,
        expected: Option<(CacheRefillPolicy, bool, bool)>,
    }

    let cases = [
        Case {
            name: "streaming role uses streaming side of Both",
            role: Role::Streaming,
            default_policy: CacheRefillPolicy::Disabled,
            policy: Some(CacheRefillPolicy::Both),
            has_streaming_vnodes: true,
            has_serving_vnodes: true,
            expected: Some((CacheRefillPolicy::Both, true, false)),
        },
        Case {
            name: "serving role uses serving side of Both",
            role: Role::Serving,
            default_policy: CacheRefillPolicy::Disabled,
            policy: Some(CacheRefillPolicy::Both),
            has_streaming_vnodes: true,
            has_serving_vnodes: true,
            expected: Some((CacheRefillPolicy::Both, false, true)),
        },
        Case {
            name: "both role keeps both sides",
            role: Role::Both,
            default_policy: CacheRefillPolicy::Disabled,
            policy: Some(CacheRefillPolicy::Both),
            has_streaming_vnodes: true,
            has_serving_vnodes: true,
            expected: Some((CacheRefillPolicy::Both, true, true)),
        },
        Case {
            name: "both role keeps streaming-only ownership",
            role: Role::Both,
            default_policy: CacheRefillPolicy::Disabled,
            policy: Some(CacheRefillPolicy::Both),
            has_streaming_vnodes: true,
            has_serving_vnodes: false,
            expected: Some((CacheRefillPolicy::Both, true, false)),
        },
        Case {
            name: "streaming scope without ownership has no usable bitmap",
            role: Role::Streaming,
            default_policy: CacheRefillPolicy::Disabled,
            policy: Some(CacheRefillPolicy::Streaming),
            has_streaming_vnodes: false,
            has_serving_vnodes: false,
            expected: Some((CacheRefillPolicy::Streaming, false, false)),
        },
        Case {
            name: "pure serving worker excludes unmapped table",
            role: Role::Serving,
            default_policy: CacheRefillPolicy::Disabled,
            policy: Some(CacheRefillPolicy::Serving),
            has_streaming_vnodes: true,
            has_serving_vnodes: false,
            expected: None,
        },
        Case {
            name: "default Enabled retains serving ownership",
            role: Role::Serving,
            default_policy: CacheRefillPolicy::Enabled,
            policy: None,
            has_streaming_vnodes: false,
            has_serving_vnodes: true,
            expected: Some((CacheRefillPolicy::Enabled, false, true)),
        },
        Case {
            name: "explicit policy overrides default",
            role: Role::Serving,
            default_policy: CacheRefillPolicy::Enabled,
            policy: Some(CacheRefillPolicy::Disabled),
            has_streaming_vnodes: false,
            has_serving_vnodes: true,
            expected: Some((CacheRefillPolicy::Disabled, false, false)),
        },
    ];

    let table_id = TableId::from(233);
    let streaming_vnodes = Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [1, 3]);
    let serving_vnodes = Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [2, 4]);
    for case in cases {
        let mut cache_metadata = TableCacheMetadata::new(case.role, case.default_policy);

        if let Some(policy) = case.policy {
            cache_metadata
                .replace_policy_snapshot(HashMap::from([(table_id, policy)]), HashSet::new());
        }
        if case.has_streaming_vnodes {
            cache_metadata.update_streaming_table_vnodes(table_id, Some(streaming_vnodes.clone()));
        }
        if case.has_serving_vnodes {
            cache_metadata
                .replace_serving_mapping(HashMap::from([(table_id, serving_vnodes.clone())]));
        }

        let contexts = table_cache_refill_contexts(&cache_metadata, [table_id; 3]);
        let actual = contexts.get(&table_id).map(|context| {
            (
                context.policy,
                context.streaming_vnode_bitmap.as_ref(),
                context.serving_vnode_bitmap.as_ref(),
            )
        });
        let expected = case.expected.map(|(policy, streaming, serving)| {
            (
                policy,
                streaming.then_some(&streaming_vnodes),
                serving.then_some(&serving_vnodes),
            )
        });
        assert_eq!(actual, expected, "{}", case.name);
        // Ownership comes from the current policy/ownership maps, independently of Foyer policy.
        let ownership = cache_metadata.ownership();
        assert_eq!(ownership.streaming.is_some(), case.role.for_streaming());
        assert_eq!(ownership.serving.is_some(), case.role.for_serving());
        assert_eq!(
            ownership.streaming.and_then(|map| map.get(&table_id)),
            (case.role.for_streaming() && case.has_streaming_vnodes).then_some(&streaming_vnodes),
            "{}: streaming ownership",
            case.name
        );
        assert_eq!(
            ownership.serving.and_then(|map| map.get(&table_id)),
            (case.role.for_serving() && case.has_serving_vnodes).then_some(&serving_vnodes),
            "{}: serving ownership",
            case.name
        );
    }
}

#[tokio::test]
async fn test_refill_task_captures_runtime_context_snapshot() {
    let table_id = TableId::from(233);
    let old_vnodes = Bitmap::ones(VirtualNode::COUNT_FOR_TEST);
    let new_vnodes = Bitmap::from_range(VirtualNode::COUNT_FOR_TEST, 0..8);
    let captured_context = Arc::new(Mutex::new(None::<CacheRefillContext>));
    let captured_context_clone = captured_context.clone();
    let spawn_refill_task: SpawnRefillTask = Arc::new(move |_, context| {
        *captured_context_clone.lock() = Some(context);
        tokio::spawn(async {})
    });
    let refill_config = test_refill_config(CacheRefillPolicy::Enabled);
    let refill_store = mock_sstable_store().await;
    let mut cache_metadata = TableCacheMetadata::new(
        Role::Serving,
        refill_config.table_cache_refill_default_policy,
    );
    let refiller =
        CacheRefiller::new(refill_config, refill_store).with_foyer_spawn_task(spawn_refill_task);

    cache_metadata.replace_policy_snapshot(
        HashMap::from([(table_id, CacheRefillPolicy::Serving)]),
        HashSet::new(),
    );
    cache_metadata.replace_serving_mapping(HashMap::from([(table_id, old_vnodes.clone())]));

    let _condition = refiller.start_cache_refill(
        vec![SstDeltaInfo {
            insert_sst_infos: vec![SstableInfo::from(SstableInfoInner {
                table_ids: vec![table_id],
                ..Default::default()
            })],
            ..Default::default()
        }],
        pinned_version_for_test(),
        Some(&[]),
        &cache_metadata,
    );
    cache_metadata.replace_policy_snapshot(
        HashMap::from([(table_id, CacheRefillPolicy::Disabled)]),
        HashSet::new(),
    );
    cache_metadata.replace_serving_mapping(HashMap::from([(table_id, new_vnodes.clone())]));
    assert_eq!(
        cache_metadata
            .ownership()
            .serving
            .and_then(|map| map.get(&table_id)),
        Some(&new_vnodes)
    );

    let captured_context = captured_context.lock();
    let context = captured_context
        .as_ref()
        .unwrap()
        .table_cache_refill_context_map
        .get(&table_id)
        .unwrap();
    assert_eq!(context.policy, CacheRefillPolicy::Serving);
    assert_eq!(context.serving_vnode_bitmap.as_ref(), Some(&old_vnodes));
}

#[tokio::test]
async fn test_cache_refill_prunes_whole_ssts_before_meta_load() {
    let streaming_table = TableId::from(1);
    let serving_table = TableId::from(2);
    let disabled_table = TableId::from(3);
    let internal_table = TableId::from(4);
    let fallback_table = TableId::from(5);
    let owned_vnodes = Bitmap::ones(VirtualNode::COUNT_FOR_TEST);
    let sstable_store = mock_sstable_store().await;
    let sst_info = |table_ids: Vec<TableId>| {
        SstableInfo::from(SstableInfoInner {
            table_ids,
            ..Default::default()
        })
    };
    let capture_pruned_table_ids = |role,
                                    default_policy,
                                    policies: HashMap<TableId, CacheRefillPolicy>,
                                    streaming_tables: &[TableId],
                                    serving_tables: &[TableId],
                                    delta| {
        let captured_deltas = Arc::new(Mutex::new(None::<Vec<SstDeltaInfo>>));
        let captured_deltas_clone = captured_deltas.clone();
        let spawn_refill_task: SpawnRefillTask = Arc::new(move |deltas, _| {
            *captured_deltas_clone.lock() = Some(deltas);
            tokio::spawn(async {})
        });
        let refill_config = test_refill_config(default_policy);
        let refill_store = sstable_store.clone();
        let mut cache_metadata =
            TableCacheMetadata::new(role, refill_config.table_cache_refill_default_policy);
        let refiller = CacheRefiller::new(refill_config, refill_store)
            .with_foyer_spawn_task(spawn_refill_task);
        cache_metadata.replace_policy_snapshot(policies, HashSet::new());
        for table_id in streaming_tables {
            cache_metadata.update_streaming_table_vnodes(*table_id, Some(owned_vnodes.clone()));
        }
        cache_metadata.replace_serving_mapping(
            serving_tables
                .iter()
                .map(|table_id| (*table_id, owned_vnodes.clone()))
                .collect(),
        );
        let _condition = refiller.start_cache_refill(
            vec![delta],
            pinned_version_for_test(),
            Some(&[]),
            &cache_metadata,
        );
        captured_deltas
            .lock()
            .take()
            .unwrap()
            .pop()
            .unwrap()
            .insert_sst_infos
            .into_iter()
            .map(|sst| sst.table_ids.clone())
            .collect::<Vec<_>>()
    };

    let normal_delta = |insert_sst_infos| SstDeltaInfo {
        insert_sst_infos,
        delete_sst_infos: vec![deleted_sst(1.into())],
        insert_sst_level: 1,
    };
    let insert_only_delta = |insert_sst_infos| SstDeltaInfo {
        insert_sst_infos,
        delete_sst_infos: vec![],
        insert_sst_level: 0,
    };

    assert_eq!(
        capture_pruned_table_ids(
            Role::Both,
            CacheRefillPolicy::Disabled,
            HashMap::from([
                (disabled_table, CacheRefillPolicy::Disabled),
                (streaming_table, CacheRefillPolicy::Enabled),
            ]),
            &[],
            &[],
            normal_delta(vec![
                sst_info(vec![disabled_table]),
                sst_info(vec![streaming_table]),
            ]),
        ),
        vec![vec![streaming_table]],
    );

    assert_eq!(
        capture_pruned_table_ids(
            Role::Both,
            CacheRefillPolicy::Disabled,
            HashMap::from([
                (streaming_table, CacheRefillPolicy::Streaming),
                (internal_table, CacheRefillPolicy::Serving),
            ]),
            &[streaming_table],
            &[],
            normal_delta(vec![
                sst_info(vec![streaming_table]),
                sst_info(vec![internal_table]),
            ]),
        ),
        vec![vec![streaming_table]],
    );

    assert_eq!(
        capture_pruned_table_ids(
            Role::Both,
            CacheRefillPolicy::Disabled,
            HashMap::from([
                (streaming_table, CacheRefillPolicy::Streaming),
                (serving_table, CacheRefillPolicy::Serving),
                (internal_table, CacheRefillPolicy::Serving),
            ]),
            &[streaming_table],
            &[serving_table],
            insert_only_delta(vec![
                sst_info(vec![streaming_table]),
                sst_info(vec![serving_table]),
                sst_info(vec![internal_table]),
            ]),
        ),
        vec![vec![serving_table]],
    );

    assert_eq!(
        capture_pruned_table_ids(
            Role::Serving,
            CacheRefillPolicy::Disabled,
            HashMap::from([
                (streaming_table, CacheRefillPolicy::Streaming),
                (serving_table, CacheRefillPolicy::Serving),
            ]),
            &[],
            &[serving_table],
            normal_delta(vec![
                sst_info(vec![streaming_table]),
                sst_info(vec![serving_table]),
            ]),
        ),
        vec![vec![serving_table]],
    );

    assert_eq!(
        capture_pruned_table_ids(
            Role::Serving,
            CacheRefillPolicy::Disabled,
            HashMap::from([
                (streaming_table, CacheRefillPolicy::Enabled),
                (serving_table, CacheRefillPolicy::Enabled),
            ]),
            &[],
            &[serving_table],
            normal_delta(vec![
                sst_info(vec![streaming_table]),
                sst_info(vec![serving_table]),
                sst_info(vec![streaming_table, serving_table]),
            ]),
        ),
        vec![
            vec![streaming_table],
            vec![serving_table],
            vec![streaming_table, serving_table],
        ],
    );

    for (streaming_tables, serving_tables, expected) in [
        (vec![], vec![], vec![]),
        (vec![streaming_table], vec![], vec![vec![streaming_table]]),
        (vec![], vec![serving_table], vec![vec![serving_table]]),
    ] {
        assert_eq!(
            capture_pruned_table_ids(
                Role::Both,
                CacheRefillPolicy::Disabled,
                HashMap::from([
                    (streaming_table, CacheRefillPolicy::Both),
                    (serving_table, CacheRefillPolicy::Both),
                ]),
                &streaming_tables,
                &serving_tables,
                normal_delta(vec![
                    sst_info(vec![streaming_table]),
                    sst_info(vec![serving_table]),
                ]),
            ),
            expected,
            "streaming tables {streaming_tables:?}, serving tables {serving_tables:?}",
        );
    }

    let unowned_ssts = vec![
        sst_info(vec![streaming_table]),
        sst_info(vec![serving_table]),
    ];
    for delta in [
        normal_delta(unowned_ssts.clone()),
        insert_only_delta(unowned_ssts),
    ] {
        let insert_only = delta.delete_sst_infos.is_empty();
        assert_eq!(
            capture_pruned_table_ids(
                Role::Streaming,
                CacheRefillPolicy::Disabled,
                HashMap::from([
                    (streaming_table, CacheRefillPolicy::Streaming),
                    (serving_table, CacheRefillPolicy::Serving),
                ]),
                &[],
                &[],
                delta,
            ),
            Vec::<Vec<TableId>>::new(),
            "streaming worker without ownership, insert_only={insert_only}",
        );
    }

    assert_eq!(
        capture_pruned_table_ids(
            Role::Both,
            CacheRefillPolicy::Enabled,
            HashMap::from([(disabled_table, CacheRefillPolicy::Disabled)]),
            &[],
            &[],
            normal_delta(vec![
                sst_info(vec![disabled_table]),
                sst_info(vec![fallback_table]),
                sst_info(vec![disabled_table, fallback_table]),
            ]),
        ),
        vec![vec![fallback_table], vec![disabled_table, fallback_table]],
    );
}

#[tokio::test]
async fn test_data_refill_requires_disk_cache() {
    let fixture = DataRefillGeneratorTestFixture::new(None).await;
    let delta = fixture.normal_l0_delta();
    let mut context = fixture.context(CacheRefillPolicy::Enabled, None, None, |_| {});
    assert!(!fixture.generate(&context, &delta).await.is_empty());

    context.sstable_store = mock_sstable_store().await;
    assert!(!context.sstable_store.block_cache().is_hybrid());
    assert!(fixture.generate(&context, &delta).await.is_empty());
}

#[tokio::test]
async fn test_normal_refill_applies_policy_and_vnode_ownership() {
    let fixture = DataRefillGeneratorTestFixture::new(None).await;
    let delta = fixture.normal_l0_delta();
    let owned = Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [0]);
    let unowned = Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [1]);
    let cases = vec![
        ("Enabled", CacheRefillPolicy::Enabled, None, None, true),
        (
            "Disabled",
            CacheRefillPolicy::Disabled,
            Some(owned.clone()),
            Some(owned.clone()),
            false,
        ),
        (
            "Streaming match",
            CacheRefillPolicy::Streaming,
            Some(owned.clone()),
            None,
            true,
        ),
        (
            "Streaming miss",
            CacheRefillPolicy::Streaming,
            Some(unowned.clone()),
            None,
            false,
        ),
        (
            "Streaming ownership missing",
            CacheRefillPolicy::Streaming,
            None,
            None,
            false,
        ),
        (
            "Serving match",
            CacheRefillPolicy::Serving,
            None,
            Some(owned.clone()),
            true,
        ),
        (
            "Serving miss",
            CacheRefillPolicy::Serving,
            None,
            Some(unowned.clone()),
            false,
        ),
        (
            "Serving ownership missing",
            CacheRefillPolicy::Serving,
            None,
            None,
            false,
        ),
        (
            "Both streaming match",
            CacheRefillPolicy::Both,
            Some(owned.clone()),
            Some(unowned.clone()),
            true,
        ),
        (
            "Both serving match",
            CacheRefillPolicy::Both,
            Some(unowned.clone()),
            Some(owned),
            true,
        ),
        (
            "Both misses",
            CacheRefillPolicy::Both,
            Some(unowned.clone()),
            Some(unowned),
            false,
        ),
    ];

    for (name, policy, streaming_vnodes, serving_vnodes, should_refill) in cases {
        let context = fixture.context(policy, streaming_vnodes, serving_vnodes, |_| {});
        assert_eq!(
            !fixture.generate(&context, &delta).await.is_empty(),
            should_refill,
            "{name}"
        );
    }
}

#[tokio::test]
async fn test_normal_refill_applies_recent_and_inheritance_filters() {
    let recent_filter = SimpleRecentFilter::new(3, Duration::from_secs(60));
    let fixture =
        DataRefillGeneratorTestFixture::new(Some(Arc::new(recent_filter.clone().into()))).await;
    let delta = fixture.normal_l0_delta();

    let serving_context = fixture.context(
        CacheRefillPolicy::Serving,
        None,
        Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
        |config| {
            config.skip_recent_filter = false;
        },
    );
    assert!(
        fixture.generate(&serving_context, &delta).await.is_empty(),
        "explicit Serving policy is not an implicit skip_recent_filter"
    );

    recent_filter.insert((fixture.deleted_sst_object_id, usize::MAX));
    assert!(
        !fixture.generate(&serving_context, &delta).await.is_empty(),
        "explicit Serving policy should produce tasks after recent admission hits"
    );

    let (_, parent_sst_info) =
        gen_test_sst_with_object_id(fixture.table_id, fixture.sstable_store.clone(), 2).await;
    let non_l0_delta = fixture.normal_delta(1, parent_sst_info.object_id);
    let non_l0_context = fixture.context(CacheRefillPolicy::Enabled, None, None, |config| {
        config.data_refill_levels.insert(1);
        config.skip_recent_filter = false;
        config.skip_inheritance_filter = false;
    });

    recent_filter.insert((parent_sst_info.object_id, usize::MAX));
    let generator = DataCacheRefillTaskGenerator {
        context: &non_l0_context,
        delta: &non_l0_delta,
        ssts: std::slice::from_ref(&fixture.sst),
    };
    let unfiltered_tasks = generator.generate_unfiltered_tasks();
    assert_eq!(
        unfiltered_tasks
            .iter()
            .map(|task| task.blks.len())
            .sum::<usize>(),
        fixture.sst.block_count(),
        "recent-admitted blocks should reach the inheritance stage"
    );
    assert!(
        generator
            .filter_by_inheritance_if_needed(unfiltered_tasks)
            .await
            .is_empty(),
        "after recent admission, parent block recent miss should filter non-L0 normal refill"
    );

    recent_filter.insert((parent_sst_info.object_id, 0));
    let tasks = fixture.generate(&non_l0_context, &non_l0_delta).await;
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].sst.id, fixture.sst.id);
    assert_eq!(tasks[0].blks, 0..1);
}

#[tokio::test]
async fn test_l0_refill_after_truncation_uses_serving_ownership() {
    use risingwave_hummock_sdk::level::Levels;
    use risingwave_hummock_sdk::version::{
        GroupDelta, GroupDeltas, HummockVersion, HummockVersionStateTableInfo,
    };
    use risingwave_pb::hummock::{StateTableInfo, StateTableInfoDelta};

    let fixture = DataRefillGeneratorTestFixture::new(None).await;
    let group = 1.into();
    let staging_table = TableId::new(234);
    let mut version = HummockVersion::default();
    version.levels.insert(
        group,
        Levels {
            group_id: group,
            ..Default::default()
        },
    );
    version.state_table_info = HummockVersionStateTableInfo::from_protobuf_owned(
        [fixture.table_id, staging_table]
            .map(|table_id| {
                (
                    table_id,
                    StateTableInfo {
                        compaction_group_id: group,
                        ..Default::default()
                    },
                )
            })
            .into(),
    );
    let mut raw = version.version_delta_after();
    for table_id in [fixture.table_id, staging_table] {
        raw.state_table_info_delta.insert(
            table_id,
            StateTableInfoDelta {
                committed_epoch: test_epoch(233),
                compaction_group_id: group,
            },
        );
    }
    raw.group_deltas.insert(
        group,
        GroupDeltas {
            group_deltas: vec![
                GroupDelta::NewL0SubLevel(vec![fixture.sst_info.clone()]),
                GroupDelta::PruneTableIdsFromSsts([staging_table].into()),
            ],
        },
    );
    let infos = version.build_sst_delta_infos(&raw);
    let base = PinnedVersion::new(version, unbounded_channel().0);
    let mut target = (*base).clone();
    target.apply_version_delta(&raw);
    let target = base.new_with_local_version(target).unwrap();
    let mut config = test_refill_config(CacheRefillPolicy::Serving);
    config.data_refill_levels.insert(0);
    let refill_config = config;
    let refill_store = fixture.sstable_store.clone();
    let mut cache_metadata = TableCacheMetadata::new(
        Role::Serving,
        refill_config.table_cache_refill_default_policy,
    );
    let refiller = CacheRefiller::new(refill_config, refill_store);
    cache_metadata.replace_serving_mapping(HashMap::from([(
        fixture.table_id,
        Bitmap::ones(VirtualNode::COUNT_FOR_TEST),
    )]));
    fixture.sstable_store.clear_block_cache().await.unwrap();
    let condition =
        refiller.start_cache_refill(infos.clone(), target, Some(&[raw]), &cache_metadata);
    tokio::time::timeout(Duration::from_secs(5), condition)
        .await
        .unwrap();
    for block_idx in 0..fixture.sst.block_count() {
        let key = super::SstableBlockIndex {
            sst_id: fixture.sst.id,
            block_idx: block_idx as u64,
        };
        assert!(
            fixture
                .sstable_store
                .block_cache()
                .get(&key)
                .await
                .unwrap()
                .is_some(),
            "a staging-table truncation must not hide another table's L0 refill"
        );
    }
    let delta = infos.into_iter().next().unwrap();
    let cases = [
        (
            "Enabled + serving overlap",
            CacheRefillPolicy::Enabled,
            None,
            Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
            true,
        ),
        (
            "Enabled without serving ownership",
            CacheRefillPolicy::Enabled,
            None,
            None,
            false,
        ),
        (
            "Serving + serving overlap",
            CacheRefillPolicy::Serving,
            None,
            Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
            true,
        ),
        (
            "Streaming + streaming overlap",
            CacheRefillPolicy::Streaming,
            Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
            None,
            false,
        ),
        (
            "Both + streaming overlap + serving non-overlap",
            CacheRefillPolicy::Both,
            Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
            Some(Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [1])),
            false,
        ),
        (
            "Both + serving overlap",
            CacheRefillPolicy::Both,
            Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
            Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
            true,
        ),
        (
            "Disabled + serving overlap",
            CacheRefillPolicy::Disabled,
            None,
            Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
            false,
        ),
    ];

    for (name, policy, streaming_vnodes, serving_vnodes, should_refill) in cases {
        let context = fixture.context(policy, streaming_vnodes, serving_vnodes, |config| {
            config.skip_recent_filter = false;
            config.skip_inheritance_filter = false;
        });
        assert_eq!(
            !fixture.generate(&context, &delta).await.is_empty(),
            should_refill,
            "{name}"
        );
    }
}

#[tokio::test]
async fn test_refill_units_do_not_cross_table_projection_boundaries() {
    let table_a = TableId::from(233);
    let table_b = TableId::from(234);
    let (sstable_store, _cache_dir) = mock_sstable_store_with_disk_cache(None).await;
    let (sst, sst_info) = gen_test_sstable_with_table_ids(
        default_builder_opt_for_test(),
        1,
        [table_a, table_b].into_iter().map(|table_id| {
            (
                FullKey {
                    user_key: UserKey::for_test(table_id, iterator_test_table_key_of(0)),
                    epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(233)),
                },
                HummockValue::put(b"value".to_vec()),
            )
        }),
        sstable_store.clone(),
        vec![table_a.as_raw_id(), table_b.as_raw_id()],
    )
    .await;
    assert_eq!(sst.block_count(), 2, "table switch must form a new block");

    let mut next_sst_id = 100.into();
    let (table_a_projection, table_b_projection) =
        split_sst_with_table_ids(&sst_info, &mut next_sst_id, 1, 1, vec![table_b]);
    assert_eq!(table_a_projection.object_id, sst_info.object_id);
    assert_eq!(table_b_projection.object_id, sst_info.object_id);
    assert_ne!(table_a_projection.sst_id, table_b_projection.sst_id);
    assert_eq!(table_a_projection.table_ids, vec![table_a]);
    assert_eq!(table_b_projection.table_ids, vec![table_b]);

    let deltas = [table_a_projection, table_b_projection].map(|projection| SstDeltaInfo {
        insert_sst_infos: vec![projection],
        delete_sst_infos: vec![],
        insert_sst_level: 0,
    });
    let normal_deltas = deltas.clone().map(|mut delta| {
        // A synthetic delete marks this as a normal delta; recent and inheritance filters
        // are disabled below, so the test does not rely on a matching parent SST.
        delta.delete_sst_infos = vec![deleted_sst(999.into())];
        delta
    });
    let serving_vnodes = Bitmap::ones(VirtualNode::COUNT_FOR_TEST);
    let table_cache_refill_context_map = [table_a, table_b]
        .into_iter()
        .map(|table_id| {
            (
                table_id,
                TableCacheRefillContext {
                    streaming_vnode_bitmap: None,
                    serving_vnode_bitmap: Some(serving_vnodes.clone()),
                    policy: CacheRefillPolicy::Serving,
                },
            )
        })
        .collect::<TableCacheRefillContextMap>();
    let make_context = |unit| {
        let mut config = test_refill_config(CacheRefillPolicy::Disabled);
        config.data_refill_levels.insert(0);
        config.unit = unit;
        CacheRefillContext {
            config: Arc::new(config),
            meta_refill_concurrency: None,
            concurrency: Arc::new(tokio::sync::Semaphore::new(1)),
            sstable_store: sstable_store.clone(),
            table_cache_refill_context_map: table_cache_refill_context_map.clone(),
        }
    };
    for (unit, deltas, description) in [
        (1, &deltas, "each logical projection selects its own block"),
        (2, &deltas, "wide units stop at table boundaries"),
        (2, &normal_deltas, "normal refill uses the same geometry"),
    ] {
        let context = make_context(unit);
        let tasks = deltas
            .iter()
            .map(|delta| {
                DataCacheRefillTaskGenerator {
                    context: &context,
                    delta,
                    ssts: std::slice::from_ref(&sst),
                }
                .generate_unfiltered_tasks()
            })
            .collect::<Vec<_>>();
        let ranges = tasks
            .iter()
            .map(|tasks| {
                tasks
                    .iter()
                    .map(|task| task.blks.clone())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(ranges, vec![vec![0..1], vec![1..2]], "{description}");

        for task in tasks.into_iter().flatten() {
            assert!(task.blks.len() <= unit);
            assert_eq!(
                task.sst.meta.block_metas[task.blks.start].table_id(),
                task.sst.meta.block_metas[task.blks.end - 1].table_id(),
                "a refill unit must not cross a table boundary"
            );
        }
    }
}

#[tokio::test]
async fn test_scoped_refill_handles_multi_table_vnode_boundary() {
    let table_a = TableId::from(233);
    let table_b = TableId::from(234);
    let vnode_a = VirtualNode::COUNT_FOR_TEST - 1;
    let (sstable_store, _cache_dir) = mock_sstable_store_with_disk_cache(None).await;
    let (sst, sst_info) = gen_test_sstable_with_table_ids(
        default_builder_opt_for_test(),
        1,
        [
            (
                FullKey {
                    user_key: UserKey::for_test(
                        table_a,
                        prefix_slice_with_vnode(VirtualNode::from_index(vnode_a), b"table_a"),
                    ),
                    epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(233)),
                },
                HummockValue::put(Bytes::from_static(b"a")),
            ),
            (
                FullKey {
                    user_key: UserKey::for_test(
                        table_b,
                        prefix_slice_with_vnode(VirtualNode::ZERO, b"table_b"),
                    ),
                    epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(233)),
                },
                HummockValue::put(Bytes::from_static(b"b")),
            ),
        ]
        .into_iter(),
        sstable_store.clone(),
        vec![table_a.as_raw_id(), table_b.as_raw_id()],
    )
    .await;
    assert_eq!(sst.block_count(), 2, "table switch must form a new block");

    let delta = SstDeltaInfo {
        insert_sst_infos: vec![sst_info],
        delete_sst_infos: vec![deleted_sst(2330.into())],
        insert_sst_level: 0,
    };
    let generate = |streaming_vnodes| {
        let mut config = test_refill_config(CacheRefillPolicy::Streaming);
        config.data_refill_levels.insert(0);
        let context = CacheRefillContext {
            config: Arc::new(config),
            meta_refill_concurrency: None,
            concurrency: Arc::new(tokio::sync::Semaphore::new(1)),
            sstable_store: sstable_store.clone(),
            table_cache_refill_context_map: HashMap::from([(
                table_a,
                TableCacheRefillContext {
                    streaming_vnode_bitmap: Some(streaming_vnodes),
                    serving_vnode_bitmap: None,
                    policy: CacheRefillPolicy::Streaming,
                },
            )]),
        };
        DataCacheRefillTaskGenerator {
            context: &context,
            delta: &delta,
            ssts: std::slice::from_ref(&sst),
        }
        // This L0 geometry case has recent and inheritance filtering disabled.
        .generate_unfiltered_tasks()
    };

    let matching_tasks = generate(Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [vnode_a]));
    assert_eq!(matching_tasks.len(), 1);
    assert_eq!(matching_tasks[0].blks, 0..1);

    let non_matching_tasks = generate(Bitmap::from_indices(
        VirtualNode::COUNT_FOR_TEST,
        [VirtualNode::ZERO.to_index()],
    ));
    assert!(non_matching_tasks.is_empty());
}

#[tokio::test]
async fn test_without_pin_backend_preserves_foyer_task_lifecycle() {
    use std::panic::AssertUnwindSafe;

    use risingwave_common::util::panic::FutureCatchUnwindExt;

    let gate = Arc::new(Semaphore::new(0));
    let worker_gate = gate.clone();
    let completed = Arc::new(tokio::sync::Notify::new());
    let worker_completed = completed.clone();
    let spawn: SpawnRefillTask = Arc::new(move |_, _| {
        let gate = worker_gate.clone();
        let completed = worker_completed.clone();
        tokio::spawn(async move {
            gate.acquire().await.unwrap().forget();
            completed.notify_one();
        })
    });
    let version = pinned_version_for_test();
    let refill_config = test_refill_config(CacheRefillPolicy::Enabled);
    let refill_store = mock_sstable_store().await;
    let cache_metadata = TableCacheMetadata::new(
        Role::Streaming,
        refill_config.table_cache_refill_default_policy,
    );
    let mut refiller = CacheRefiller::new(refill_config, refill_store).with_foyer_spawn_task(spawn);
    assert!(refiller.pin_cache_refill.is_none());
    // Work starts without polling; dropping the condition keeps legacy Foyer work alive.
    drop(refiller.start_cache_refill(vec![], version.clone(), Some(&[]), &cache_metadata));
    gate.add_permits(1);
    tokio::time::timeout(Duration::from_secs(1), completed.notified())
        .await
        .expect("dropping the condition must not abort legacy Foyer work");

    refiller.foyer.spawn_refill_task = Some(Arc::new(|_, _| {
        tokio::spawn(async { panic!("simulated Foyer task failure") })
    }));
    let condition = refiller.start_cache_refill(vec![], version, Some(&[]), &cache_metadata);
    assert!(AssertUnwindSafe(condition).rw_catch_unwind().await.is_err());
}
