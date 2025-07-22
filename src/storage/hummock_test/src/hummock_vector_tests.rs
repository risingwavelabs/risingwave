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

use std::collections::HashSet;

use bytes::Bytes;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::util::epoch::{EpochExt, test_epoch};
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_pb::hummock::vector_index_delta::{PbVectorIndexInit, vector_index_init};
use risingwave_pb::hummock::{PbDistanceType, PbFlatIndexConfig};
use risingwave_rpc_client::HummockMetaClient;
use risingwave_storage::StateStore;
use risingwave_storage::store::{
    NewReadSnapshotOptions, NewVectorWriterOptions, SealCurrentEpochOptions, StateStoreReadVector,
    StateStoreWriteEpochControl, StateStoreWriteVector, VectorNearestOptions,
};
use risingwave_storage::vector::distance::InnerProductDistance;
use risingwave_storage::vector::test_utils::{gen_info, gen_vector};
use risingwave_storage::vector::{DistanceMeasurement, NearestBuilder, Vector, VectorRef};

use crate::local_state_store_test_utils::LocalStateStoreTestExt;
use crate::test_utils::prepare_hummock_test_env;

#[tokio::test]
async fn test_flat_vector() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    const VECTOR_DIM: usize = 128;
    let base_epoch = test_epoch(233);
    let table_id_set = HashSet::from_iter([TEST_TABLE_ID]);
    let test_env = prepare_hummock_test_env().await;
    test_env
        .register_vector_index(
            TEST_TABLE_ID,
            base_epoch,
            PbVectorIndexInit {
                dimension: VECTOR_DIM as _,
                distance_type: PbDistanceType::InnerProduct as _,
                config: Some(vector_index_init::PbConfig::Flat(PbFlatIndexConfig {})),
            },
        )
        .await;
    let mut vector_writer = test_env
        .storage
        .new_vector_writer(NewVectorWriterOptions {
            table_id: TEST_TABLE_ID,
        })
        .await;

    let epoch1 = base_epoch.next_epoch();
    test_env
        .storage
        .start_epoch(epoch1, HashSet::from_iter([TEST_TABLE_ID]));
    vector_writer.init_for_test(epoch1).await.unwrap();

    let mut next_index = 0;
    let next_index = &mut next_index;
    let mut next_input = || -> (Vector, Bytes) {
        let input = gen_vector(VECTOR_DIM);
        let info = gen_info(*next_index);
        *next_index += 1;
        (input, info)
    };

    let epoch1_vectors = (0..100).map(|_| next_input()).collect_vec();
    for (vec, info) in &epoch1_vectors {
        vector_writer.insert(vec.clone(), info.clone()).unwrap();
        vector_writer.try_flush().await.unwrap();
    }

    let epoch2 = epoch1.next_epoch();
    test_env
        .storage
        .start_epoch(epoch2, HashSet::from_iter([TEST_TABLE_ID]));
    vector_writer.flush().await.unwrap();
    vector_writer.seal_current_epoch(epoch2, SealCurrentEpochOptions::for_test());

    let epoch2_vectors = (0..100).map(|_| next_input()).collect_vec();
    for (vec, info) in &epoch2_vectors {
        vector_writer.insert(vec.clone(), info.clone()).unwrap();
        vector_writer.try_flush().await.unwrap();
    }

    let epoch3 = epoch2.next_epoch();
    test_env
        .storage
        .start_epoch(epoch3, HashSet::from_iter([TEST_TABLE_ID]));
    vector_writer.flush().await.unwrap();
    vector_writer.seal_current_epoch(epoch3, SealCurrentEpochOptions::for_test());

    let res = test_env
        .storage
        .seal_and_sync_epoch(epoch1, table_id_set.clone())
        .await
        .unwrap();
    test_env
        .meta_client
        .commit_epoch(epoch1, res)
        .await
        .unwrap();

    let res = test_env
        .storage
        .seal_and_sync_epoch(epoch2, table_id_set.clone())
        .await
        .unwrap();
    test_env
        .meta_client
        .commit_epoch(epoch2, res)
        .await
        .unwrap();
    test_env.wait_sync_committed_version().await;

    let query = gen_vector(VECTOR_DIM);
    let top_n = 10;
    fn on_nearest_item(_vec: VectorRef<'_>, distance: f32, info: &[u8]) -> (f32, Bytes) {
        (distance, Bytes::copy_from_slice(info))
    }
    let check_query = async |epoch, vectors: &[&Vec<(Vector, Bytes)>]| {
        let read_snapshot_epoch = test_env
            .storage
            .new_read_snapshot(
                HummockReadEpoch::Committed(epoch),
                NewReadSnapshotOptions {
                    table_id: TEST_TABLE_ID,
                },
            )
            .await
            .unwrap();

        let output = read_snapshot_epoch
            .nearest(
                query.clone(),
                VectorNearestOptions {
                    top_n,
                    measure: DistanceMeasurement::InnerProduct,
                },
                on_nearest_item,
            )
            .await
            .unwrap();

        let mut builder = NearestBuilder::<'_, _, InnerProductDistance>::new(query.to_ref(), top_n);
        builder.add(
            vectors
                .iter()
                .cloned()
                .flatten()
                .map(|(vec, info)| (vec.to_ref(), info.as_ref())),
            &on_nearest_item,
        );
        let expected = builder.finish();
        assert_eq!(output, expected);
    };
    check_query(epoch1, &[&epoch1_vectors]).await;
    check_query(epoch2, &[&epoch1_vectors, &epoch2_vectors]).await;

    drop(vector_writer);
    let mut vector_writer = test_env
        .storage
        .new_vector_writer(NewVectorWriterOptions {
            table_id: TEST_TABLE_ID,
        })
        .await;

    vector_writer.init_for_test(epoch3).await.unwrap();
    let epoch3_vectors = (0..100).map(|_| next_input()).collect_vec();
    for (vec, info) in &epoch3_vectors {
        vector_writer.insert(vec.clone(), info.clone()).unwrap();
        vector_writer.try_flush().await.unwrap();
    }

    let epoch4 = epoch3.next_epoch();
    vector_writer.flush().await.unwrap();
    vector_writer.seal_current_epoch(epoch4, SealCurrentEpochOptions::for_test());

    let res = test_env
        .storage
        .seal_and_sync_epoch(epoch3, table_id_set.clone())
        .await
        .unwrap();
    test_env
        .meta_client
        .commit_epoch(epoch3, res)
        .await
        .unwrap();
    test_env.wait_sync_committed_version().await;

    check_query(epoch1, &[&epoch1_vectors]).await;
    check_query(epoch2, &[&epoch1_vectors, &epoch2_vectors]).await;
    check_query(epoch3, &[&epoch1_vectors, &epoch2_vectors, &epoch3_vectors]).await;
}
