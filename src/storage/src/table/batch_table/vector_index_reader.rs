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

use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::array::{
    ArrayBuilder, ArrayImpl, ListValue, StructArrayBuilder, StructValue, VectorRef,
};
use risingwave_common::catalog::TableId;
use risingwave_common::row::RowDeserializer;
use risingwave_common::types::{DataType, ScalarImpl, ScalarRef, StructType};
use risingwave_common::util::value_encoding::BasicDeserializer;
use risingwave_common::vector::distance::DistanceMeasurement;
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_pb::common::PbDistanceType;
use risingwave_pb::plan_common::PbVectorIndexReaderDesc;

use crate::StateStore;
use crate::error::StorageResult;
use crate::store::{NewReadSnapshotOptions, StateStoreReadVector, VectorNearestOptions};

pub struct VectorIndexReader<S> {
    vector_info_struct_type: StructType,
    state_store: S,
    table_id: TableId,
    top_n: usize,
    measure: DistanceMeasurement,
    sqrt_distance: bool,
    deserializer: Arc<BasicDeserializer>,
    hnsw_ef_search: usize,
}

impl<S: StateStore> VectorIndexReader<S> {
    pub fn new(reader_desc: &PbVectorIndexReaderDesc, state_store: S) -> Self {
        let deserializer = Arc::new(RowDeserializer::new(
            reader_desc
                .info_column_desc
                .iter()
                .map(|col| DataType::from(col.column_type.clone().unwrap()))
                .collect_vec(),
        ));

        let vector_info_struct_type = StructType::new(
            reader_desc
                .info_column_desc
                .iter()
                .map(|col| {
                    (
                        col.name.clone(),
                        DataType::from(col.column_type.clone().unwrap()),
                    )
                })
                .chain([("__distance".to_owned(), DataType::Float64)]),
        );

        let measure = PbDistanceType::try_from(reader_desc.distance_type)
            .unwrap()
            .into();

        let sqrt_distance = match measure {
            DistanceMeasurement::L2Sqr => true,
            DistanceMeasurement::L1
            | DistanceMeasurement::Cosine
            | DistanceMeasurement::InnerProduct => false,
        };

        Self {
            vector_info_struct_type,
            state_store,
            table_id: reader_desc.table_id.into(),

            top_n: reader_desc.top_n as usize,
            measure,
            sqrt_distance,
            deserializer,
            hnsw_ef_search: reader_desc.hnsw_ef_search as usize,
        }
    }

    pub fn info_struct_type(&self) -> &StructType {
        &self.vector_info_struct_type
    }

    pub async fn new_snapshot(
        &self,
        epoch: HummockReadEpoch,
    ) -> StorageResult<VectorIndexSnapshot<'_, S>> {
        Ok(VectorIndexSnapshot {
            reader: self,
            snapshot: self
                .state_store
                .new_read_snapshot(
                    epoch,
                    NewReadSnapshotOptions {
                        table_id: self.table_id,
                    },
                )
                .await?,
        })
    }
}

pub struct VectorIndexSnapshot<'a, S: StateStore> {
    reader: &'a VectorIndexReader<S>,
    snapshot: S::ReadSnapshot,
}

impl<S: StateStore> VectorIndexSnapshot<'_, S> {
    pub async fn query(&self, vector: VectorRef<'_>) -> StorageResult<ListValue> {
        let deserializer = self.reader.deserializer.clone();
        let sqrt_distance = self.reader.sqrt_distance;
        let row_results: Vec<StorageResult<StructValue>> = self
            .snapshot
            .nearest(
                vector.to_owned_scalar(),
                VectorNearestOptions {
                    top_n: self.reader.top_n,
                    measure: self.reader.measure,
                    hnsw_ef_search: self.reader.hnsw_ef_search,
                },
                move |_vec, distance, value| {
                    let mut values = Vec::with_capacity(deserializer.data_types().len() + 1);
                    deserializer.deserialize_to(value, &mut values)?;
                    let distance = if sqrt_distance {
                        distance.sqrt()
                    } else {
                        distance
                    };
                    values.push(Some(ScalarImpl::Float64(distance.into())));
                    Ok(StructValue::new(values))
                },
            )
            .await?;
        let mut struct_array_builder = StructArrayBuilder::with_type(
            row_results.len(),
            DataType::Struct(self.reader.vector_info_struct_type.clone()),
        );
        for row in row_results {
            let row = row?;
            struct_array_builder.append_owned(Some(row));
        }
        let struct_array = struct_array_builder.finish();

        Ok(ListValue::new(ArrayImpl::Struct(struct_array)))
    }
}
