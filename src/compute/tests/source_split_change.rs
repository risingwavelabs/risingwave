// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::Arc;

use futures::stream::StreamExt;
use maplit::hashmap;
use risingwave_common::catalog::{ColumnId, Field, Schema, TableId};
use risingwave_common::error::Result;
use risingwave_common::util::sort_util::{OrderPair, OrderType};
use risingwave_connector::kafka::KafkaSplit;
use risingwave_connector::SplitImpl;
use risingwave_pb::catalog::StreamSourceInfo;
use risingwave_pb::data::data_type::TypeName;
use risingwave_pb::data::DataType as ProstDataType;
use risingwave_pb::plan_common::{
    ColumnCatalog as ProstColumnCatalog, ColumnDesc as ProstColumnDesc,
    RowFormatType as ProstRowFormatType,
};
use risingwave_source::{MemSourceManager, SourceDesc, SourceManager};
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::Keyspace;
use risingwave_stream::executor::monitor::StreamingMetrics;
use risingwave_stream::executor::{
    Barrier, Executor, MaterializeExecutor, Mutation, SourceExecutor,
};
use risingwave_stream::task::ActorId;
use tokio::sync::mpsc::unbounded_channel;

fn mock_stream_source_info() -> StreamSourceInfo {
    let properties: HashMap<String, String> = hashmap! {
        "kafka.brokers".to_string() => "127.0.0.1:29092".to_string(),
        "kafka.topic".to_string() => "kafka_1_partition_topic".to_string(),
        "connector".to_string() => "kafka".to_string(),
        "kafka.scan.startup.mode".to_string() => "earliest".to_string(),
    };

    let columns = vec![
        ProstColumnCatalog {
            column_desc: Some(ProstColumnDesc {
                column_type: Some(ProstDataType {
                    type_name: TypeName::Int64 as i32,
                    ..Default::default()
                }),
                column_id: 0,
                ..Default::default()
            }),
            is_hidden: false,
        },
        ProstColumnCatalog {
            column_desc: Some(ProstColumnDesc {
                column_type: Some(ProstDataType {
                    type_name: TypeName::Int32 as i32,
                    ..Default::default()
                }),
                column_id: 1,
                name: "v1".to_string(),
                ..Default::default()
            }),
            is_hidden: false,
        },
        ProstColumnCatalog {
            column_desc: Some(ProstColumnDesc {
                column_type: Some(ProstDataType {
                    type_name: TypeName::Varchar as i32,
                    ..Default::default()
                }),
                column_id: 2,
                name: "v2".to_string(),
                ..Default::default()
            }),
            is_hidden: false,
        },
    ];

    StreamSourceInfo {
        properties,
        row_format: ProstRowFormatType::Json as i32,
        row_schema_location: "".to_string(),
        row_id_index: 0,
        columns,
        pk_column_ids: vec![0],
    }
}

#[tokio::test]
async fn test_split_change_mutation() -> Result<()> {
    let stream_source_info = mock_stream_source_info();
    let source_table_id = TableId::default();
    let source_manager = Arc::new(MemSourceManager::default());

    source_manager
        .create_source(&source_table_id, stream_source_info)
        .await?;

    let get_schema = |column_ids: &[ColumnId], source_desc: &SourceDesc| {
        let mut fields = Vec::with_capacity(column_ids.len());
        for &column_id in column_ids {
            let column_desc = source_desc
                .columns
                .iter()
                .find(|c| c.column_id == column_id)
                .unwrap();
            fields.push(Field::unnamed(column_desc.data_type.clone()));
        }
        Schema::new(fields)
    };

    let actor_id = ActorId::default();
    let source_desc = source_manager.get_source(&source_table_id)?;
    let keyspace = Keyspace::table_root(MemoryStateStore::new(), &TableId::from(0x2333));
    let column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let schema = get_schema(&column_ids, &source_desc);
    let pk_indices = vec![0 as usize];
    let (barrier_tx, barrier_rx) = unbounded_channel::<Barrier>();

    let source_exec = SourceExecutor::new(
        actor_id,
        source_table_id,
        source_desc,
        keyspace.clone(),
        column_ids.clone(),
        schema,
        pk_indices,
        barrier_rx,
        1,
        1,
        "SourceExecutor".to_string(),
        Arc::new(StreamingMetrics::unused()),
        vec![SplitImpl::Kafka(KafkaSplit::new(
            0,
            Some(0),
            None,
            "kafka_3_partition_topic".to_string(),
        ))],
        u64::MAX,
    )?;

    let mut materialize = MaterializeExecutor::new(
        Box::new(source_exec),
        keyspace.clone(),
        vec![OrderPair::new(0, OrderType::Ascending)],
        column_ids.clone(),
        2,
        vec![0usize],
    )
    .boxed()
    .execute();

    let curr_epoch = 1919;
    barrier_tx
        .send(Barrier::new_test_barrier(curr_epoch))
        .unwrap();

    println!("{:?}", materialize.next().await); // barrier
    println!("{:?}", materialize.next().await);

    let change_split_mutation = Barrier::new_test_barrier(curr_epoch + 1).with_mutation(Mutation::SourceChangeSplit(
        hashmap!{
            ActorId::default() => Some(vec![SplitImpl::Kafka(KafkaSplit::new(
                0,
                Some(0),
                None,
                "kafka_3_partition_topic".to_string(),
            )), SplitImpl::Kafka(KafkaSplit::new(1, Some(0), None, "kafka_3_partition_topic".to_string()))])
        }
    ));
    barrier_tx.send(change_split_mutation).unwrap();

    println!("{:?}", materialize.next().await); // barrier
    println!("{:?}", materialize.next().await);
    Ok(())
}
