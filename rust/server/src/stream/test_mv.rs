use pb_construct::make_proto;
use pb_convert::FromProtobuf;
use protobuf::well_known_types::Any as AnyProto;
use risingwave_pb::plan::{DatabaseRefId, SchemaRefId, TableRefId};
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_pb::stream_plan::{
    dispatcher::DispatcherType, Dispatcher, MViewNode, ProjectNode, StreamFragment, StreamNode,
    TableSourceNode,
};
use risingwave_pb::stream_service::{ActorInfo, ActorInfoTable};
use risingwave_pb::task_service::HostAddress;
use risingwave_pb::ToProst;
use risingwave_pb::ToProto;
use risingwave_proto::data::{DataType, DataType_TypeName};
use risingwave_proto::expr::{ExprNode_Type, InputRefExpr};
use risingwave_proto::plan::{ColumnDesc, ColumnDesc_ColumnEncodingType};

use crate::array::column::Column;
use crate::array::{ArrayBuilder, DataChunk, PrimitiveArrayBuilder};
use crate::catalog::TableId;
use crate::error::ErrorCode::InternalError;
use crate::error::RwError;
use crate::storage::SimpleTableRef;
use crate::storage::{Row, SimpleTableManager, Table, TableManager};
use crate::stream::StreamManager;
use crate::stream_op::Message;
use crate::types::{Int32Type, Scalar};
use futures::StreamExt;
use smallvec::SmallVec;
use std::sync::Arc;

fn make_int32_type_proto() -> risingwave_proto::data::DataType {
    return make_proto!(DataType, { type_name: DataType_TypeName::INT32 });
}

fn make_table_ref_id(id: i32) -> TableRefId {
    TableRefId {
        schema_ref_id: Some(SchemaRefId {
            database_ref_id: Some(DatabaseRefId { database_id: 0 }),
            schema_id: 0,
        }),
        table_id: id,
    }
}

#[tokio::test]
async fn test_stream_mv_proto() {
    // Build example proto for a stream executor chain.
    // TableSource -> Project -> Materialized View
    // Select v1 from T(v1,v2).
    let source_proto = StreamNode {
        node: Some(Node::TableSourceNode(TableSourceNode {
            table_ref_id: Some(make_table_ref_id(0)),
            column_ids: vec![0, 1],
        })),
        input: None,
    };
    let expr_proto = make_proto!(risingwave_proto::expr::ExprNode,{
        expr_type: ExprNode_Type::INPUT_REF,
        body: AnyProto::pack(
            &make_proto!(InputRefExpr,{
                column_idx: 0
            })
        ).unwrap(),
        return_type: make_int32_type_proto()
    });
    let column_desc_proto = make_proto!(risingwave_proto::plan::ColumnDesc,{
        column_type: make_int32_type_proto(),
        encoding: ColumnDesc_ColumnEncodingType::RAW,
        name: "v1".to_string()
    });

    let project_proto = StreamNode {
        node: Some(Node::ProjectNode(ProjectNode {
            select_list: vec![expr_proto.to_prost::<risingwave_pb::expr::ExprNode>()],
        })),
        input: Some(Box::new(source_proto)),
    };
    let mview_proto = StreamNode {
        node: Some(Node::MviewNode(MViewNode {
            table_ref_id: Some(make_table_ref_id(1)),
            column_descs: vec![column_desc_proto.to_prost::<risingwave_pb::plan::ColumnDesc>()],
            pk_indices: vec![],
        })),
        input: Some(Box::new(project_proto)),
    };
    let fragment_proto = StreamFragment {
        fragment_id: 1,
        nodes: Some(mview_proto),
        upstream_fragment_id: vec![0],
        dispatcher: Some(Dispatcher {
            r#type: DispatcherType::Simple as i32,
            column_idx: 0,
        }),
        downstream_fragment_id: vec![233],
    };

    // Initialize storage.
    let table_manager = Arc::new(SimpleTableManager::new());
    let table_id = TableId::from_protobuf(
        &make_table_ref_id(0).to_proto::<risingwave_proto::plan::TableRefId>(),
    )
    .expect("Failed to convert table id");
    let column1 = make_proto!(ColumnDesc, {
      column_type: make_proto!(DataType, {
        type_name: DataType_TypeName::INT32
      }),
      encoding: ColumnDesc_ColumnEncodingType::RAW,
      is_primary: false,
      name: "test_col".to_string()
    });
    let column2 = make_proto!(ColumnDesc, {
      column_type: make_proto!(DataType, {
        type_name: DataType_TypeName::INT32
      }),
      encoding: ColumnDesc_ColumnEncodingType::RAW,
      is_primary: false,
      name: "test_col".to_string()
    });
    let columns = vec![column1, column2];
    let _res = table_manager.create_table(&table_id, &columns);
    let table_ref =
        (if let SimpleTableRef::Columnar(table_ref) = table_manager.get_table(&table_id).unwrap() {
            Ok(table_ref)
        } else {
            Err(RwError::from(InternalError(
                "Only columnar table support insert".to_string(),
            )))
        })
        .unwrap();
    // Mock initial data.
    // One row of (1,2)
    let mut array_builder1 = PrimitiveArrayBuilder::<i32>::new(1).unwrap();
    array_builder1.append(Some(1_i32)).unwrap();
    let array1 = array_builder1.finish().unwrap();
    let column1 = Column::new(Arc::new(array1.into()), Int32Type::create(false));
    let mut array_builder2 = PrimitiveArrayBuilder::<i32>::new(1).unwrap();
    array_builder2.append(Some(2_i32)).unwrap();
    let array2 = array_builder2.finish().unwrap();
    let column2 = Column::new(Arc::new(array2.into()), Int32Type::create(false));
    let columns = vec![column1, column2];
    let append_chunk = DataChunk::builder().columns(columns).build();

    // Build stream actor.
    let stream_manager = StreamManager::new();

    let actor_info_proto = ActorInfo {
        fragment_id: 1,
        host: Some(HostAddress {
            host: "127.0.0.1".into(),
            port: 2333,
        }),
    };
    let actor_info_table = ActorInfoTable {
        info: vec![actor_info_proto],
    };
    stream_manager.update_actor_info(actor_info_table).unwrap();
    stream_manager.update_fragment(&[fragment_proto]).unwrap();
    stream_manager
        .build_fragment(&[1], table_manager.clone())
        .unwrap();

    // Insert data and check if the materialized view has been updated.
    let _res_app = table_ref.append(append_chunk);
    let table_id_mv = TableId::from_protobuf(
        &make_table_ref_id(1).to_proto::<risingwave_proto::plan::TableRefId>(),
    )
    .map_err(|e| InternalError(format!("Failed to parse table id: {:?}", e)))
    .unwrap();
    let table_ref_mv = table_manager.get_table(&table_id_mv).unwrap();

    let mut sink = stream_manager.take_sink(233);
    if let Message::Chunk(_chunk) = sink.next().await.unwrap() {
        if let SimpleTableRef::Row(table_mv) = table_ref_mv {
            let mut value_vec = SmallVec::new();
            value_vec.push(Some(1.to_scalar_value()));
            let value_row = Row(value_vec);
            let res_row = table_mv.get(value_row);
            if let Ok(res_row_in) = res_row {
                let datum = res_row_in.unwrap().0.get(0).unwrap().clone();
                let d_value = datum.unwrap().as_int32() + 1;
                assert_eq!(d_value, 2);
            } else {
                unreachable!();
            }
        } else {
            unreachable!();
        }
    } else {
        unreachable!();
    }
}
