use pgwire::pg_response::PgResponse;
use risingwave_common::error::Result;
use risingwave_pb::plan::{QueryId, StageId, TaskId, TaskSinkId};
use risingwave_rpc_client::{ComputeClient, ExchangeSource};
use risingwave_sqlparser::ast::{Query, Statement};
use uuid::Uuid;

use crate::binder::Binder;
use crate::planner::Planner;
use crate::scheduler::schedule::WorkerNodeManager;
use crate::session::SessionImpl;

pub async fn handle_query(session: &SessionImpl, query: Box<Query>) -> Result<PgResponse> {
    let catalog = session
        .env()
        .catalog_mgr()
        .get_database_snapshot(session.database())
        .unwrap();
    let mut binder = Binder::new(catalog);
    let bound = binder.bind(Statement::Query(query))?;
    let plan = Planner::new().plan(bound)?;
    let batch = plan.gen_batch_query_plan();

    // let fragmented_query = BatchPlanFragmenter::new().split(batch)?;

    // For now, we simply choose the first worker node to execute the entire plan.
    // We will let BatchScheduler do this job when it's fully completed.
    let worker_node_manager = WorkerNodeManager::new(session.env().meta_client().clone()).await?;
    let nodes = worker_node_manager.list_worker_nodes();
    let node = nodes.get(0).unwrap();
    let addr = node.get_host().unwrap().to_socket_addr()?;

    let task_id = TaskId {
        stage_id: Some(StageId {
            query_id: Some(QueryId {
                trace_id: Uuid::new_v4().to_string(),
            }),
            stage_id: 0,
        }),
        task_id: 0,
    };
    let client = ComputeClient::new(&addr).await?;
    client
        .create_task(task_id.clone(), batch.to_batch_prost())
        .await?;
    let mut source = client
        .get_data(TaskSinkId {
            task_id: Some(task_id),
            sink_id: 0,
        })
        .await?;
    let mut resps = vec![];
    loop {
        match source.take_data().await? {
            None => break,
            Some(chunk) => todo!(),
        }
    }
    Ok(resps)
}
