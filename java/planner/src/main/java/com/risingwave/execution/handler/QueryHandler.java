package com.risingwave.execution.handler;

import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.execution.result.BatchDataChunkResult;
import com.risingwave.pgwire.database.PgResult;
import com.risingwave.planner.planner.batch.BatchPlanner;
import com.risingwave.planner.rel.physical.batch.BatchPlan;
import com.risingwave.proto.computenode.GetDataResponse;
import com.risingwave.proto.computenode.TaskSinkId;
import com.risingwave.rpc.ComputeClient;
import com.risingwave.rpc.Messages;
import com.risingwave.scheduler.QueryManager;
import com.risingwave.scheduler.QueryResultLocation;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

/** Handler of user queries. */
@HandlerSignature(sqlKinds = {SqlKind.SELECT, SqlKind.INSERT, SqlKind.ORDER_BY})
public class QueryHandler implements SqlHandler {

  @Override
  public PgResult handle(SqlNode ast, ExecutionContext context) {
    BatchPlanner planner = new BatchPlanner();
    BatchPlan plan = planner.plan(ast, context);

    QueryResultLocation resultLocation;
    try {
      QueryManager queryManager = context.getQueryManager();
      resultLocation = queryManager.schedule(plan).get();
    } catch (Exception exp) {
      throw new RuntimeException(exp);
    }

    TaskSinkId taskSinkId = Messages.buildTaskSinkId(resultLocation.getTaskId().toTaskIdProto());
    ComputeClient client = context.getComputeClientManager().getOrCreate(resultLocation.getNode());
    Iterator<GetDataResponse> taskDataIterator = client.getData(taskSinkId);

    // Convert task data to list to iterate it multiple times.
    // FIXME: use Iterator<TaskData>
    ArrayList<GetDataResponse> responses = new ArrayList();
    while (taskDataIterator.hasNext()) {
      responses.add(taskDataIterator.next());
    }
    return new BatchDataChunkResult(
        SqlHandler.getStatementType(ast), responses, plan.getRoot().getRowType());
  }
}
