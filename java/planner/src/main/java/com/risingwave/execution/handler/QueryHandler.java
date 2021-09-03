package com.risingwave.execution.handler;

import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.execution.result.BatchDataChunkResult;
import com.risingwave.pgwire.database.PgResult;
import com.risingwave.pgwire.msg.StatementType;
import com.risingwave.planner.planner.batch.BatchPlanner;
import com.risingwave.planner.rel.physical.batch.BatchPlan;
import com.risingwave.proto.computenode.TaskData;
import com.risingwave.proto.computenode.TaskSinkId;
import com.risingwave.rpc.ComputeClient;
import com.risingwave.rpc.Messages;
import com.risingwave.scheduler.QueryManager;
import com.risingwave.scheduler.QueryResultLocation;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

@HandlerSignature(sqlKinds = {SqlKind.SELECT, SqlKind.INSERT})
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
    Iterator<TaskData> taskDataIterator = client.getData(taskSinkId);

    // Convert task data to list to iterate it multiple times.
    // FIXME: use Iterator<TaskData>
    ArrayList<TaskData> taskDataList = new ArrayList();
    while (taskDataIterator.hasNext()) {
      taskDataList.add(taskDataIterator.next());
    }
    return new BatchDataChunkResult(
        getStatementType(ast), true, taskDataList, plan.getRoot().getRowType());
  }

  // Helpers.
  private static StatementType getStatementType(SqlNode ast) {
    switch (ast.getKind()) {
      case INSERT:
        return StatementType.INSERT;
      case DELETE:
        return StatementType.DELETE;
      case UPDATE:
        return StatementType.UPDATE;
      case SELECT:
        return StatementType.SELECT;
      case OTHER:
        return StatementType.OTHER;
      default:
        throw new UnsupportedOperationException("Unsupported statement type");
    }
  }
}
