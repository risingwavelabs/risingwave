package com.risingwave.execution.handler;

import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.execution.result.BatchDataChunkResult;
import com.risingwave.pgwire.database.PgResult;
import com.risingwave.pgwire.msg.StatementType;
import com.risingwave.planner.planner.batch.BatchPlanner;
import com.risingwave.planner.rel.physical.batch.BatchPlan;
import com.risingwave.proto.common.Status;
import com.risingwave.proto.computenode.CreateTaskRequest;
import com.risingwave.proto.computenode.CreateTaskResponse;
import com.risingwave.proto.computenode.TaskData;
import com.risingwave.proto.computenode.TaskSinkId;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

@HandlerSignature(sqlKinds = {SqlKind.SELECT})
public class QueryHandler implements SqlHandler {

  @Override
  public PgResult handle(SqlNode ast, ExecutionContext context) {
    BatchPlanner planner = new BatchPlanner();
    BatchPlan plan = planner.plan(ast, context);

    RpcExecutor rpcExecutor = context.getRpcExecutor();
    CreateTaskRequest createTaskRequest = rpcExecutor.buildCreateTaskRequest(plan.serialize());
    CreateTaskResponse createTaskResponse = rpcExecutor.createTask(createTaskRequest);
    if (createTaskResponse.getStatus().getCode() != Status.Code.OK) {
      throw new PgException(PgErrorCode.INTERNAL_ERROR, "Creat Task failed");
    }
    TaskSinkId taskSinkId = rpcExecutor.buildTaskSinkId(createTaskRequest.getTaskId());
    Iterator<TaskData> taskDataIterator = rpcExecutor.getData(taskSinkId);

    // Convert task data to list to iterate it multiple times.
    // FIXME: use Iterator<TaskData>
    ArrayList<TaskData> taskDataList = new ArrayList();
    while (taskDataIterator.hasNext()) {
      taskDataList.add(taskDataIterator.next());
    }
    return new BatchDataChunkResult(
        getStatementType(ast), false, taskDataList, plan.getRoot().getRowType());
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
