package com.risingwave.execution.handler;

import com.risingwave.common.config.FrontendServerConfigurations;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.execution.result.BatchDataChunkResult;
import com.risingwave.pgwire.database.PgResult;
import com.risingwave.pgwire.msg.StatementType;
import com.risingwave.planner.planner.batch.BatchPlanner;
import com.risingwave.planner.rel.physical.batch.BatchPlan;
import com.risingwave.proto.computenode.CreateTaskRequest;
import com.risingwave.proto.computenode.CreateTaskResponse;
import com.risingwave.proto.computenode.ExchangeServiceGrpc;
import com.risingwave.proto.computenode.QueryId;
import com.risingwave.proto.computenode.StageId;
import com.risingwave.proto.computenode.TaskData;
import com.risingwave.proto.computenode.TaskId;
import com.risingwave.proto.computenode.TaskServiceGrpc;
import com.risingwave.proto.computenode.TaskSinkId;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.UUID;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@HandlerSignature(sqlKinds = {SqlKind.SELECT})
public class QueryHandler implements SqlHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryHandler.class);

  @Override
  public PgResult handle(SqlNode ast, ExecutionContext context) {
    BatchPlanner planner = new BatchPlanner();
    BatchPlan plan = planner.plan(ast, context);

    // Prepare channel. FIXME: No TLS Support Yet.
    Channel channel =
        ManagedChannelBuilder.forAddress(
                "localhost",
                context.getConf().get(FrontendServerConfigurations.COMPUTE_NODE_SERVER_PORT))
            .usePlaintext()
            .build();

    // Prepare Task service stub.
    TaskServiceGrpc.TaskServiceBlockingStub blockingTaskStub =
        TaskServiceGrpc.newBlockingStub(channel);

    // Invoke Create Task RPC.
    TaskId taskId = buildTaskId();
    CreateTaskRequest request =
        CreateTaskRequest.newBuilder().setTaskId(taskId).setPlan(plan.serialize()).build();
    CreateTaskResponse response;
    try {
      response = blockingTaskStub.create(request);
    } catch (StatusRuntimeException e) {
      LOGGER.atWarn().log("RPC failed: {0}", e.getStatus());
      throw new PgException(PgErrorCode.INTERNAL_ERROR, "Create task RPC failed");
    }

    // Prepare Exchange service stub.
    ExchangeServiceGrpc.ExchangeServiceBlockingStub blockingExchangeStub =
        ExchangeServiceGrpc.newBlockingStub(channel);

    // Invoke GetData RPC.
    TaskSinkId taskSinkId = buildTaskSinkId(taskId);
    Iterator<TaskData> taskDataIterator;
    try {
      taskDataIterator = blockingExchangeStub.getData(taskSinkId);
    } catch (StatusRuntimeException e) {
      LOGGER.atWarn().log("RPC failed: {0}", e.getStatus());
      throw new PgException(PgErrorCode.INTERNAL_ERROR, "Get data RPC failed");
    }

    // Convert to List so that iterate multiple times.
    // FIXME: use Iterator<TaskData>
    ArrayList<TaskData> taskDataList = new ArrayList<TaskData>();
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

  private static TaskId buildTaskId() {
    // TODO: Set StageId etc.
    return TaskId.newBuilder()
        .setStageId(
            StageId.newBuilder()
                .setQueryId(QueryId.newBuilder().setTraceId(UUID.randomUUID().toString())))
        .build();
  }

  private static TaskSinkId buildTaskSinkId(TaskId taskId) {
    // TODO: Set SinkId.
    return TaskSinkId.newBuilder().setTaskId(taskId).build();
  }
}
