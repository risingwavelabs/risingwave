package com.risingwave.execution.handler;

import com.risingwave.catalog.TableCatalog;
import com.risingwave.common.config.FrontendServerConfigurations;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.execution.context.ExecutionContext;
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
import com.risingwave.proto.plan.DatabaseRefId;
import com.risingwave.proto.plan.PlanFragment;
import com.risingwave.proto.plan.SchemaRefId;
import com.risingwave.proto.plan.TableRefId;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.util.Iterator;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryHandler.class);
  private Channel channel;

  public RpcHelper(ExecutionContext context) {
    // Prepare channel. FIXME: No TLS Support Yet.
    channel =
        ManagedChannelBuilder.forAddress(
                "localhost",
                context.getConf().get(FrontendServerConfigurations.COMPUTE_NODE_SERVER_PORT))
            .usePlaintext()
            .build();
  }

  public CreateTaskResponse creatTask(BatchPlan plan, TaskId taskId) {
    // Prepare Task service stub.
    TaskServiceGrpc.TaskServiceBlockingStub blockingTaskStub =
        TaskServiceGrpc.newBlockingStub(channel);

    // Invoke Create Task RPC.
    CreateTaskRequest request =
        CreateTaskRequest.newBuilder().setTaskId(taskId).setPlan(plan.serialize()).build();
    CreateTaskResponse response;
    try {
      response = blockingTaskStub.create(request);
    } catch (StatusRuntimeException e) {
      LOGGER.atWarn().log("RPC failed: {0}", e.getStatus());
      throw new PgException(PgErrorCode.INTERNAL_ERROR, "Create task RPC failed");
    }
    return response;
  }

  public CreateTaskResponse createTaskFromFragment(PlanFragment planFragment, TaskId taskId) {
    // Prepare Task service stub.
    TaskServiceGrpc.TaskServiceBlockingStub blockingTaskStub =
        TaskServiceGrpc.newBlockingStub(channel);

    // Invoke Create Task RPC.
    CreateTaskRequest request =
        CreateTaskRequest.newBuilder().setTaskId(taskId).setPlan(planFragment).build();
    CreateTaskResponse response;
    try {
      response = blockingTaskStub.create(request);
    } catch (StatusRuntimeException e) {
      LOGGER.atWarn().log("RPC failed: {0}", e.getStatus());
      throw new PgException(PgErrorCode.INTERNAL_ERROR, "Create task RPC failed");
    }
    return response;
  }

  public Iterator<TaskData> getData(TaskId taskId) {
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
    return taskDataIterator;
  }

  public TaskId buildTaskId() {
    // TODO: Set StageId etc.
    return TaskId.newBuilder()
        .setStageId(
            StageId.newBuilder()
                .setQueryId(QueryId.newBuilder().setTraceId(UUID.randomUUID().toString())))
        .build();
  }

  private TaskSinkId buildTaskSinkId(TaskId taskId) {
    // TODO: Set SinkId.
    return TaskSinkId.newBuilder().setTaskId(taskId).build();
  }

  /**
   * Utility function to create a table ref id from table id.
   *
   * @param tableId table id
   * @return table ref id
   */
  public static TableRefId getTableRefId(TableCatalog.TableId tableId) {
    return TableRefId.newBuilder()
        .setTableId(tableId.getValue())
        .setSchemaRefId(
            SchemaRefId.newBuilder()
                .setSchemaId(tableId.getParent().getValue())
                .setDatabaseRefId(
                    DatabaseRefId.newBuilder()
                        .setDatabaseId(tableId.getParent().getParent().getValue())))
        .build();
  }
}
