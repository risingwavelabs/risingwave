package com.risingwave.execution.handler;

import com.risingwave.catalog.TableCatalog;
import com.risingwave.proto.computenode.CreateTaskRequest;
import com.risingwave.proto.computenode.CreateTaskResponse;
import com.risingwave.proto.computenode.QueryId;
import com.risingwave.proto.computenode.StageId;
import com.risingwave.proto.computenode.TaskData;
import com.risingwave.proto.computenode.TaskId;
import com.risingwave.proto.computenode.TaskSinkId;
import com.risingwave.proto.plan.DatabaseRefId;
import com.risingwave.proto.plan.PlanFragment;
import com.risingwave.proto.plan.SchemaRefId;
import com.risingwave.proto.plan.TableRefId;
import java.util.Iterator;
import java.util.UUID;

public interface RpcExecutor {

  CreateTaskResponse createTask(CreateTaskRequest createTaskRequest);

  Iterator<TaskData> getData(TaskSinkId taskId);

  default CreateTaskRequest buildCreateTaskRequest(PlanFragment planFragment) {
    TaskId taskId =
        TaskId.newBuilder()
            .setStageId(
                StageId.newBuilder()
                    .setQueryId(QueryId.newBuilder().setTraceId(UUID.randomUUID().toString())))
            .build();
    CreateTaskRequest createTaskRequest =
        CreateTaskRequest.newBuilder().setTaskId(taskId).setPlan(planFragment).build();
    return createTaskRequest;
  }

  default TaskSinkId buildTaskSinkId(TaskId taskId) {
    // TODO: Set SinkId.
    return TaskSinkId.newBuilder().setTaskId(taskId).build();
  }

  /**
   * Utility function to create a table ref id from table id.
   *
   * @param tableId table id
   * @return table ref id
   */
  static TableRefId getTableRefId(TableCatalog.TableId tableId) {
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
