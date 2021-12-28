package com.risingwave.execution.handler;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Any;
import com.risingwave.catalog.TableCatalog;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.execution.result.DdlResult;
import com.risingwave.pgwire.database.PgResult;
import com.risingwave.pgwire.msg.StatementType;
import com.risingwave.proto.common.Status;
import com.risingwave.proto.computenode.CreateTaskRequest;
import com.risingwave.proto.computenode.CreateTaskResponse;
import com.risingwave.proto.computenode.GetDataRequest;
import com.risingwave.proto.computenode.TaskSinkId;
import com.risingwave.proto.plan.DropTableNode;
import com.risingwave.proto.plan.ExchangeInfo;
import com.risingwave.proto.plan.PlanFragment;
import com.risingwave.proto.plan.PlanNode;
import com.risingwave.rpc.ComputeClient;
import com.risingwave.rpc.ComputeClientManager;
import com.risingwave.rpc.Messages;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlDropTable;

/**
 * DropTableHandler is the handler of both DropTable and DropSource. It determines the type by
 * `isSource`.
 */
@HandlerSignature(sqlKinds = {SqlKind.DROP_TABLE})
public class DropTableHandler implements SqlHandler {
  @Override
  public PgResult handle(SqlNode ast, ExecutionContext context) {
    PlanFragment planFragment = execute(ast, context);
    if (planFragment == null) {
      // No plan is generated (i.e. the table does not exist). 
      // Just return success.
      return new DdlResult(StatementType.DROP_TABLE, 0);
    }
    ComputeClientManager clientManager = context.getComputeClientManager();

    for (var node : context.getWorkerNodeManager().allNodes()) {
      ComputeClient client = clientManager.getOrCreate(node);
      CreateTaskRequest createTaskRequest = Messages.buildCreateTaskRequest(planFragment);
      CreateTaskResponse createTaskResponse = client.createTask(createTaskRequest);
      if (createTaskResponse.getStatus().getCode() != Status.Code.OK) {
        throw new PgException(PgErrorCode.INTERNAL_ERROR, "Create Task failed");
      }
      TaskSinkId taskSinkId = Messages.buildTaskSinkId(createTaskRequest.getTaskId());
      client.getData(GetDataRequest.newBuilder().setSinkId(taskSinkId).build());
    }

    return new DdlResult(StatementType.DROP_TABLE, 0);
  }

  private static PlanFragment serialize(TableCatalog table) {
    TableCatalog.TableId tableId = table.getId();
    DropTableNode dropTableNode =
        DropTableNode.newBuilder().setTableRefId(Messages.getTableRefId(tableId)).build();
    ExchangeInfo exchangeInfo =
        ExchangeInfo.newBuilder().setMode(ExchangeInfo.DistributionMode.SINGLE).build();

    PlanNode.PlanNodeType planNodeType = PlanNode.PlanNodeType.DROP_TABLE;

    if (table.isSource()) {
      planNodeType = PlanNode.PlanNodeType.DROP_SOURCE;
    }
    PlanNode rootNode =
        PlanNode.newBuilder().setBody(Any.pack(dropTableNode)).setNodeType(planNodeType).build();

    return PlanFragment.newBuilder().setRoot(rootNode).setExchangeInfo(exchangeInfo).build();
  }

  @VisibleForTesting
  protected PlanFragment execute(SqlNode ast, ExecutionContext context) {
    SqlDropTable sql = (SqlDropTable) ast;
    TableCatalog.TableName tableName =
        TableCatalog.TableName.of(context.getDatabase(), context.getSchema(), sql.name.getSimple());
    TableCatalog table = context.getCatalogService().getTable(tableName);
    if (table == null) {
      if (!sql.ifExists) {
        throw new PgException(PgErrorCode.UNDEFINED_TABLE, "Table does not eixst");
      }
      return null;
    }
    context.getCatalogService().dropTable(tableName);
    return serialize(table);
  }
}
