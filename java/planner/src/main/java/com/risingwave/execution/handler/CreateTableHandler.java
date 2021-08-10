package com.risingwave.execution.handler;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Any;
import com.risingwave.catalog.ColumnCatalog;
import com.risingwave.catalog.ColumnDesc;
import com.risingwave.catalog.ColumnEncoding;
import com.risingwave.catalog.CreateTableInfo;
import com.risingwave.catalog.SchemaCatalog;
import com.risingwave.catalog.TableCatalog;
import com.risingwave.common.datatype.RisingWaveDataType;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.execution.result.DdlResult;
import com.risingwave.pgwire.msg.StatementType;
import com.risingwave.planner.sql.SqlConverter;
import com.risingwave.proto.common.Status;
import com.risingwave.proto.computenode.CreateTaskRequest;
import com.risingwave.proto.computenode.CreateTaskResponse;
import com.risingwave.proto.computenode.TaskSinkId;
import com.risingwave.proto.plan.CreateTableNode;
import com.risingwave.proto.plan.PlanFragment;
import com.risingwave.proto.plan.PlanNode;
import com.risingwave.proto.plan.ShuffleInfo;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.validate.SqlValidator;

@HandlerSignature(sqlKinds = {SqlKind.CREATE_TABLE})
public class CreateTableHandler implements SqlHandler {
  @Override
  public DdlResult handle(SqlNode ast, ExecutionContext context) {
    PlanFragment planFragment = executeDdl(ast, context);
    RpcExecutor rpcExecutor = context.getRpcExecutor();
    CreateTaskRequest createTaskRequest = rpcExecutor.buildCreateTaskRequest(planFragment);
    CreateTaskResponse createTaskResponse = rpcExecutor.createTask(createTaskRequest);

    TaskSinkId taskSinkId = rpcExecutor.buildTaskSinkId(createTaskRequest.getTaskId());
    rpcExecutor.getData(taskSinkId);

    if (createTaskResponse.getStatus().getCode() != Status.Code.OK) {
      throw new PgException(PgErrorCode.INTERNAL_ERROR, "Create Task failed");
    }

    return new DdlResult(StatementType.CREATE_TABLE, 0);
  }

  private static PlanFragment ddlSerializer(TableCatalog table) {
    TableCatalog.TableId tableId = table.getId();
    CreateTableNode.Builder createTableNodeBuilder = CreateTableNode.newBuilder();
    for (ColumnCatalog columnCatalog : table.getAllColumnCatalogs()) {
      com.risingwave.proto.plan.ColumnDesc.Builder columnDescBuilder =
          com.risingwave.proto.plan.ColumnDesc.newBuilder();
      columnDescBuilder
          .setEncoding(com.risingwave.proto.plan.ColumnDesc.ColumnEncodingType.RAW)
          .setColumnType(columnCatalog.getDesc().getDataType().getProtobufType())
          .setIsPrimary(false);
      createTableNodeBuilder.addColumnDescs(columnDescBuilder);
    }
    CreateTableNode creatTableNode =
        createTableNodeBuilder.setTableRefId(RpcExecutor.getTableRefId(tableId)).build();

    ShuffleInfo shuffleInfo =
        ShuffleInfo.newBuilder().setPartitionMode(ShuffleInfo.PartitionMode.SINGLE).build();

    PlanNode rootNode =
        PlanNode.newBuilder()
            .setBody(Any.pack(creatTableNode))
            .setNodeType(PlanNode.PlanNodeType.CREATE_TABLE)
            .build();

    return PlanFragment.newBuilder().setRoot(rootNode).setShuffleInfo(shuffleInfo).build();
  }

  @VisibleForTesting
  protected PlanFragment executeDdl(SqlNode ast, ExecutionContext context) {
    SqlCreateTable sql = (SqlCreateTable) ast;

    SchemaCatalog.SchemaName schemaName = context.getCurrentSchema();

    String tableName = sql.name.getSimple();
    CreateTableInfo.Builder createTableInfoBuilder = CreateTableInfo.builder(tableName);
    if (sql.columnList != null) {
      SqlValidator sqlConverter = SqlConverter.builder(context).build().getValidator();
      for (SqlNode column : sql.columnList) {
        SqlColumnDeclaration columnDef = (SqlColumnDeclaration) column;

        ColumnDesc columnDesc =
            new ColumnDesc(
                (RisingWaveDataType) columnDef.dataType.deriveType(sqlConverter),
                false,
                ColumnEncoding.RAW);
        createTableInfoBuilder.addColumn(columnDef.name.getSimple(), columnDesc);
      }
    }
    CreateTableInfo tableInfo = createTableInfoBuilder.build();
    // Build a plan distribute to compute node.
    TableCatalog table = context.getCatalogService().createTable(schemaName, tableInfo);
    return ddlSerializer(table);
  }
}
