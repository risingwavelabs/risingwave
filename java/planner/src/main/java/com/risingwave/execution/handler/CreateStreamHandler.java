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
import com.risingwave.proto.computenode.GetDataRequest;
import com.risingwave.proto.computenode.TaskSinkId;
import com.risingwave.proto.plan.CreateStreamNode;
import com.risingwave.proto.plan.ExchangeInfo;
import com.risingwave.proto.plan.PlanFragment;
import com.risingwave.proto.plan.PlanNode;
import com.risingwave.rpc.ComputeClient;
import com.risingwave.rpc.ComputeClientManager;
import com.risingwave.rpc.Messages;
import com.risingwave.sql.node.SqlCreateStream;
import com.risingwave.sql.node.SqlTableOption;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.validate.SqlValidator;

/** Handler of <code>CREATE STREAM</code> statement */
public class CreateStreamHandler implements SqlHandler {
  @Override
  public DdlResult handle(SqlNode ast, ExecutionContext context) {
    PlanFragment planFragment = executeDdl(ast, context);
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

    return new DdlResult(StatementType.CREATE_STREAM, 0);
  }

  private static PlanFragment ddlSerializer(TableCatalog table) {
    TableCatalog.TableId tableId = table.getId();
    CreateStreamNode.Builder createStreamNodeBuilder = CreateStreamNode.newBuilder();
    for (ColumnCatalog columnCatalog : table.getAllColumnCatalogs()) {
      com.risingwave.proto.plan.ColumnDesc.Builder columnDescBuilder =
          com.risingwave.proto.plan.ColumnDesc.newBuilder();

      columnDescBuilder
          .setName(columnCatalog.getName())
          .setEncoding(com.risingwave.proto.plan.ColumnDesc.ColumnEncodingType.RAW)
          .setColumnType(columnCatalog.getDesc().getDataType().getProtobufType())
          .setIsPrimary(false);

      createStreamNodeBuilder.addColumnDescs(columnDescBuilder);
    }

    createStreamNodeBuilder.putAllProperties(table.getProperties());

    switch (table.getRowFormat().toLowerCase()) {
      case "json":
        createStreamNodeBuilder.setFormat(CreateStreamNode.RowFormatType.JSON);
        break;
      case "avro":
        createStreamNodeBuilder.setFormat(CreateStreamNode.RowFormatType.AVRO);
        break;
      case "protobuf":
        createStreamNodeBuilder.setFormat(CreateStreamNode.RowFormatType.PROTOBUF);
        break;
      default:
        throw new PgException(PgErrorCode.PROTOCOL_VIOLATION, "unsupported row format");
    }

    CreateStreamNode creatStreamNode =
        createStreamNodeBuilder.setTableRefId(Messages.getTableRefId(tableId)).build();

    ExchangeInfo exchangeInfo =
        ExchangeInfo.newBuilder().setMode(ExchangeInfo.DistributionMode.SINGLE).build();

    PlanNode rootNode =
        PlanNode.newBuilder()
            .setBody(Any.pack(creatStreamNode))
            .setNodeType(PlanNode.PlanNodeType.CREATE_STREAM)
            .build();

    return PlanFragment.newBuilder().setRoot(rootNode).setExchangeInfo(exchangeInfo).build();
  }

  @VisibleForTesting
  protected PlanFragment executeDdl(SqlNode ast, ExecutionContext context) {
    SqlCreateStream sql = (SqlCreateStream) ast;

    SchemaCatalog.SchemaName schemaName = context.getCurrentSchema();

    String tableName = sql.getName().getSimple();
    CreateTableInfo.Builder createStreamInfoBuilder = CreateTableInfo.builder(tableName);

    if (sql.getColumnList() != null) {
      SqlValidator sqlConverter = SqlConverter.builder(context).build().getValidator();

      for (SqlNode column : sql.getColumnList()) {
        SqlColumnDeclaration columnDef = (SqlColumnDeclaration) column;

        ColumnDesc columnDesc =
            new ColumnDesc(
                (RisingWaveDataType) columnDef.dataType.deriveType(sqlConverter),
                false,
                ColumnEncoding.RAW);
        createStreamInfoBuilder.addColumn(columnDef.name.getSimple(), columnDesc);
      }
    }

    Map<String, String> properties =
        sql.getPropertyList().stream()
            .map(n -> (SqlTableOption) n)
            .collect(
                Collectors.toMap(
                    n -> ((SqlCharStringLiteral) n.getKey()).getValueAs(String.class),
                    n -> ((SqlCharStringLiteral) n.getValue()).getValueAs(String.class)));

    createStreamInfoBuilder.setProperties(properties);
    createStreamInfoBuilder.setStream(true);
    createStreamInfoBuilder.setRowFormat(sql.getRowFormat().getValueAs(String.class));

    CreateTableInfo streamInfo = createStreamInfoBuilder.build();
    // Build a plan distribute to compute node.
    TableCatalog table = context.getCatalogService().createTable(schemaName, streamInfo);
    return ddlSerializer(table);
  }
}
