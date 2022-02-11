package com.risingwave.execution.handler;

import com.google.common.annotations.VisibleForTesting;
import com.risingwave.catalog.ColumnDesc;
import com.risingwave.catalog.ColumnEncoding;
import com.risingwave.catalog.CreateMaterializedViewInfo;
import com.risingwave.catalog.CreateTableInfo;
import com.risingwave.catalog.MaterializedViewCatalog;
import com.risingwave.catalog.SchemaCatalog;
import com.risingwave.catalog.TableCatalog;
import com.risingwave.common.datatype.RisingWaveDataType;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.execution.handler.serializer.TableNodeSerializer;
import com.risingwave.execution.result.DdlResult;
import com.risingwave.pgwire.msg.StatementType;
import com.risingwave.planner.planner.streaming.StreamPlanner;
import com.risingwave.planner.rel.serialization.StreamingPlanSerializer;
import com.risingwave.planner.rel.streaming.RwStreamMaterializedView;
import com.risingwave.planner.rel.streaming.StreamingPlan;
import com.risingwave.planner.sql.SqlConverter;
import com.risingwave.proto.common.Status;
import com.risingwave.proto.computenode.CreateTaskRequest;
import com.risingwave.proto.computenode.CreateTaskResponse;
import com.risingwave.proto.computenode.GetDataRequest;
import com.risingwave.proto.plan.PlanFragment;
import com.risingwave.proto.plan.TableRefId;
import com.risingwave.proto.plan.TaskSinkId;
import com.risingwave.proto.streaming.plan.StreamNode;
import com.risingwave.rpc.ComputeClient;
import com.risingwave.rpc.ComputeClientManager;
import com.risingwave.rpc.Messages;
import com.risingwave.scheduler.streaming.StreamManager;
import com.risingwave.sql.parser.SqlParser;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateMaterializedView;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handler of <code>CREATE TABLE</code> or <code>CREATE TABLE_V2</code> statement */
@HandlerSignature(sqlKinds = {SqlKind.CREATE_TABLE})
public class CreateTableHandler implements SqlHandler {
  private static final Logger log = LoggerFactory.getLogger(CreateTableHandler.class);
  private TableCatalog.TableId tableId;
  private String name;
  private String sourceName;

  @Override
  public DdlResult handle(SqlNode ast, ExecutionContext context) {
    handleCreateTable(ast, context);

    var createMvSql =
        String.format("CREATE MATERIALIZED VIEW \"%s\" AS SELECT * FROM \"%s\"", name, sourceName);
    var createMv = SqlParser.createCalciteStatement(createMvSql);
    handleCreateMv(createMv, context);

    return new DdlResult(StatementType.CREATE_TABLE, 0);
  }

  // Create Materialize View
  public DdlResult handleCreateMv(SqlNode ast, ExecutionContext context) {
    SqlCreateMaterializedView createMaterializedView = (SqlCreateMaterializedView) ast;
    String tableName = createMaterializedView.name.getSimple();

    // Generate a streaming plan representing the (distributed) dataflow for MV construction.
    // The planner decides whether the dataflow is in distributed mode by checking the cluster
    // configuration.
    StreamPlanner planner = new StreamPlanner();
    StreamingPlan plan = planner.plan(ast, context);

    // Check whether the naming for columns in MV is valid.
    boolean isAllAliased = isAllAliased(plan.getStreamingPlan());
    if (!isAllAliased) {
      throw new PgException(
          PgErrorCode.INVALID_COLUMN_DEFINITION,
          "An alias name must be specified for an aggregation function");
    }

    // Register the view on catalog.
    TableCatalog catalog = convertPlanToCatalog(tableName, plan, context);

    // Bind stream plan with materialized view catalog.
    var streamingPlan = plan.getStreamingPlan();
    streamingPlan.setTableId(catalog.getId());
    streamingPlan.setAssociatedTableId(tableId); // associated streaming task to the table

    // Send the create MV requests to compute nodes.
    PlanFragment planFragment =
        TableNodeSerializer.createProtoFromCatalog(catalog, false, streamingPlan);
    ComputeClientManager clientManager = context.getComputeClientManager();
    for (var node : context.getWorkerNodeManager().allNodes()) {
      ComputeClient client = clientManager.getOrCreate(node);
      CreateTaskRequest createTaskRequest = Messages.buildCreateTaskRequest(planFragment);
      log.info("Send request to:" + node.getRpcEndPoint().toString());
      log.info("Create task request:\n" + Messages.jsonFormat(createTaskRequest));
      CreateTaskResponse createTaskResponse = client.createTask(createTaskRequest);
      if (createTaskResponse.getStatus().getCode() != Status.Code.OK) {
        throw new PgException(PgErrorCode.INTERNAL_ERROR, "Create Task failed");
      }
      TaskSinkId taskSinkId = Messages.buildTaskSinkId(createTaskRequest.getTaskId());
      client.getData(GetDataRequest.newBuilder().setSinkId(taskSinkId).build());
    }

    // Send the create MV request meta.
    StreamManager streamManager = context.getStreamManager();
    StreamNode streamNode = StreamingPlanSerializer.serialize(plan.getStreamingPlan());
    log.debug("stream node ser:\n" + Messages.jsonFormat(streamNode));
    TableRefId tableRefId = Messages.getTableRefId(catalog.getId());
    streamManager.createMaterializedView(streamNode, tableRefId);
    return new DdlResult(StatementType.CREATE_MATERIALIZED_VIEW, 0);
  }

  @VisibleForTesting
  public MaterializedViewCatalog convertPlanToCatalog(
      String tableName, StreamingPlan plan, ExecutionContext context) {
    SchemaCatalog.SchemaName schemaName = context.getCurrentSchema();

    CreateMaterializedViewInfo.Builder builder = CreateMaterializedViewInfo.builder(tableName);
    RwStreamMaterializedView rootNode = plan.getStreamingPlan();
    var columns = rootNode.getColumns();
    for (var column : columns) {
      builder.addColumn(column.getKey(), column.getValue());
    }
    builder.setCollation(rootNode.getCollation());
    builder.setMv(true);
    builder.setAssociated(true);
    CreateMaterializedViewInfo mvInfo = builder.build();
    MaterializedViewCatalog viewCatalog =
        context.getCatalogService().createMaterializedView(schemaName, mvInfo);
    rootNode.setTableId(viewCatalog.getId());
    return viewCatalog;
  }

  @VisibleForTesting
  public boolean isAllAliased(RwStreamMaterializedView root) {
    // Trick for checking whether is there any un-aliased aggregations: check the name pattern of
    // columns. Un-aliased column is named as EXPR$1 etc.
    var columns = root.getColumns();
    for (var pair : columns) {
      if (pair.left.startsWith("EXPR$")) {
        return false;
      }
    }
    return true;
  }

  // Create Table

  public DdlResult handleCreateTable(SqlNode ast, ExecutionContext context) {
    PlanFragment planFragment = execute(ast, context);
    ComputeClientManager clientManager = context.getComputeClientManager();

    for (var node : context.getWorkerNodeManager().allNodes()) {
      ComputeClient client = clientManager.getOrCreate(node);
      CreateTaskRequest createTaskRequest = Messages.buildCreateTaskRequest(planFragment);
      log.info("Send request to:" + node.getRpcEndPoint().toString());
      log.info("Create task request:\n" + Messages.jsonFormat(createTaskRequest));
      CreateTaskResponse createTaskResponse = client.createTask(createTaskRequest);
      if (createTaskResponse.getStatus().getCode() != Status.Code.OK) {
        throw new PgException(PgErrorCode.INTERNAL_ERROR, "Create Task failed");
      }
      TaskSinkId taskSinkId = Messages.buildTaskSinkId(createTaskRequest.getTaskId());
      client.getData(GetDataRequest.newBuilder().setSinkId(taskSinkId).build());
    }

    return new DdlResult(StatementType.CREATE_TABLE, 0);
  }

  @VisibleForTesting
  protected PlanFragment execute(SqlNode ast, ExecutionContext context) {
    SqlCreateTable sql = (SqlCreateTable) ast;

    SchemaCatalog.SchemaName schemaName = context.getCurrentSchema();

    // rename table to `_rw_source_table`
    name = sql.name.getSimple();
    sourceName = TableCatalog.getTableSourceName(name);
    CreateTableInfo.Builder createTableInfoBuilder = CreateTableInfo.builder(sourceName);
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
    tableId = table.getId();
    return TableNodeSerializer.createProtoFromCatalog(table, true, null);
  }
}
