package com.risingwave.execution.handler;

import com.google.common.annotations.VisibleForTesting;
import com.risingwave.catalog.ColumnDesc;
import com.risingwave.catalog.ColumnEncoding;
import com.risingwave.catalog.CreateTableInfo;
import com.risingwave.catalog.SchemaCatalog;
import com.risingwave.catalog.TableCatalog;
import com.risingwave.common.datatype.RisingWaveDataType;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.execution.handler.util.CreateTaskBroadcaster;
import com.risingwave.execution.handler.util.TableNodeSerializer;
import com.risingwave.execution.result.DdlResult;
import com.risingwave.pgwire.msg.StatementType;
import com.risingwave.planner.planner.streaming.StreamPlanner;
import com.risingwave.planner.rel.serialization.StreamingPlanSerializer;
import com.risingwave.planner.rel.streaming.StreamingPlan;
import com.risingwave.planner.sql.SqlConverter;
import com.risingwave.proto.plan.PlanFragment;
import com.risingwave.proto.plan.TableRefId;
import com.risingwave.proto.streaming.plan.StreamNode;
import com.risingwave.rpc.Messages;
import com.risingwave.scheduler.streaming.StreamManager;
import com.risingwave.sql.parser.SqlParser;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateMaterializedView;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handler of <code>CREATE TABLE</code> or <code>CREATE TABLE_V2</code> statement */
@HandlerSignature(sqlKinds = {SqlKind.CREATE_TABLE})
public class CreateTableHandler implements SqlHandler {
  private static final Logger log = LoggerFactory.getLogger(CreateTableHandler.class);

  @Override
  public DdlResult handle(SqlNode ast, ExecutionContext context) {
    var pair = execute(ast, context);
    var plan = pair.left;
    var mvCatalog = pair.right.right;
    // Broadcast create source tasks to compute nodes.
    CreateTaskBroadcaster.broadCastTaskFromPlanFragment(pair.right.left, context);
    // Broadcast create mv tasks to compute nodes.
    var mvFragment =
        TableNodeSerializer.createProtoFromCatalog(mvCatalog, false, plan.getStreamingPlan());
    CreateTaskBroadcaster.broadCastTaskFromPlanFragment(mvFragment, context);

    // Send the create MV request meta.
    StreamManager streamManager = context.getStreamManager();
    StreamNode streamNode = StreamingPlanSerializer.serialize(plan.getStreamingPlan());
    log.debug("stream node ser:\n" + Messages.jsonFormat(streamNode));
    TableRefId tableRefId = Messages.getTableRefId(mvCatalog.getId());
    streamManager.createMaterializedView(streamNode, tableRefId);

    return new DdlResult(StatementType.CREATE_TABLE, 0);
  }

  /**
   * @param ast the input create table AST.
   * @param context the execution context.
   * @return The pair of: 1. The streaming plan of creating materialized view from source. 2.1 The
   *     plan fragment of creating source. 2.2 The catalog of materialized view.
   */
  @VisibleForTesting
  protected ImmutablePair<StreamingPlan, ImmutablePair<PlanFragment, TableCatalog>> execute(
      SqlNode ast, ExecutionContext context) {
    SqlCreateTable sql = (SqlCreateTable) ast;
    // Create source associated table.
    TableCatalog sourceTableCatalog = createSourceCatalog(ast, context);
    PlanFragment sourceTableFragment =
        TableNodeSerializer.createProtoFromCatalog(sourceTableCatalog, true, null);
    TableCatalog.TableId sourceTableId = sourceTableCatalog.getId();

    // Rename table to `_rw_source_table`.
    String name = sql.name.getSimple();
    var createMvSql =
        String.format(
            "CREATE MATERIALIZED VIEW \"%s\" AS SELECT * FROM \"%s\"",
            name, TableCatalog.getTableSourceName(name));
    var createMvAst = SqlParser.createCalciteStatement(createMvSql);
    SqlCreateMaterializedView createMaterializedView = (SqlCreateMaterializedView) createMvAst;
    String tableName = createMaterializedView.name.getSimple();

    // Generate a streaming plan representing the (distributed) dataflow for MV construction.
    // The planner decides whether the dataflow is in distributed mode by checking the cluster
    // configuration.
    StreamPlanner planner = new StreamPlanner();
    StreamingPlan plan = planner.plan(createMvAst, context);

    // Check whether the naming for columns in MV is valid.
    boolean isAllAliased = CreateMaterializedViewHandler.isAllAliased(plan.getStreamingPlan());
    if (!isAllAliased) {
      throw new PgException(
          PgErrorCode.INVALID_COLUMN_DEFINITION,
          "An alias name must be specified for an aggregation function");
    }
    // Register the view on catalog.
    TableCatalog mvCatalog =
        CreateMaterializedViewHandler.convertPlanToCatalog(tableName, plan, context, sourceTableId);

    // Bind stream plan with materialized view catalog.
    var streamingPlan = plan.getStreamingPlan();
    streamingPlan.setTableId(mvCatalog.getId());
    streamingPlan.setAssociatedTableId(sourceTableId); // associated streaming task to the table

    // Send the create MV requests to compute nodes.
    PlanFragment createMvFragment =
        TableNodeSerializer.createProtoFromCatalog(mvCatalog, false, streamingPlan);

    return ImmutablePair.of(plan, ImmutablePair.of(sourceTableFragment, mvCatalog));
  }

  /**
   * @param ast the input create table AST.
   * @param context the execution context.
   * @return The TableCatalog of source.
   */
  @VisibleForTesting
  protected TableCatalog createSourceCatalog(SqlNode ast, ExecutionContext context) {
    SqlCreateTable sql = (SqlCreateTable) ast;
    SchemaCatalog.SchemaName schemaName = context.getCurrentSchema();

    // Rename table to `_rw_source_table`.
    String name = sql.name.getSimple();
    CreateTableInfo.Builder createTableInfoBuilder =
        CreateTableInfo.builder(TableCatalog.getTableSourceName(name));

    // Create source table.
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
    return table;
  }
}
