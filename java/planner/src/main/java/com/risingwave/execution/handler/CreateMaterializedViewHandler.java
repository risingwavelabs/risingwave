package com.risingwave.execution.handler;

import com.google.common.annotations.VisibleForTesting;
import com.risingwave.catalog.CreateTableInfo;
import com.risingwave.catalog.SchemaCatalog;
import com.risingwave.catalog.TableCatalog;
import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.execution.result.DdlResult;
import com.risingwave.pgwire.msg.StatementType;
import com.risingwave.planner.planner.streaming.StreamPlanner;
import com.risingwave.planner.rel.physical.streaming.RwStreamMaterializedView;
import com.risingwave.planner.rel.physical.streaming.StreamingPlan;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateMaterializedView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@HandlerSignature(sqlKinds = {SqlKind.CREATE_MATERIALIZED_VIEW})
public class CreateMaterializedViewHandler implements SqlHandler {
  private static final Logger log = LoggerFactory.getLogger(CreateMaterializedViewHandler.class);

  @Override
  public DdlResult handle(SqlNode ast, ExecutionContext context) {
    SqlCreateMaterializedView createMaterializedView = (SqlCreateMaterializedView) ast;
    String tableName = createMaterializedView.name.getSimple();

    StreamPlanner planner = new StreamPlanner();
    StreamingPlan plan = planner.plan(ast, context);

    TableCatalog catalog = convertPlanToCatalog(tableName, plan, context);
    plan.getStreamingPlan().setTableId(catalog.getId());

    return new DdlResult(StatementType.CREATE_MATERIALIZED_VIEW, 0);
  }

  @VisibleForTesting
  public TableCatalog convertPlanToCatalog(
      String tableName, StreamingPlan plan, ExecutionContext context) {
    SchemaCatalog.SchemaName schemaName = context.getCurrentSchema();

    CreateTableInfo.Builder createTableInfoBuilder = CreateTableInfo.builder(tableName);
    RwStreamMaterializedView rootNode = (RwStreamMaterializedView) plan.getStreamingPlan();
    var columns = rootNode.getColumns();
    for (int i = 0; i < columns.size(); i++) {
      createTableInfoBuilder.addColumn(columns.get(i).getKey(), columns.get(i).getValue());
    }
    createTableInfoBuilder.setMv(true);
    CreateTableInfo tableInfo = createTableInfoBuilder.build();
    TableCatalog tableCatalog = context.getCatalogService().createTable(schemaName, tableInfo);
    rootNode.setTableId(tableCatalog.getId());
    return tableCatalog;
  }
}
