package com.risingwave.execution.handler;

import com.google.common.annotations.VisibleForTesting;
import com.risingwave.catalog.ColumnDesc;
import com.risingwave.catalog.ColumnEncoding;
import com.risingwave.catalog.CreateTableInfo;
import com.risingwave.catalog.SchemaCatalog;
import com.risingwave.catalog.TableCatalog;
import com.risingwave.common.datatype.RisingWaveDataType;
import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.execution.result.DdlResult;
import com.risingwave.pgwire.msg.StatementType;
import com.risingwave.planner.planner.batch.BatchPlanner;
import com.risingwave.planner.rel.physical.batch.BatchPlan;
import com.risingwave.planner.rel.physical.batch.RisingWaveBatchPhyRel;
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
    TableCatalog catalog = convertAstToCatalog(ast, context);
    return new DdlResult(StatementType.CREATE_MATERIALIZED_VIEW, 0);
  }

  @VisibleForTesting
  protected TableCatalog convertAstToCatalog(SqlNode ast, ExecutionContext context) {
    SqlCreateMaterializedView createMaterializedView = (SqlCreateMaterializedView) ast;

    SchemaCatalog.SchemaName schemaName = context.getCurrentSchema();
    String tableName = createMaterializedView.name.getSimple();
    SqlNode query = createMaterializedView.query;

    CreateTableInfo.Builder createTableInfoBuilder = CreateTableInfo.builder(tableName);

    if (query != null) {
      BatchPlanner planner = new BatchPlanner();
      BatchPlan plan = planner.plan(query, context);

      RisingWaveBatchPhyRel rootNode = plan.getRoot();
      var rowType = rootNode.getRowType();
      for (int i = 0; i < rowType.getFieldCount(); i++) {
        var field = rowType.getFieldList().get(i);
        ColumnDesc columnDesc =
            new ColumnDesc((RisingWaveDataType) field.getType(), false, ColumnEncoding.RAW);
        createTableInfoBuilder.addColumn(field.getName(), columnDesc);
      }
    }
    createTableInfoBuilder.setMv(true);
    CreateTableInfo tableInfo = createTableInfoBuilder.build();
    return context.getCatalogService().createTable(schemaName, tableInfo);
  }
}
