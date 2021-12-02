package com.risingwave.execution.handler;

import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.pgwire.database.PgResult;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlDropMaterializedView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The handler for processing drop materialized view statements. The handler works in two steps: (1)
 * tell meta service to send a termination barrier to all sources; (2) remove catalog information.
 */
@HandlerSignature(sqlKinds = {SqlKind.DROP_MATERIALIZED_VIEW})
public class DropMaterializedViewHandler implements SqlHandler {
  private static final Logger log = LoggerFactory.getLogger(DropMaterializedViewHandler.class);

  @Override
  public PgResult handle(SqlNode ast, ExecutionContext context) {
    SqlDropMaterializedView dropMaterializedView = (SqlDropMaterializedView) ast;
    String tableName = dropMaterializedView.name.getSimple();
    return null;
  }
}
