package com.risingwave.execution.handler;

import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.execution.result.DdlResult;
import com.risingwave.pgwire.database.PgResult;
import com.risingwave.pgwire.msg.StatementType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlDropTable;

@HandlerSignature(sqlKinds = {SqlKind.DROP_TABLE})
public class DropTableHandler implements SqlHandler {
  @Override
  public PgResult handle(SqlNode ast, ExecutionContext context) {
    SqlDropTable sqlDropTable = (SqlDropTable) ast;

    context
        .getCatalogService()
        .dropTable(context.getDatabase(), context.getSchema(), sqlDropTable.name.getSimple());
    return new DdlResult(StatementType.OTHER, 0);
  }
}
