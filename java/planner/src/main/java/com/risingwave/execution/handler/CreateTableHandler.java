package com.risingwave.execution.handler;

import com.risingwave.catalog.ColumnDesc;
import com.risingwave.catalog.ColumnEncoding;
import com.risingwave.catalog.CreateTableInfo;
import com.risingwave.catalog.SchemaCatalog;
import com.risingwave.common.datatype.RisingWaveDataType;
import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.execution.result.DdlResult;
import com.risingwave.pgwire.msg.StatementType;
import com.risingwave.planner.sql.SqlConverter;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.validate.SqlValidator;

@HandlerSignature(sqlKinds = {SqlKind.CREATE_TABLE})
public class CreateTableHandler implements SqlHandler {
  @Override
  public DdlResult handle(SqlNode ast, ExecutionContext context) {
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

    context.getCatalogService().createTable(schemaName, createTableInfoBuilder.build());

    return new DdlResult(StatementType.OTHER, 0);
  }
}
