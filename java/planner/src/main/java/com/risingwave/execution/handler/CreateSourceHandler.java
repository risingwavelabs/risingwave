package com.risingwave.execution.handler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.risingwave.catalog.ColumnCatalog;
import com.risingwave.catalog.ColumnDesc;
import com.risingwave.catalog.ColumnEncoding;
import com.risingwave.catalog.CreateTableInfo;
import com.risingwave.catalog.SchemaCatalog;
import com.risingwave.catalog.TableCatalog;
import com.risingwave.common.datatype.RisingWaveDataType;
import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.execution.handler.util.CreateTaskBroadcaster;
import com.risingwave.execution.result.DdlResult;
import com.risingwave.pgwire.msg.StatementType;
import com.risingwave.planner.sql.SqlConverter;
import com.risingwave.proto.plan.*;
import com.risingwave.rpc.Messages;
import com.risingwave.sql.node.SqlCreateSource;
import com.risingwave.sql.node.SqlTableOption;
import com.risingwave.sql.tree.ColumnDefinition;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.validate.SqlValidator;

/** Handler of <code>CREATE SOURCE</code> statement */
public class CreateSourceHandler implements SqlHandler {
  @Override
  public DdlResult handle(SqlNode ast, ExecutionContext context) {
    PlanFragment planFragment = execute(ast, context);
    CreateTaskBroadcaster.broadCastTaskFromPlanFragment(planFragment, context);
    return new DdlResult(StatementType.CREATE_STREAM, 0);
  }

  private static PlanFragment serialize(TableCatalog table) {
    TableCatalog.TableId tableId = table.getId();
    CreateSourceNode.Builder createSourceNodeBuilder = CreateSourceNode.newBuilder();
    var sourceInfoBuilder = StreamSourceInfo.newBuilder();

    ImmutableList<ColumnCatalog> allColumns = table.getAllColumns(false);
    sourceInfoBuilder.setRowIdIndex(-1);

    if (allColumns.stream().noneMatch(column -> column.getDesc().isPrimary())) {
      allColumns = table.getAllColumns(true);
      sourceInfoBuilder.setRowIdIndex(0);
    }

    for (ColumnCatalog columnCatalog : allColumns) {
      com.risingwave.proto.plan.ColumnDesc.Builder columnDescBuilder =
          com.risingwave.proto.plan.ColumnDesc.newBuilder();

      columnDescBuilder
          .setName(columnCatalog.getName())
          .setColumnType(columnCatalog.getDesc().getDataType().getProtobufType())
          .setColumnId(columnCatalog.getId().getValue());

      createSourceNodeBuilder.addColumnDescs(columnDescBuilder);
    }
    sourceInfoBuilder.putAllProperties(table.getProperties());
    sourceInfoBuilder.setRowFormat(table.getRowFormat());
    sourceInfoBuilder.setRowSchemaLocation(table.getRowSchemaLocation());

    CreateSourceNode createSourceNode =
        createSourceNodeBuilder
            .setTableRefId(Messages.getTableRefId(tableId))
            .setInfo(sourceInfoBuilder)
            .build();

    ExchangeInfo exchangeInfo =
        ExchangeInfo.newBuilder().setMode(ExchangeInfo.DistributionMode.SINGLE).build();

    PlanNode rootNode = PlanNode.newBuilder().setCreateSource(createSourceNode).build();

    return PlanFragment.newBuilder().setRoot(rootNode).setExchangeInfo(exchangeInfo).build();
  }

  @VisibleForTesting
  protected PlanFragment execute(SqlNode ast, ExecutionContext context) {
    SqlCreateSource sql = (SqlCreateSource) ast;

    SchemaCatalog.SchemaName schemaName = context.getCurrentSchema();

    String tableName = sql.getName().getSimple();
    CreateTableInfo.Builder createSourceInfoBuilder = CreateTableInfo.builder(tableName);

    Set<String> primaryColumns =
        sql.getPrimaryColumns().stream().map(ColumnDefinition::ident).collect(Collectors.toSet());

    if (sql.getColumnList() != null) {
      SqlValidator sqlConverter = SqlConverter.builder(context).build().getValidator();

      for (SqlNode column : sql.getColumnList()) {
        SqlColumnDeclaration columnDef = (SqlColumnDeclaration) column;

        ColumnDesc columnDesc =
            new ColumnDesc(
                (RisingWaveDataType) columnDef.dataType.deriveType(sqlConverter),
                primaryColumns.contains(columnDef.name.getSimple()),
                ColumnEncoding.RAW);
        createSourceInfoBuilder.addColumn(columnDef.name.getSimple(), columnDesc);
      }
    }

    Map<String, String> properties =
        sql.getPropertyList().stream()
            .map(n -> (SqlTableOption) n)
            .collect(
                Collectors.toMap(
                    n -> ((SqlCharStringLiteral) n.getKey()).getValueAs(String.class).toLowerCase(),
                    n -> ((SqlCharStringLiteral) n.getValue()).getValueAs(String.class)));

    createSourceInfoBuilder.setProperties(properties);
    createSourceInfoBuilder.setSource(true);
    createSourceInfoBuilder.setRowFormatFromString(sql.getRowFormat().getValueAs(String.class));
    createSourceInfoBuilder.setRowSchemaLocation(
        sql.getRowSchemaLocation().getValueAs(String.class));

    CreateTableInfo streamInfo = createSourceInfoBuilder.build();
    // Build a plan distribute to compute node.
    TableCatalog table = context.getCatalogService().createTable(schemaName, streamInfo);
    return serialize(table);
  }
}
