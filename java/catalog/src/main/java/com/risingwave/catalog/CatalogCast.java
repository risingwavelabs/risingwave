package com.risingwave.catalog;

import com.risingwave.proto.expr.InputRefExpr;
import com.risingwave.proto.metanode.Database;
import com.risingwave.proto.metanode.Schema;
import com.risingwave.proto.metanode.Table;
import com.risingwave.proto.plan.ColumnOrder;
import com.risingwave.proto.plan.DatabaseRefId;
import com.risingwave.proto.plan.OrderType;
import com.risingwave.proto.plan.SchemaRefId;
import com.risingwave.proto.plan.TableRefId;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.commons.lang3.SerializationException;

/** CatalogCast provides static cast functions for catalog. */
public class CatalogCast {
  public static DatabaseRefId buildDatabaseRefId(DatabaseCatalog databaseCatalog) {
    return DatabaseRefId.newBuilder().setDatabaseId(databaseCatalog.getId().getValue()).build();
  }

  public static SchemaRefId buildSchemaRefId(
      DatabaseCatalog databaseCatalog, SchemaCatalog schemaCatalog) {
    SchemaRefId.Builder builder = SchemaRefId.newBuilder();
    builder.setSchemaId(schemaCatalog.getId().getValue());
    builder.setDatabaseRefId(buildDatabaseRefId(databaseCatalog));

    return builder.build();
  }

  public static TableRefId buildTableRefId(
      DatabaseCatalog databaseCatalog,
      SchemaCatalog schemaCatalog,
      TableCatalog.TableName tableName) {
    TableCatalog tableCatalog = schemaCatalog.getTableCatalog(tableName);
    TableRefId.Builder builder = TableRefId.newBuilder();
    builder.setTableId(tableCatalog.getId().getValue());
    builder.setSchemaRefId(buildSchemaRefId(databaseCatalog, schemaCatalog));

    return builder.build();
  }

  public static Database databaseToProto(String databaseName) {
    Database.Builder builder = Database.newBuilder();
    builder.setDatabaseName(databaseName);
    return builder.build();
  }

  public static Schema schemaToProto(
      DatabaseCatalog databaseCatalog, SchemaCatalog.SchemaName schemaName) {
    Schema.Builder builder = Schema.newBuilder();
    builder.setSchemaName(schemaName.getValue());
    builder.setSchemaRefId(
        SchemaRefId.newBuilder().setDatabaseRefId(buildDatabaseRefId(databaseCatalog)).build());
    return builder.build();
  }

  public static Table tableToProto(
      DatabaseCatalog databaseCatalog,
      SchemaCatalog schemaCatalog,
      CreateTableInfo createTableInfo) {
    Table.Builder builder = Table.newBuilder();
    builder.setTableName(createTableInfo.getName());
    builder.setTableRefId(
        TableRefId.newBuilder().setSchemaRefId(buildSchemaRefId(databaseCatalog, schemaCatalog)));
    builder.setIsMaterializedView(createTableInfo.isMv());
    builder.setIsSource(createTableInfo.isSource());
    builder.setDistType(Table.DistributionType.ALL);
    builder.setRowFormat(createTableInfo.getRowFormat());
    builder.putAllProperties(createTableInfo.getProperties());
    builder.addAllPkColumns(createTableInfo.getPrimaryKeyIndices());
    builder.setRowSchemaLocation(createTableInfo.getRowSchemaLocation());
    for (var columns : createTableInfo.getColumns()) {
      com.risingwave.proto.plan.ColumnDesc.Builder colBuilder =
          com.risingwave.proto.plan.ColumnDesc.newBuilder();
      colBuilder.setName(columns.left);
      colBuilder.setEncoding(
          com.risingwave.proto.plan.ColumnDesc.ColumnEncodingType.valueOf(
              columns.right.getEncoding().name()));
      colBuilder.setIsPrimary(columns.right.isPrimary());
      colBuilder.setColumnType(columns.right.getDataType().getProtobufType());
      builder.addColumnDescs(colBuilder.build());
    }

    if (createTableInfo.isMv()) {
      CreateMaterializedViewInfo createMaterializedViewInfo =
          (CreateMaterializedViewInfo) createTableInfo;
      if (createMaterializedViewInfo.getCollation() != null) {
        List<RelFieldCollation> rfc =
            createMaterializedViewInfo.getCollation().getFieldCollations();
        for (RelFieldCollation relFieldCollation : rfc) {
          InputRefExpr inputRefExpr =
              InputRefExpr.newBuilder().setColumnIdx(relFieldCollation.getFieldIndex()).build();
          RelFieldCollation.Direction dir = relFieldCollation.getDirection();
          OrderType orderType;
          if (dir == RelFieldCollation.Direction.ASCENDING) {
            orderType = OrderType.ASCENDING;
          } else if (dir == RelFieldCollation.Direction.DESCENDING) {
            orderType = OrderType.DESCENDING;
          } else {
            throw new SerializationException(String.format("%s direction not supported", dir));
          }
          ColumnOrder columnOrder =
              ColumnOrder.newBuilder().setOrderType(orderType).setInputRef(inputRefExpr).build();
          builder.addColumnOrders(columnOrder);
        }
      }
      builder.setIsAssociated(createMaterializedViewInfo.isAssociated());
    }

    return builder.build();
  }

  public static CreateTableInfo tableFromProto(Table table) {
    CreateTableInfo.Builder builder;
    if (!table.getIsMaterializedView()) {
      builder = CreateTableInfo.builder(table.getTableName());
    } else {
      builder = CreateMaterializedViewInfo.builder(table.getTableName());
    }
    builder.setMv(false);
    builder.setProperties(table.getPropertiesMap());
    table.getPkColumnsList().forEach(builder::addPrimaryKey);
    builder.setSource(table.getIsSource());
    builder.setAppendOnly(table.getAppendOnly());
    builder.setRowFormat(table.getRowFormat());
    builder.setRowSchemaLocation(table.getRowSchemaLocation());
    for (com.risingwave.proto.plan.ColumnDesc desc : table.getColumnDescsList()) {
      builder.addColumn(desc.getName(), new com.risingwave.catalog.ColumnDesc(desc));
    }
    if (!table.getIsMaterializedView()) {
      return builder.build();
    }
    var fieldCollations = new ArrayList<RelFieldCollation>();
    for (var columnOrder : table.getColumnOrdersList()) {
      var orderType = columnOrder.getOrderType();
      var columnIdx = columnOrder.getInputRef().getColumnIdx();
      RelFieldCollation.Direction dir;
      if (orderType == OrderType.ASCENDING) {
        dir = RelFieldCollation.Direction.ASCENDING;
      } else if (orderType == OrderType.DESCENDING) {
        dir = RelFieldCollation.Direction.DESCENDING;
      } else {
        throw new SerializationException(String.format("%s direction not supported", orderType));
      }
      fieldCollations.add(new RelFieldCollation(columnIdx, dir));
    }
    if (!fieldCollations.isEmpty()) {
      ((CreateMaterializedViewInfo.Builder) builder)
          .setCollation(RelCollations.of(fieldCollations));
    }
    ((CreateMaterializedViewInfo.Builder) builder).setAssociated(table.getIsAssociated());
    return builder.build();
  }
}
