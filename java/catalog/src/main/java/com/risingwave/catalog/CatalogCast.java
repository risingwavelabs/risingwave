package com.risingwave.catalog;

import com.risingwave.common.datatype.RisingWaveTypeFactory;
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

  public static Database databaseToProto(DatabaseCatalog databaseCatalog) {
    Database.Builder builder = Database.newBuilder();
    builder.setDatabaseName(databaseCatalog.getEntityName().getValue());
    builder.setDatabaseRefId(buildDatabaseRefId(databaseCatalog));
    return builder.build();
  }

  public static Schema schemaToProto(DatabaseCatalog databaseCatalog, SchemaCatalog schemaCatalog) {
    Schema.Builder builder = Schema.newBuilder();
    builder.setSchemaName(schemaCatalog.getEntityName().getValue());
    builder.setSchemaRefId(buildSchemaRefId(databaseCatalog, schemaCatalog));
    return builder.build();
  }

  public static Table tableToProto(
      DatabaseCatalog databaseCatalog, SchemaCatalog schemaCatalog, TableCatalog tableCatalog) {
    Table.Builder builder = Table.newBuilder();
    builder.setTableName(tableCatalog.getEntityName().getValue());
    builder.setTableRefId(
        buildTableRefId(databaseCatalog, schemaCatalog, tableCatalog.getEntityName()));
    builder.setIsMaterializedView(tableCatalog.isMaterializedView());
    builder.setIsStream(tableCatalog.isStream());
    builder.setDistType(Table.DistributionType.valueOf(tableCatalog.getDistributionType().name()));
    builder.setRowFormat(tableCatalog.getRowFormat());
    builder.putAllProperties(tableCatalog.getProperties());
    builder.addAllPkColumns(tableCatalog.getPrimaryKeyColumnIds());
    builder.setRowSchemaLocation(tableCatalog.getRowSchemaLocation());
    for (ColumnCatalog columnCatalog : tableCatalog.getAllColumns()) {
      com.risingwave.proto.plan.ColumnDesc.Builder colBuilder =
          com.risingwave.proto.plan.ColumnDesc.newBuilder();
      colBuilder.setName(columnCatalog.getName());
      colBuilder.setEncoding(
          com.risingwave.proto.plan.ColumnDesc.ColumnEncodingType.valueOf(
              columnCatalog.getDesc().getEncoding().name()));
      colBuilder.setIsPrimary(columnCatalog.getDesc().isPrimary());
      colBuilder.setColumnType(columnCatalog.getDesc().getDataType().getProtobufType());
      builder.addColumnDescs(colBuilder.build());
    }

    if (tableCatalog.isMaterializedView()) {
      MaterializedViewCatalog materializedViewCatalog = (MaterializedViewCatalog) tableCatalog;
      if (materializedViewCatalog.getCollation() != null) {
        List<RelFieldCollation> rfc = materializedViewCatalog.getCollation().getFieldCollations();
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
    builder.setStream(table.getIsStream());
    builder.setAppendOnly(table.getAppendOnly());
    builder.setRowFormat(table.getRowFormat());
    builder.setRowSchemaLocation(table.getRowSchemaLocation());
    for (com.risingwave.proto.plan.ColumnDesc desc : table.getColumnDescsList()) {
      builder.addColumn(desc.getName(), new com.risingwave.catalog.ColumnDesc(desc));
    }
    if (!table.getIsMaterializedView()) {
      return builder.build();
    }
    var typeFactory = RisingWaveTypeFactory.INSTANCE;
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
    return builder.build();
  }
}
