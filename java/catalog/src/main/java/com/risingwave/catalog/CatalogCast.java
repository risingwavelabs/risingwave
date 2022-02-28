package com.risingwave.catalog;

import com.risingwave.proto.expr.InputRefExpr;
import com.risingwave.proto.metanode.Database;
import com.risingwave.proto.metanode.Schema;
import com.risingwave.proto.metanode.Table;
import com.risingwave.proto.plan.*;
import java.util.ArrayList;
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

    // ColumnDesc
    for (var columns : createTableInfo.getColumns()) {
      com.risingwave.proto.plan.ColumnDesc.Builder colBuilder =
          com.risingwave.proto.plan.ColumnDesc.newBuilder();
      colBuilder.setName(columns.left);
      colBuilder.setColumnType(columns.right.getDataType().getProtobufType());
      builder.addColumnDescs(colBuilder.build());
    }

    // Info
    if (createTableInfo.isSource()) {
      var sourceInfoBuilder = StreamSourceInfo.newBuilder();
      sourceInfoBuilder.setRowFormat(createTableInfo.getRowFormat());
      sourceInfoBuilder.putAllProperties(createTableInfo.getProperties());
      sourceInfoBuilder.setRowSchemaLocation(createTableInfo.getRowSchemaLocation());

      builder.setStreamSource(sourceInfoBuilder);
    } else if (createTableInfo.isMv()) {
      var mvInfoBuilder = MaterializedViewInfo.newBuilder();
      mvInfoBuilder.addAllPkIndices(createTableInfo.getPrimaryKeyIndices());

      // Dependent tables
      for (var tableId : createTableInfo.getDependentTables()) {
        SchemaCatalog.SchemaId schemaId = tableId.getParent();
        DatabaseCatalog.DatabaseId databaseId = schemaId.getParent();

        DatabaseRefId.Builder databaseRefIdBuilder = DatabaseRefId.newBuilder();
        databaseRefIdBuilder.setDatabaseId(databaseId.getValue());

        SchemaRefId.Builder schemaRefIdBuilder = SchemaRefId.newBuilder();
        schemaRefIdBuilder.setSchemaId(schemaId.getValue());
        schemaRefIdBuilder.setDatabaseRefId(databaseRefIdBuilder);

        TableRefId.Builder tableRefIdBuilder = TableRefId.newBuilder();
        tableRefIdBuilder.setTableId(tableId.getValue());
        tableRefIdBuilder.setSchemaRefId(schemaRefIdBuilder);

        mvInfoBuilder.addDependentTables(tableRefIdBuilder);
      }

      var createMaterializedViewInfo = (CreateMaterializedViewInfo) createTableInfo;
      // Column orders
      if (createMaterializedViewInfo.getCollation() != null) {
        var rfc = createMaterializedViewInfo.getCollation().getFieldCollations();
        for (RelFieldCollation relFieldCollation : rfc) {
          var inputRefExpr =
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
          mvInfoBuilder.addColumnOrders(columnOrder);
        }
      }

      // Associated table id
      var associatedTableRefId =
          TableRefId.newBuilder()
              .setTableId(createMaterializedViewInfo.getAssociatedTableId().getValue());
      mvInfoBuilder.setAssociatedTableRefId(associatedTableRefId);

      builder.setMaterializedView(mvInfoBuilder);
    } else {
      var tableInfoBuilder = TableSourceInfo.newBuilder();

      builder.setTableSource(tableInfoBuilder);
    }

    return builder.build();
  }

  public static CreateTableInfo tableFromProto(Table table) {
    CreateTableInfo.Builder builder = null;

    switch (table.getInfoCase()) {
      case STREAM_SOURCE:
        {
          builder = CreateTableInfo.builder(table.getTableName());
          builder.setSource(true);
          var info = table.getStreamSource();
          builder.setProperties(info.getPropertiesMap());
          builder.setAppendOnly(info.getAppendOnly());
          builder.setRowFormat(info.getRowFormat());
          builder.setRowSchemaLocation(info.getRowSchemaLocation());

          break;
        }
      case TABLE_SOURCE:
        {
          builder = CreateTableInfo.builder(table.getTableName());

          break;
        }
      case MATERIALIZED_VIEW:
        {
          builder = CreateMaterializedViewInfo.builder(table.getTableName());
          builder.setMv(true);
          var info = table.getMaterializedView();
          info.getPkIndicesList().forEach(builder::addPrimaryKey);

          for (var tableRefId : info.getDependentTablesList()) {
            SchemaRefId schemaRefId = tableRefId.getSchemaRefId();
            DatabaseRefId databaseRefId = schemaRefId.getDatabaseRefId();
            DatabaseCatalog.DatabaseId databaseId =
                new DatabaseCatalog.DatabaseId(databaseRefId.getDatabaseId());
            SchemaCatalog.SchemaId schemaId =
                new SchemaCatalog.SchemaId(schemaRefId.getSchemaId(), databaseId);
            TableCatalog.TableId tableId =
                new TableCatalog.TableId(tableRefId.getTableId(), schemaId);
            builder.addDependentTable(tableId);
          }

          var fieldCollations = new ArrayList<RelFieldCollation>();
          for (var columnOrder : info.getColumnOrdersList()) {
            var orderType = columnOrder.getOrderType();
            var columnIdx = columnOrder.getInputRef().getColumnIdx();
            RelFieldCollation.Direction dir;
            if (orderType == OrderType.ASCENDING) {
              dir = RelFieldCollation.Direction.ASCENDING;
            } else if (orderType == OrderType.DESCENDING) {
              dir = RelFieldCollation.Direction.DESCENDING;
            } else {
              throw new SerializationException(
                  String.format("%s direction not supported", orderType));
            }
            fieldCollations.add(new RelFieldCollation(columnIdx, dir));
          }
          if (!fieldCollations.isEmpty()) {
            ((CreateMaterializedViewInfo.Builder) builder)
                .setCollation(RelCollations.of(fieldCollations));
          }

          var associatedTableId =
              new TableCatalog.TableId(info.getAssociatedTableRefId().getTableId(), null);
          ((CreateMaterializedViewInfo.Builder) builder).setAssociated(associatedTableId);

          break;
        }

      default:
        throw new SerializationException("table info not set or not supported");
    }

    for (com.risingwave.proto.plan.ColumnDesc desc : table.getColumnDescsList()) {
      builder.addColumn(desc.getName(), new com.risingwave.catalog.ColumnDesc(desc));
    }
    return builder.build();
  }
}
