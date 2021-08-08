package com.risingwave.catalog;

import com.risingwave.common.error.MetaServiceError;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.common.exception.RisingWaveException;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/** Manipulating risingwave catalog. */
@ThreadSafe
public interface CatalogService {

  String DEFAULT_DATABASE_NAME = "dev";
  String DEFAULT_SCHEMA_NAME = "main";

  /**
   * Create a database will also create a default schema. Specify schema name parameter if do not
   * want to create default schema.
   *
   * @param databaseName Can be default db name or test db name
   * @return database catalog (include schema)
   */
  default DatabaseCatalog createDatabase(String databaseName) {
    return createDatabase(databaseName, DEFAULT_SCHEMA_NAME);
  }

  DatabaseCatalog createDatabase(String databaseName, String schemaName);

  @Nullable
  DatabaseCatalog getDatabase(DatabaseCatalog.DatabaseName databaseName);

  default DatabaseCatalog getDatabaseChecked(DatabaseCatalog.DatabaseName databaseName) {
    DatabaseCatalog databaseCatalog = getDatabase(databaseName);
    if (databaseCatalog == null) {
      throw RisingWaveException.from(MetaServiceError.DATABASE_NOT_EXISTS, databaseName);
    }
    return databaseCatalog;
  }

  default DatabaseCatalog getDatabaseChecked(String name) {
    return getDatabaseChecked(DatabaseCatalog.DatabaseName.of(name));
  }

  @Nullable
  SchemaCatalog getSchema(SchemaCatalog.SchemaName schemaName);

  default SchemaCatalog createSchema(String databaseName, String schemaName) {
    DatabaseCatalog database = getDatabaseChecked(databaseName);
    return createSchema(new SchemaCatalog.SchemaName(schemaName, database.getEntityName()));
  }

  SchemaCatalog createSchema(SchemaCatalog.SchemaName schemaName);

  default SchemaCatalog getSchemaChecked(SchemaCatalog.SchemaName schemaName) {
    SchemaCatalog schema = getSchema(schemaName);
    if (schema == null) {
      throw RisingWaveException.from(MetaServiceError.SCHEMA_NOT_EXISTS, schemaName.getValue());
    }
    return schema;
  }

  default SchemaCatalog getSchemaChecked(String db, String schema) {
    return getSchemaChecked(SchemaCatalog.SchemaName.of(db, schema));
  }

  TableCatalog createTable(SchemaCatalog.SchemaName schemaName, CreateTableInfo createTableInfo);

  default TableCatalog createTable(
      String database, String schema, CreateTableInfo createTableInfo) {
    return createTable(SchemaCatalog.SchemaName.of(database, schema), createTableInfo);
  }

  TableCatalog getTable(TableCatalog.TableName tableName);

  @Nullable
  default TableCatalog getTable(String database, String schema, String table) {
    return getTable(TableCatalog.TableName.of(database, schema, table));
  }

  default TableCatalog getTableChecked(TableCatalog.TableName tableName) {
    TableCatalog table = getTable(tableName);
    if (table == null) {
      throw new PgException(PgErrorCode.UNDEFINED_TABLE, "Table %s not found.", tableName);
    }

    return table;
  }

  void dropTable(TableCatalog.TableName tableName);

  default void dropTable(String database, String schema, String tableName) {
    dropTable(TableCatalog.TableName.of(database, schema, tableName));
  }
}
