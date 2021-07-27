package com.risingwave.catalog;

import com.risingwave.common.error.MetaServiceError;
import com.risingwave.common.exception.RisingWaveException;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/** Manipulating risingwave catalog. */
@ThreadSafe
public interface CatalogService {

  String DEFAULT_DATABASE_NAME = "dev";
  String DEFAULT_SCHEMA_NAME = "main";

  default DatabaseCatalog createDatabase(String name) {
    return createDatabase(DatabaseCatalog.DatabaseName.of(name));
  }

  DatabaseCatalog createDatabase(DatabaseCatalog.DatabaseName databaseName);

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
      throw RisingWaveException.from(MetaServiceError.SCHEMA_NOT_EXISTS, schemaName);
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
}
