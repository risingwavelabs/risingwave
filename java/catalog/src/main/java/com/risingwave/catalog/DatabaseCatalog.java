package com.risingwave.catalog;

import com.risingwave.common.entity.EntityBase;
import com.risingwave.common.entity.RootLikeBase;
import com.risingwave.common.error.MetaServiceError;
import com.risingwave.common.exception.RisingWaveException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class DatabaseCatalog
    extends EntityBase<DatabaseCatalog.DatabaseId, DatabaseCatalog.DatabaseName> {
  private final AtomicInteger nextSchemaId = new AtomicInteger(0);
  private final List<SchemaCatalog> schemas;
  private final ConcurrentMap<SchemaCatalog.SchemaId, SchemaCatalog> schemaById;
  private final ConcurrentMap<SchemaCatalog.SchemaName, SchemaCatalog> schemaByName;

  public DatabaseCatalog(DatabaseId databaseId, DatabaseName databaseName) {
    this(databaseId, databaseName, Collections.emptyList());
  }

  public DatabaseCatalog(
      DatabaseId databaseId, DatabaseName databaseName, Collection<SchemaCatalog> schemas) {
    super(databaseId, databaseName);
    this.schemas = new ArrayList<>(schemas);
    this.schemaById = EntityBase.groupBy(schemas, SchemaCatalog::getId);
    this.schemaByName = EntityBase.groupBy(schemas, SchemaCatalog::getEntityName);
  }

  void createSchema(String name) {
    SchemaCatalog.SchemaName schemaName = new SchemaCatalog.SchemaName(name, getEntityName());

    if (schemaByName.containsKey(schemaName)) {
      throw RisingWaveException.from(
          MetaServiceError.SCHEMA_ALREADY_EXISTS, name, getEntityName().getValue());
    }

    SchemaCatalog.SchemaId schemaId =
        new SchemaCatalog.SchemaId(nextSchemaId.incrementAndGet(), getId());
    SchemaCatalog schema = new SchemaCatalog(schemaId, schemaName);
    registerSchema(schema);
  }

  private void registerSchema(SchemaCatalog schema) {
    schemas.add(schema);
    schemaById.put(schema.getId(), schema);
    schemaByName.put(schema.getEntityName(), schema);
  }

  public SchemaCatalog getSchema(SchemaCatalog.SchemaName schemaName) {
    return schemaByName.get(schemaName);
  }

  public static class DatabaseId extends RootLikeBase<Integer> {
    public DatabaseId(Integer value) {
      super(value);
    }

    public static DatabaseId of(int id) {
      return new DatabaseId(id);
    }
  }

  public static class DatabaseName extends RootLikeBase<String> {
    public DatabaseName(String value) {
      super(value);
    }

    public static DatabaseName of(String name) {
      return new DatabaseName(name);
    }
  }
}
