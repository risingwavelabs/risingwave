package com.risingwave.planner.context;

import static java.util.Objects.requireNonNull;

import com.risingwave.catalog.CatalogService;
import com.risingwave.catalog.SchemaCatalog;
import com.risingwave.common.error.ExecutionError;
import com.risingwave.common.exception.RisingWaveException;
import java.util.Optional;
import org.apache.calcite.plan.Context;
import org.apache.calcite.schema.SchemaPlus;
import org.checkerframework.checker.nullness.qual.Nullable;

public class ExecutionContext implements Context {
  private final Optional<String> database;
  private final Optional<String> schema;
  private final CatalogService catalogService;

  private ExecutionContext(Builder builder) {
    this.database = Optional.ofNullable(builder.database);
    this.schema = Optional.ofNullable(builder.schema);
    this.catalogService = requireNonNull(builder.catalogService, "Catalog service can't be null!");
  }

  public static Builder builder() {
    return new Builder();
  }

  public CatalogService getCatalogService() {
    return catalogService;
  }

  public Optional<String> getDatabase() {
    return database;
  }

  public Optional<String> getSchema() {
    return schema;
  }

  public Optional<SchemaCatalog.SchemaName> getCurrentSchema() {
    return database.flatMap(db -> schema.map(s -> SchemaCatalog.SchemaName.of(db, s)));
  }

  /**
   * Use by calcite.
   *
   * @return Current root schema.
   */
  public Optional<SchemaPlus> getCalciteRootSchema() {
    return getCurrentSchema().map(catalogService::getSchemaChecked).map(SchemaCatalog::plus);
  }

  public SchemaPlus getCalciteRootSchemaChecked() {
    return getCalciteRootSchema()
        .orElseThrow(() -> RisingWaveException.from(ExecutionError.CURRENT_SCHEMA_NOT_SET));
  }

  @Override
  public <C> @Nullable C unwrap(Class<C> klass) {
    if (klass.isInstance(this)) {
      return klass.cast(this);
    } else {
      return null;
    }
  }

  public static class Builder {
    private CatalogService catalogService;
    private String database;
    private String schema;

    private Builder() {}

    public Builder withCatalogService(CatalogService catalogService) {
      this.catalogService = catalogService;
      return this;
    }

    public Builder withDatabase(String database) {
      this.database = database;
      return this;
    }

    public Builder withSchema(String schema) {
      this.schema = schema;
      return this;
    }

    public ExecutionContext build() {
      return new ExecutionContext(this);
    }
  }
}
