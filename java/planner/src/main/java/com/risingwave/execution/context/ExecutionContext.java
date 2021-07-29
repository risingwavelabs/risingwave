package com.risingwave.execution.context;

import static java.util.Objects.requireNonNull;

import com.risingwave.catalog.CatalogService;
import com.risingwave.catalog.SchemaCatalog;
import com.risingwave.common.config.Configuration;
import org.apache.calcite.plan.Context;
import org.apache.calcite.schema.SchemaPlus;
import org.checkerframework.checker.nullness.qual.Nullable;

public class ExecutionContext implements Context {
  private final Configuration conf;
  private final String database;
  private final String schema;
  private final CatalogService catalogService;

  private ExecutionContext(Builder builder) {
    this.database = requireNonNull(builder.database, "Current database can't be null!");
    this.schema = requireNonNull(builder.schema, "Current schema can't be null!");
    this.catalogService = requireNonNull(builder.catalogService, "Catalog service can't be null!");
    this.conf = requireNonNull(builder.conf, "Configuration can't be null!");
  }

  public static Builder builder() {
    return new Builder();
  }

  public CatalogService getCatalogService() {
    return catalogService;
  }

  public String getDatabase() {
    return database;
  }

  public String getSchema() {
    return schema;
  }

  public SchemaCatalog.SchemaName getCurrentSchema() {
    return SchemaCatalog.SchemaName.of(database, schema);
  }

  public Configuration getConf() {
    return conf;
  }

  /**
   * Use by calcite.
   *
   * @return Current root schema.
   */
  public SchemaPlus getCalciteRootSchema() {
    return catalogService.getSchemaChecked(getCurrentSchema()).plus();
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
    private Configuration conf;
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

    public Builder withConfiguration(Configuration conf) {
      this.conf = conf;
      return this;
    }

    public ExecutionContext build() {
      return new ExecutionContext(this);
    }
  }
}
