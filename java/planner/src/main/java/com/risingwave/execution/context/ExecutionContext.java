package com.risingwave.execution.context;

import static java.util.Objects.requireNonNull;

import com.risingwave.catalog.CatalogService;
import com.risingwave.catalog.SchemaCatalog;
import com.risingwave.common.config.Configuration;
import com.risingwave.execution.handler.RpcExecutor;
import com.risingwave.execution.handler.RpcExecutorEmpty;
import com.risingwave.execution.handler.RpcExecutorImpl;
import com.risingwave.execution.handler.SqlHandlerFactory;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.schema.SchemaPlus;
import org.checkerframework.checker.nullness.qual.Nullable;

public class ExecutionContext implements Context {
  private final Configuration conf;
  private final String database;
  private final String schema;
  private final CatalogService catalogService;
  private final SqlHandlerFactory sqlHandlerFactory;
  private final RpcExecutor rpcExecutor;

  private ExecutionContext(Builder builder) {
    this.database = requireNonNull(builder.database, "Current database can't be null!");
    this.schema = requireNonNull(builder.schema, "Current schema can't be null!");
    this.catalogService = requireNonNull(builder.catalogService, "Catalog service can't be null!");
    this.conf = requireNonNull(builder.conf, "Configuration can't be null!");
    this.sqlHandlerFactory = requireNonNull(builder.sqlHandlerFactory, "sqlHandlerFactory");
    if (builder.rpcExecutor == null) {
      this.rpcExecutor = new RpcExecutorEmpty();
    } else {
      this.rpcExecutor = builder.rpcExecutor;
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public CatalogService getCatalogService() {
    return catalogService;
  }

  public RpcExecutor getRpcExecutor() {
    return rpcExecutor;
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

  public SqlHandlerFactory getSqlHandlerFactory() {
    return sqlHandlerFactory;
  }

  public Configuration getConf() {
    return conf;
  }

  public static ExecutionContext contextOf(RelOptCluster optCluster) {
    return contextOf(optCluster.getPlanner());
  }

  public static ExecutionContext contextOf(RelOptRuleCall call) {
    return contextOf(call.getPlanner());
  }

  public static ExecutionContext contextOf(RelOptPlanner planner) {
    return planner.getContext().unwrapOrThrow(ExecutionContext.class);
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
    private RpcExecutor rpcExecutor;
    private SqlHandlerFactory sqlHandlerFactory;

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

    public Builder withRpcExecutor() {
      this.rpcExecutor = new RpcExecutorImpl(this.conf);
      return this;
    }

    public Builder withSqlHandlerFactory(SqlHandlerFactory sqlHandlerFactory) {
      this.sqlHandlerFactory = sqlHandlerFactory;
      return this;
    }

    public ExecutionContext build() {
      return new ExecutionContext(this);
    }
  }
}
