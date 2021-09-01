package com.risingwave.execution.context;

import static java.util.Objects.requireNonNull;

import com.risingwave.catalog.CatalogService;
import com.risingwave.catalog.SchemaCatalog;
import com.risingwave.common.config.Configuration;
import com.risingwave.execution.handler.RpcExecutor;
import com.risingwave.execution.handler.SqlHandlerFactory;
import com.risingwave.node.WorkerNodeManager;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.schema.SchemaPlus;
import org.checkerframework.checker.nullness.qual.Nullable;

public class ExecutionContext implements Context {
  private final String database;
  private final String schema;
  private final FrontendEnv frontendEnv;

  private ExecutionContext(Builder builder) {
    this.database = requireNonNull(builder.database, "Current database can't be null!");
    this.schema = requireNonNull(builder.schema, "Current schema can't be null!");
    this.frontendEnv = requireNonNull(builder.frontendEnv, "frontendEnv");
  }

  public static Builder builder() {
    return new Builder();
  }

  public CatalogService getCatalogService() {
    return frontendEnv.getCatalogService();
  }

  public WorkerNodeManager getWorkerNodeManager() {
    return frontendEnv.getWorkerNodeManager();
  }

  public RpcExecutor getRpcExecutor() {
    return frontendEnv.getRpcExecutor();
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
    return frontendEnv.getSqlHandlerFactory();
  }

  public Configuration getConf() {
    return frontendEnv.getConfiguration();
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
    return frontendEnv.getCatalogService().getSchemaChecked(getCurrentSchema()).plus();
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
    private String database;
    private String schema;
    private FrontendEnv frontendEnv;

    private Builder() {}

    public Builder withDatabase(String database) {
      this.database = database;
      return this;
    }

    public Builder withSchema(String schema) {
      this.schema = schema;
      return this;
    }

    public Builder withFrontendEnv(FrontendEnv frontendEnv) {
      this.frontendEnv = frontendEnv;
      return this;
    }

    public ExecutionContext build() {
      return new ExecutionContext(this);
    }
  }
}
