package com.risingwave.execution.context;

import static java.util.Objects.requireNonNull;

import com.risingwave.catalog.CatalogService;
import com.risingwave.catalog.SchemaCatalog;
import com.risingwave.common.config.Configuration;
import com.risingwave.execution.handler.SqlHandlerFactory;
import com.risingwave.execution.handler.cache.HummockSnapshotManager;
import com.risingwave.node.WorkerNodeManager;
import com.risingwave.rpc.ComputeClientManager;
import com.risingwave.scheduler.QueryManager;
import com.risingwave.scheduler.streaming.StreamManager;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.RelBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;

/** The context used for one execution. */
public class ExecutionContext implements Context {
  private final String database;
  private final String schema;
  private final FrontendEnv frontendEnv;
  private final SessionConfiguration sessionConfiguration;

  private ExecutionContext(Builder builder) {
    this.database = requireNonNull(builder.database, "Current database can't be null!");
    this.schema = requireNonNull(builder.schema, "Current schema can't be null!");
    this.frontendEnv = requireNonNull(builder.frontendEnv, "frontendEnv");
    this.sessionConfiguration =
        requireNonNull(builder.sessionConfiguration, "sessionConfiguration");
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

  public ComputeClientManager getComputeClientManager() {
    return frontendEnv.getComputeClientManager();
  }

  public HummockSnapshotManager getHummockSnapshotManager() {
    return frontendEnv.getHummockSnapshotManager();
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

  public SessionConfiguration getSessionConfiguration() {
    return sessionConfiguration;
  }

  public QueryManager getQueryManager() {
    return frontendEnv.getQueryManager();
  }

  public StreamManager getStreamManager() {
    return frontendEnv.getStreamManager();
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
    } else if (klass == RelBuilder.Config.class) {
      return klass.cast(RelBuilder.Config.DEFAULT.withSimplify(false));
    } else {
      return null;
    }
  }

  /** The builder class for constructing one `ExecutionContext`. */
  public static class Builder {
    private String database;
    private String schema;
    private FrontendEnv frontendEnv;
    private SessionConfiguration sessionConfiguration;

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

    public Builder withSessionConfig(SessionConfiguration sessionConfig) {
      this.sessionConfiguration = sessionConfig;
      return this;
    }

    public ExecutionContext build() {
      return new ExecutionContext(this);
    }
  }
}
