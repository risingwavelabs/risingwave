package com.risingwave.execution.context;

import static java.util.Objects.requireNonNull;

import com.google.inject.Inject;
import com.risingwave.catalog.CatalogService;
import com.risingwave.common.config.Configuration;
import com.risingwave.execution.handler.RpcExecutor;
import com.risingwave.execution.handler.RpcExecutorEmpty;
import com.risingwave.execution.handler.SqlHandlerFactory;
import com.risingwave.node.WorkerNodeManager;
import java.util.Objects;

public class FrontendEnv {
  private final CatalogService catalogService;
  private final SqlHandlerFactory sqlHandlerFactory;
  private final RpcExecutor rpcExecutor;
  private final WorkerNodeManager nodeManager;
  private final Configuration conf;

  @Inject
  public FrontendEnv(
      CatalogService catalogService,
      SqlHandlerFactory sqlHandlerFactory,
      RpcExecutor rpcExecutor,
      WorkerNodeManager nodeManager,
      Configuration conf) {
    this.catalogService = requireNonNull(catalogService, "catalogService");
    this.sqlHandlerFactory = requireNonNull(sqlHandlerFactory, "sqlHandlerFactory");
    this.rpcExecutor = Objects.requireNonNullElseGet(rpcExecutor, RpcExecutorEmpty::new);
    // TODO: add null-check
    this.nodeManager = nodeManager;
    this.conf = requireNonNull(conf, "conf");
  }

  public CatalogService getCatalogService() {
    return catalogService;
  }

  public SqlHandlerFactory getSqlHandlerFactory() {
    return sqlHandlerFactory;
  }

  public RpcExecutor getRpcExecutor() {
    return rpcExecutor;
  }

  public WorkerNodeManager getWorkerNodeManager() {
    return nodeManager;
  }

  public Configuration getConfiguration() {
    return conf;
  }
}
