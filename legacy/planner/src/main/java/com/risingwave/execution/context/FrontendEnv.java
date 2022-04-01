package com.risingwave.execution.context;

import static java.util.Objects.requireNonNull;

import com.google.inject.Inject;
import com.risingwave.catalog.CatalogService;
import com.risingwave.common.config.Configuration;
import com.risingwave.execution.handler.SqlHandlerFactory;
import com.risingwave.execution.handler.cache.HummockSnapshotManager;
import com.risingwave.node.WorkerNodeManager;
import com.risingwave.rpc.ComputeClientManager;
import com.risingwave.scheduler.QueryManager;
import com.risingwave.scheduler.streaming.StreamManager;
import java.util.Objects;

/** The front-end environment for leader node. */
public class FrontendEnv {
  private final CatalogService catalogService;
  private final SqlHandlerFactory sqlHandlerFactory;
  private final ComputeClientManager clientManager;
  private final WorkerNodeManager nodeManager;
  private final Configuration conf;
  private final QueryManager queryManager;
  private final StreamManager streamManager;
  private final HummockSnapshotManager hummockSnapshotManager;

  @Inject
  public FrontendEnv(
      CatalogService catalogService,
      SqlHandlerFactory sqlHandlerFactory,
      ComputeClientManager clientManager,
      WorkerNodeManager nodeManager,
      Configuration conf,
      QueryManager queryManager,
      StreamManager streamManager,
      HummockSnapshotManager hummockSnapshotManager) {
    this.catalogService = requireNonNull(catalogService, "catalogService");
    this.sqlHandlerFactory = requireNonNull(sqlHandlerFactory, "sqlHandlerFactory");
    this.clientManager = Objects.requireNonNull(clientManager);
    // TODO: add null-check
    this.nodeManager = nodeManager;
    this.conf = requireNonNull(conf, "conf");
    this.queryManager = requireNonNull(queryManager, "queryManager");
    this.streamManager = requireNonNull(streamManager, "streamManager");
    this.hummockSnapshotManager = requireNonNull(hummockSnapshotManager, "hummockSnapshotManager");
  }

  public CatalogService getCatalogService() {
    return catalogService;
  }

  public SqlHandlerFactory getSqlHandlerFactory() {
    return sqlHandlerFactory;
  }

  public ComputeClientManager getComputeClientManager() {
    return clientManager;
  }

  public WorkerNodeManager getWorkerNodeManager() {
    return nodeManager;
  }

  public Configuration getConfiguration() {
    return conf;
  }

  public QueryManager getQueryManager() {
    return queryManager;
  }

  public StreamManager getStreamManager() {
    return streamManager;
  }

  public HummockSnapshotManager getHummockSnapshotManager() {
    return hummockSnapshotManager;
  }
}
