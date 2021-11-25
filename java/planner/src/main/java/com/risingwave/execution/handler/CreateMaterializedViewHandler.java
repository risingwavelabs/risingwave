package com.risingwave.execution.handler;

import com.google.common.annotations.VisibleForTesting;
import com.risingwave.catalog.CreateTableInfo;
import com.risingwave.catalog.SchemaCatalog;
import com.risingwave.catalog.TableCatalog;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.execution.result.DdlResult;
import com.risingwave.node.WorkerNodeManager;
import com.risingwave.pgwire.msg.StatementType;
import com.risingwave.planner.planner.streaming.StreamPlanner;
import com.risingwave.planner.rel.physical.streaming.RwStreamMaterializedView;
import com.risingwave.planner.rel.physical.streaming.StreamingPlan;
import com.risingwave.proto.streaming.streamnode.BroadcastActorInfoTableRequest;
import com.risingwave.proto.streaming.streamnode.BuildFragmentRequest;
import com.risingwave.proto.streaming.streamnode.UpdateFragmentRequest;
import com.risingwave.rpc.Messages;
import com.risingwave.scheduler.streaming.StreamFragmenter;
import com.risingwave.scheduler.streaming.StreamManager;
import com.risingwave.scheduler.streaming.graph.StreamGraph;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateMaterializedView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A `CreateMaterializedViewHandler` handles the <code>Create Materialized View</code> statement in
 * following steps. (1) Generating a (distributed) stream plan representing the dataflow of the
 * stream processing. (2) Register the materialized view as a table in the storage catalog. (3)
 * Generating a stream DAG consists of stream fragments and their dependency. Each fragment
 * translates to an actor on the compute-node. (4) Allocating these actors to different
 * compute-nodes.
 */
@HandlerSignature(sqlKinds = {SqlKind.CREATE_MATERIALIZED_VIEW})
public class CreateMaterializedViewHandler implements SqlHandler {
  private static final Logger log = LoggerFactory.getLogger(CreateMaterializedViewHandler.class);

  @Override
  public DdlResult handle(SqlNode ast, ExecutionContext context) {
    SqlCreateMaterializedView createMaterializedView = (SqlCreateMaterializedView) ast;
    String tableName = createMaterializedView.name.getSimple();

    // Generate a streaming plan representing the (distributed) dataflow for MV construction.
    // The planner decides whether the dataflow is in distributed mode by checking the cluster
    // configuration.
    StreamPlanner planner = new StreamPlanner();
    StreamingPlan plan = planner.plan(ast, context);

    // Check whether the naming for columns in MV is valid.
    boolean isAllAliased = isAllAliased(plan.getStreamingPlan());
    if (!isAllAliased) {
      throw new PgException(
          PgErrorCode.INVALID_COLUMN_DEFINITION,
          "An alias name must be specified for an aggregation function");
    }

    // Bind stream plan with materialized view catalog.
    TableCatalog catalog = convertPlanToCatalog(tableName, plan, context);
    plan.getStreamingPlan().setTableId(catalog.getId());

    // Generate a stream graph that represents the dependencies between stream fragments.
    StreamGraph streamGraph = StreamFragmenter.generateGraph(plan, context);

    // Schedule fragments among workers.
    WorkerNodeManager nodeManager = context.getWorkerNodeManager();
    var clientManager = context.getComputeClientManager();
    StreamManager streamManager = context.getStreamManager();
    var streamRequests = streamManager.scheduleStreamGraph(streamGraph);
    var actorInfo = streamManager.getActorInfo(streamGraph.getAllFragmentId());

    for (var streamRequest : streamRequests) {
      var node = streamRequest.getWorkerNode();
      var client = clientManager.getOrCreate(node);

      BroadcastActorInfoTableRequest actorInfoTableRequest = actorInfo.serialize();
      log.debug("ActorInfoTable:\n" + Messages.jsonFormat(actorInfoTableRequest));
      client.broadcastActorInfoTable(actorInfoTableRequest);

      int updateRequestId = streamManager.nextScheduleId();
      UpdateFragmentRequest updateFragmentRequest = streamRequest.serialize(updateRequestId);
      log.debug("UpdateFragmentRequest:\n" + Messages.jsonFormat(updateFragmentRequest));
      client.updateFragment(updateFragmentRequest);

      int buildRequestId = streamManager.nextScheduleId();
      BuildFragmentRequest buildFragmentRequest = streamRequest.buildRequest(buildRequestId);
      log.debug("BuildFragmentRequest:\n" + Messages.jsonFormat(buildFragmentRequest));
      client.buildFragment(buildFragmentRequest);
    }

    return new DdlResult(StatementType.CREATE_MATERIALIZED_VIEW, 0);
  }

  @VisibleForTesting
  public TableCatalog convertPlanToCatalog(
      String tableName, StreamingPlan plan, ExecutionContext context) {
    SchemaCatalog.SchemaName schemaName = context.getCurrentSchema();

    CreateTableInfo.Builder createTableInfoBuilder = CreateTableInfo.builder(tableName);
    RwStreamMaterializedView rootNode = plan.getStreamingPlan();
    var columns = rootNode.getColumns();
    for (var column : columns) {
      createTableInfoBuilder.addColumn(column.getKey(), column.getValue());
    }
    createTableInfoBuilder.setMv(true);
    CreateTableInfo tableInfo = createTableInfoBuilder.build();
    TableCatalog tableCatalog = context.getCatalogService().createTable(schemaName, tableInfo);
    rootNode.setTableId(tableCatalog.getId());
    return tableCatalog;
  }

  @VisibleForTesting
  public boolean isAllAliased(RwStreamMaterializedView root) {
    // Trick for checking whether is there any un-aliased aggregations: check the name pattern of
    // columns. Un-aliased column is named as EXPR$1 etc.
    var columns = root.getColumns();
    for (var pair : columns) {
      if (pair.left.startsWith("EXPR$")) {
        return false;
      }
    }
    return true;
  }
}
