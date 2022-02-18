package com.risingwave.execution.handler;

import com.google.common.annotations.VisibleForTesting;
import com.risingwave.catalog.CreateMaterializedViewInfo;
import com.risingwave.catalog.MaterializedViewCatalog;
import com.risingwave.catalog.SchemaCatalog;
import com.risingwave.catalog.TableCatalog;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.execution.handler.serializer.TableNodeSerializer;
import com.risingwave.execution.result.DdlResult;
import com.risingwave.pgwire.msg.StatementType;
import com.risingwave.planner.planner.streaming.StreamPlanner;
import com.risingwave.planner.rel.serialization.StreamingPlanSerializer;
import com.risingwave.planner.rel.streaming.*;
import com.risingwave.proto.common.Status;
import com.risingwave.proto.computenode.CreateTaskRequest;
import com.risingwave.proto.computenode.CreateTaskResponse;
import com.risingwave.proto.computenode.GetDataRequest;
import com.risingwave.proto.plan.PlanFragment;
import com.risingwave.proto.plan.TableRefId;
import com.risingwave.proto.plan.TaskSinkId;
import com.risingwave.proto.streaming.plan.StreamNode;
import com.risingwave.rpc.ComputeClient;
import com.risingwave.rpc.ComputeClientManager;
import com.risingwave.rpc.Messages;
import com.risingwave.scheduler.streaming.StreamManager;
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

    // Send create MV tasks to all compute nodes.
    TableCatalog catalog = convertPlanToCatalog(tableName, plan, context);
    PlanFragment planFragment =
        TableNodeSerializer.createProtoFromCatalog(catalog, false, plan.getStreamingPlan());

    ComputeClientManager clientManager = context.getComputeClientManager();
    for (var node : context.getWorkerNodeManager().allNodes()) {
      ComputeClient client = clientManager.getOrCreate(node);
      CreateTaskRequest createTaskRequest = Messages.buildCreateTaskRequest(planFragment);
      log.info("Send request to:" + node.getRpcEndPoint().toString());
      log.info("Create task request:\n" + Messages.jsonFormat(createTaskRequest));
      CreateTaskResponse createTaskResponse = client.createTask(createTaskRequest);
      if (createTaskResponse.getStatus().getCode() != Status.Code.OK) {
        throw new PgException(PgErrorCode.INTERNAL_ERROR, "Create Task failed");
      }
      TaskSinkId taskSinkId = Messages.buildTaskSinkId(createTaskRequest.getTaskId());
      client.getData(GetDataRequest.newBuilder().setSinkId(taskSinkId).build());
    }

    // Bind stream plan with materialized view catalog.
    plan.getStreamingPlan().setTableId(catalog.getId());
    StreamManager streamManager = context.getStreamManager();
    StreamNode streamNode = StreamingPlanSerializer.serialize(plan.getStreamingPlan());
    log.info("Stream plan serialization:\n" + Messages.jsonFormat(streamNode));
    TableRefId tableRefId = Messages.getTableRefId(catalog.getId());

    streamManager.createMaterializedView(streamNode, tableRefId);
    return new DdlResult(StatementType.CREATE_MATERIALIZED_VIEW, 0);
  }

  @VisibleForTesting
  public MaterializedViewCatalog convertPlanToCatalog(
      String tableName, StreamingPlan plan, ExecutionContext context) {
    SchemaCatalog.SchemaName schemaName = context.getCurrentSchema();

    CreateMaterializedViewInfo.Builder builder = CreateMaterializedViewInfo.builder(tableName);
    RwStreamMaterializedView rootNode = plan.getStreamingPlan();
    var columns = rootNode.getColumns();
    for (var column : columns) {
      builder.addColumn(column.getKey(), column.getValue());
    }
    builder.setCollation(rootNode.getCollation());
    builder.setMv(true);
    builder.setDependentTables(rootNode.getDependentTables());
    rootNode.getPrimaryKeyIndices().forEach(builder::addPrimaryKey);
    CreateMaterializedViewInfo mvInfo = builder.build();
    MaterializedViewCatalog viewCatalog =
        context.getCatalogService().createMaterializedView(schemaName, mvInfo);
    rootNode.setTableId(viewCatalog.getId());
    return viewCatalog;
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
