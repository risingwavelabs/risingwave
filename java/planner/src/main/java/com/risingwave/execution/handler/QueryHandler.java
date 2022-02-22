package com.risingwave.execution.handler;

import com.risingwave.catalog.TableCatalog;
import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.execution.handler.cache.ScopedSnapshot;
import com.risingwave.execution.result.BatchDataChunkResult;
import com.risingwave.execution.result.CommandResult;
import com.risingwave.pgwire.database.PgResult;
import com.risingwave.planner.planner.batch.BatchPlanner;
import com.risingwave.planner.rel.physical.BatchPlan;
import com.risingwave.planner.rel.physical.RwBatchInsert;
import com.risingwave.proto.computenode.GetDataRequest;
import com.risingwave.proto.computenode.GetDataResponse;
import com.risingwave.proto.plan.TaskSinkId;
import com.risingwave.rpc.ComputeClient;
import com.risingwave.rpc.Messages;
import com.risingwave.scheduler.QueryManager;
import com.risingwave.scheduler.QueryResultLocation;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handler of user queries. */
@HandlerSignature(sqlKinds = {SqlKind.SELECT, SqlKind.INSERT, SqlKind.ORDER_BY})
public class QueryHandler implements SqlHandler {

  private static final Logger log = LoggerFactory.getLogger(QueryHandler.class);

  // When this env var is set, for inserting into TABLE_V2, we call `FLUSH` implicitly to ensure the
  // data is inserted.
  // Used for regression tests.
  private static final String IMPLICIT_FLUSH_ENV_VAR_KEY = "RW_IMPLICIT_FLUSH";

  @Override
  public PgResult handle(SqlNode ast, ExecutionContext context) {
    BatchPlanner planner = new BatchPlanner();
    BatchPlan plan = planner.plan(ast, context);

    BatchDataChunkResult result;
    // TODO: Temporarily force a snapshot update. Should be made into a option for user.
    context.getHummockSnapshotManager().force_update();
    try (ScopedSnapshot scopedSnapshot = context.getHummockSnapshotManager().getScopedSnapshot()) {
      QueryResultLocation resultLocation;
      try {
        QueryManager queryManager = context.getQueryManager();
        resultLocation = queryManager.schedule(plan, scopedSnapshot.getEpoch()).get();
      } catch (Exception exp) {
        throw new RuntimeException(exp);
      }

      TaskSinkId taskSinkId = Messages.buildTaskSinkId(resultLocation.getTaskId().toTaskIdProto());
      ComputeClient client =
          context.getComputeClientManager().getOrCreate(resultLocation.getNode());
      Iterator<GetDataResponse> iter =
          client.getData(GetDataRequest.newBuilder().setSinkId(taskSinkId).build());

      // Convert task data to list to iterate it multiple times.
      // FIXME: use Iterator<TaskData>
      ArrayList<GetDataResponse> responses = new ArrayList<>();
      while (iter.hasNext()) {
        responses.add(iter.next());
      }

      result =
          new BatchDataChunkResult(
              SqlHandler.getStatementType(ast), responses, plan.getRoot().getRowType());
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    var implicitFlush = System.getenv(IMPLICIT_FLUSH_ENV_VAR_KEY) != null;
    if (implicitFlush) {
      var isInsert = plan.getRoot() instanceof RwBatchInsert;
      if (isInsert) {
        var insert = (RwBatchInsert) plan.getRoot();
        var table = insert.getTable().unwrapOrThrow(TableCatalog.class);
        if (table.isAssociatedMaterializedView()) {
          log.debug("flush after inserting into table_v2 / associated materialized view");
          new FlushHandler().handle(null, context);
        }
      }
    }

    // TODO: We need a better solution for this.
    if (result.getStatementType().isCommand()) {
      var effectedRowCount =
          Integer.parseInt(result.createIterator().getRow().get(0).encodeInText());
      return new CommandResult(result.getStatementType(), effectedRowCount);
    } else {
      return result;
    }
  }
}
