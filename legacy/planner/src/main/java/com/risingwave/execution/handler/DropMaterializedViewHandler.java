package com.risingwave.execution.handler;

import com.risingwave.catalog.TableCatalog;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.execution.result.DdlResult;
import com.risingwave.pgwire.database.PgResult;
import com.risingwave.pgwire.msg.StatementType;
import com.risingwave.rpc.Messages;
import com.risingwave.scheduler.streaming.RemoteStreamManager;
import com.risingwave.scheduler.streaming.StreamManager;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlDropMaterializedView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The handler for processing drop materialized view statements. The handler works in two steps: (1)
 * tell meta service to send a termination barrier to all sources; (2) remove catalog information.
 */
@HandlerSignature(sqlKinds = {SqlKind.DROP_MATERIALIZED_VIEW})
public class DropMaterializedViewHandler implements SqlHandler {
  private static final Logger log = LoggerFactory.getLogger(DropMaterializedViewHandler.class);

  @Override
  public PgResult handle(SqlNode ast, ExecutionContext context) {
    SqlDropMaterializedView dropMaterializedView = (SqlDropMaterializedView) ast;
    TableCatalog.TableName tableName =
        TableCatalog.TableName.of(
            context.getDatabase(), context.getSchema(), dropMaterializedView.name.getSimple());
    TableCatalog table = context.getCatalogService().getTable(tableName);
    if (table == null) {
      if (!dropMaterializedView.ifExists) {
        throw new PgException(PgErrorCode.UNDEFINED_TABLE, "Materialized view does not exist");
      }
      return null;
    }
    log.debug("drop materialized view:\n" + tableName);
    context.getCatalogService().dropTable(tableName);
    StreamManager streamManager = context.getStreamManager();
    if (streamManager instanceof RemoteStreamManager) {
      RemoteStreamManager remoteStreamManager = (RemoteStreamManager) streamManager;
      remoteStreamManager.dropMaterializedView(Messages.getTableRefId(table.getId()));
      return new DdlResult(StatementType.DROP_MATERIALIZED_VIEW, 1);
    } else {
      throw new PgException(PgErrorCode.INTERNAL_ERROR, "Not available in local stream manager");
    }
  }
}
