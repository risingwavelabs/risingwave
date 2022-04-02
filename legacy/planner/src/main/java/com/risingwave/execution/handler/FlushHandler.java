package com.risingwave.execution.handler;

import com.risingwave.catalog.*;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.execution.result.DdlResult;
import com.risingwave.pgwire.msg.StatementType;
import com.risingwave.proto.plan.*;
import com.risingwave.scheduler.streaming.RemoteStreamManager;
import com.risingwave.scheduler.streaming.StreamManager;
import org.apache.calcite.sql.SqlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handler of <code>Flush</code> statement */
public class FlushHandler implements SqlHandler {
  @Override
  public DdlResult handle(SqlNode ast, ExecutionContext context) {
    log.debug("flush barrier\n");
    StreamManager streamManager = context.getStreamManager();
    if (streamManager instanceof RemoteStreamManager) {
      RemoteStreamManager remoteStreamManager = (RemoteStreamManager) streamManager;
      remoteStreamManager.flush();
      return new DdlResult(StatementType.FLUSH, 0);
    } else {
      throw new PgException(PgErrorCode.INTERNAL_ERROR, "Not available in local stream manager");
    }
  }

  private static final Logger log = LoggerFactory.getLogger(FlushHandler.class);
}
