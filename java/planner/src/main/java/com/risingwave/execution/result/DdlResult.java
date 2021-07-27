package com.risingwave.execution.result;

import com.risingwave.common.exception.PgException;
import com.risingwave.pgwire.msg.StatementType;

public class DdlResult extends AbstractResult {
  public DdlResult(StatementType statementType, int rowCount) {
    super(statementType, rowCount, false);
  }

  @Override
  public PgIter createIterator() throws PgException {
    return null;
  }
}
