package com.risingwave.execution.result;

import com.risingwave.common.exception.PgException;
import com.risingwave.pgwire.msg.StatementType;

/** Return result of set parameters handler. */
public class SetParameterResult extends AbstractResult {

  public SetParameterResult(StatementType statementType) {
    super(statementType, 1, false);
  }

  @Override
  public PgIter createIterator() throws PgException {
    return null;
  }
}
