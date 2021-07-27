package com.risingwave.execution.result;

import static java.util.Objects.requireNonNull;

import com.risingwave.common.exception.PgException;
import com.risingwave.pgwire.database.PgResult;
import com.risingwave.pgwire.msg.StatementType;

public abstract class AbstractResult implements PgResult {
  private final StatementType statementType;
  private final int rowCount;
  private final boolean query;

  protected AbstractResult(StatementType statementType, int rowCount, boolean query) {
    this.statementType = requireNonNull(statementType, "Statement type can't be null!");
    this.rowCount = rowCount;
    this.query = query;
  }

  @Override
  public StatementType getStatementType() {
    return statementType;
  }

  @Override
  public boolean isQuery() {
    return query;
  }

  @Override
  public int getEffectedRowsCnt() throws PgException {
    return rowCount;
  }
}
