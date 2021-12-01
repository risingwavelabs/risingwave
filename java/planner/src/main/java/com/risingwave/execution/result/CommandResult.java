package com.risingwave.execution.result;

import static com.google.common.base.Verify.verify;

import com.risingwave.common.exception.PgException;
import com.risingwave.pgwire.msg.StatementType;
import org.jetbrains.annotations.NotNull;

/**
 * Result for insert statement.
 *
 * <p>It should not a query result since it may cause incompatibility with pg.
 */
public class CommandResult extends AbstractResult {
  public CommandResult(StatementType statementType, int rowCount) {
    super(statementType, rowCount, false);
    verify(statementType.isCommand(), "No command statement type: {}", statementType);
  }

  @NotNull
  @Override
  public PgIter createIterator() throws PgException {
    return null;
  }
}
