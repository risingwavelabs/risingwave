package com.risingwave.pgwire.database;

import com.risingwave.common.exception.PgException;
import com.risingwave.pgwire.msg.StatementType;
import com.risingwave.pgwire.types.PgValue;
import java.util.List;
import javax.annotation.Nonnull;

/** The result of a statement. */
public interface PgResult {

  StatementType getStatementType();

  interface PgIter {
    @Nonnull
    List<PgFieldDescriptor> getRowDesc() throws PgException;

    /**
     * Move the cursor to the next row.
     *
     * @return true if there's more row.
     */
    boolean next() throws PgException;

    List<PgValue> getRow() throws PgException;
  }

  /**
   * @return Whether the statement is a query, which has non-empty results. If true, the caller will
   *     scan all rows via <code>createIterator</code>.
   */
  boolean isQuery();

  @Nonnull
  PgIter createIterator() throws PgException;

  int getEffectedRowsCnt() throws PgException;
}
