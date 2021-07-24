package com.risingwave.pgwire.database;

import com.risingwave.pgwire.types.PgValue;
import java.util.List;
import org.jetbrains.annotations.NotNull;

/** The result of a statement. */
public interface PgResult {

  String getStatementType();

  interface PgIter {
    @NotNull
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

  @NotNull
  PgIter createIterator() throws PgException;

  int getEffectedRowsCnt() throws PgException;
}
