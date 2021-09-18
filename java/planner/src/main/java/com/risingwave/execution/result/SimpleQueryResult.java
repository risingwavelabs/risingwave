package com.risingwave.execution.result;

import com.risingwave.common.exception.PgException;
import com.risingwave.pgwire.database.PgFieldDescriptor;
import com.risingwave.pgwire.msg.StatementType;
import com.risingwave.pgwire.types.PgValue;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.calcite.rel.type.RelDataType;

/**
 * Wraps a list of rows as PG query results. It's suitable for simple query-like commands like
 * <code>DESC</code> or <code>EXPLAIN</code>
 */
public class SimpleQueryResult extends AbstractQueryResult {

  private final List<List<PgValue>> rows;

  public SimpleQueryResult(
      StatementType statementType, RelDataType resultType, List<List<PgValue>> rows) {
    super(statementType, resultType, rows.size());
    this.rows = rows;
  }

  @Nonnull
  @Override
  public PgIter createIterator() throws PgException {
    return new SimpleResultIter();
  }

  class SimpleResultIter implements PgIter {

    private int rowIndex = 0;

    @Nonnull
    @Override
    public List<PgFieldDescriptor> getRowDesc() throws PgException {
      return fields;
    }

    @Override
    public boolean next() throws PgException {
      if (rowIndex < rowCount) {
        rowIndex++;
        return true;
      } else {
        return false;
      }
    }

    @Override
    public List<PgValue> getRow() throws PgException {
      return rows.get(rowIndex - 1);
    }
  }
}
