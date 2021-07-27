package com.risingwave.execution.result;

import com.risingwave.common.exception.PgException;
import com.risingwave.pgwire.database.PgFieldDescriptor;
import com.risingwave.pgwire.database.PgResult;
import com.risingwave.pgwire.types.PgValue;
import java.util.Collections;
import java.util.List;

public class EmptyIterator implements PgResult.PgIter {
  @Override
  public List<PgFieldDescriptor> getRowDesc() throws PgException {
    return Collections.emptyList();
  }

  @Override
  public boolean next() throws PgException {
    return false;
  }

  @Override
  public List<PgValue> getRow() throws PgException {
    return Collections.emptyList();
  }
}
