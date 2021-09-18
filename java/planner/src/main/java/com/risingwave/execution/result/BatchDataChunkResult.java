package com.risingwave.execution.result;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.ImmutableList;
import com.risingwave.common.exception.PgException;
import com.risingwave.execution.result.rpc.PgValueReader;
import com.risingwave.execution.result.rpc.PgValueReaders;
import com.risingwave.pgwire.database.PgFieldDescriptor;
import com.risingwave.pgwire.msg.StatementType;
import com.risingwave.pgwire.types.PgValue;
import com.risingwave.proto.computenode.TaskData;
import com.risingwave.proto.data.DataChunk;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;

/** A wrapper of grpc remote result. */
public class BatchDataChunkResult extends AbstractQueryResult {
  private final List<TaskData> data;

  // A row in calcite is represented by a struct, and each column in the row
  // is represented by the field in the struct.
  public BatchDataChunkResult(
      StatementType statementType, List<TaskData> data, RelDataType resultType) {
    super(statementType, resultType, totalRowCount(data));
    this.data = ImmutableList.copyOf(data);
  }

  private static int totalRowCount(List<TaskData> data) {
    return data.stream().mapToInt(batch -> batch.getRecordBatch().getCardinality()).sum();
  }

  @Override
  public PgIter createIterator() throws PgException {
    return new BatchDataChunkIter();
  }

  private class BatchDataChunkIter implements PgIter {
    private DataChunkIter internalIter;
    private int index;

    private BatchDataChunkIter() {
      index = 0;
      resetDataIter();
    }

    @Override
    public List<PgFieldDescriptor> getRowDesc() throws PgException {
      return fields;
    }

    @Override
    public boolean next() throws PgException {
      if (index == data.size()) {
        return false;
      }
      boolean hasNext = internalIter.next();
      if (!hasNext) {
        // If no data in current task, switch to next one.
        index++;
        if (index == data.size()) {
          return false;
        }
        resetDataIter();
        return internalIter.next();
      }

      return hasNext;
    }

    @Override
    public List<PgValue> getRow() throws PgException {
      return internalIter.getRow();
    }

    private void resetDataIter() {
      // Currently our insert return 0 results so check to avoid out of bound error.
      if (data.size() != 0) {
        DataChunk curData = data.get(index).getRecordBatch();
        List<PgValueReader> readers =
            curData.getColumnsList().stream()
                .map(PgValueReaders::create)
                .collect(ImmutableList.toImmutableList());
        this.internalIter = new DataChunkIter(readers, curData.getCardinality());
      }
    }
  }

  private class DataChunkIter implements PgIter {
    private final List<PgValueReader> valueReaders;
    private final int cardinality;
    private int rowIndex = 0;

    private DataChunkIter(List<PgValueReader> valueReaders, int cardinality) {
      checkArgument(cardinality >= 0, "Non positive cardinality: %s", cardinality);
      this.valueReaders = ImmutableList.copyOf(valueReaders);
      this.cardinality = cardinality;
    }

    @Override
    public List<PgFieldDescriptor> getRowDesc() throws PgException {
      return fields;
    }

    @Override
    public boolean next() throws PgException {
      boolean hasNext = rowIndex < cardinality;
      if (!hasNext) {
        return false;
      }
      rowIndex++;
      return true;
    }

    @Override
    public List<PgValue> getRow() throws PgException {
      ArrayList<PgValue> ret = new ArrayList<>();
      for (PgValueReader reader : valueReaders) {
        ret.add(reader.next());
      }
      return ret;
    }
  }
}
