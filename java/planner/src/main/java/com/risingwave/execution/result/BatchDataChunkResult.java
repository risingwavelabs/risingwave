package com.risingwave.execution.result;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableList;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.execution.result.rpc.PgValueReader;
import com.risingwave.execution.result.rpc.PgValueReaders;
import com.risingwave.pgwire.database.PgFieldDescriptor;
import com.risingwave.pgwire.database.TypeOid;
import com.risingwave.pgwire.msg.StatementType;
import com.risingwave.pgwire.types.PgValue;
import com.risingwave.proto.computenode.TaskData;
import com.risingwave.proto.data.DataChunk;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

/** A wrapper of grpc remote result. */
public class BatchDataChunkResult extends AbstractResult {
  private final List<TaskData> data;
  private final ImmutableList<PgFieldDescriptor> fields;

  // A row in calcite is represented by a struct, and each column in the row
  // is represented by the field in the struct.
  public BatchDataChunkResult(
      StatementType statementType, boolean query, List<TaskData> data, RelDataType resultType) {
    super(statementType, totalRowCount(data), query);
    this.data = ImmutableList.copyOf(data);
    this.fields = fieldDescriptorsOf(resultType);
  }

  private static int totalRowCount(List<TaskData> data) {
    return data.stream().mapToInt(batch -> batch.getRecordBatch().getCardinality()).sum();
  }

  private static ImmutableList<PgFieldDescriptor> fieldDescriptorsOf(RelDataType resultType) {
    checkNotNull(resultType, "Result type can't be null!");
    if (!resultType.isStruct()) {
      throw new PgException(PgErrorCode.INTERNAL_ERROR, "Must be struct type!");
    }

    return resultType.getFieldList().stream()
        .map(BatchDataChunkResult::createPgFieldDesc)
        .collect(ImmutableList.toImmutableList());
  }

  private static PgFieldDescriptor createPgFieldDesc(RelDataTypeField field) {
    return new PgFieldDescriptor(field.getName(), typeOidOf(field.getType()));
  }

  private static TypeOid typeOidOf(RelDataType type) {
    switch (type.getSqlTypeName()) {
      case BOOLEAN:
        return TypeOid.BOOLEAN;
      case SMALLINT:
        return TypeOid.SMALLINT;
      case INTEGER:
        return TypeOid.INT;
      case BIGINT:
        return TypeOid.BIGINT;
      case FLOAT:
        return TypeOid.FLOAT4;
      case DOUBLE:
        return TypeOid.FLOAT8;
      case DATE:
        return TypeOid.DATE;
      default:
        throw new PgException(
            PgErrorCode.INTERNAL_ERROR, "Unsupported sql type: %s", type.getSqlTypeName());
    }
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
      ArrayList<PgValue> ret = new ArrayList<PgValue>();
      for (PgValueReader reader : valueReaders) {
        ret.add(reader.next());
      }
      return ret;
    }
  }
}
