package com.risingwave.execution.result;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableList;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.execution.result.rpc.PgValueReader;
import com.risingwave.pgwire.database.PgFieldDescriptor;
import com.risingwave.pgwire.database.TypeOid;
import com.risingwave.pgwire.msg.StatementType;
import com.risingwave.pgwire.types.PgValue;
import com.risingwave.proto.computenode.TaskData;
import com.risingwave.proto.data.Buffer;
import com.risingwave.proto.data.ColumnCommon;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

/** A wrapper of grpc remote result. */
public class RemoteBatchPlanResult extends AbstractResult {
  private final ImmutableList<TaskData> data;
  private final ImmutableList<PgFieldDescriptor> fields;

  public RemoteBatchPlanResult(
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
        .map(RemoteBatchPlanResult::createPgFiledDesc)
        .collect(ImmutableList.toImmutableList());
  }

  private static PgFieldDescriptor createPgFiledDesc(RelDataTypeField field) {
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
      default:
        throw new PgException(
            PgErrorCode.INTERNAL_ERROR, "Unsupported sql type: %s", type.getSqlTypeName());
    }
  }

  @Override
  public PgIter createIterator() throws PgException {
    return null;
  }

  private class RecordBatchIter implements PgIter {
    private final List<PgValueReader> valueReaders;
    private final int cardinality;
    private int rowIndex = -1;

    private RecordBatchIter(List<PgValueReader> valueReaders, int cardinality) {
      checkArgument(cardinality > 0, "Non positive cardinality: %s", cardinality);
      this.valueReaders = ImmutableList.copyOf(valueReaders);
      this.cardinality = cardinality;
    }

    @Override
    public List<PgFieldDescriptor> getRowDesc() throws PgException {
      return fields;
    }

    @Override
    public boolean next() throws PgException {
      throw new UnsupportedOperationException("");
    }

    @Override
    public List<PgValue> getRow() throws PgException {
      throw new UnsupportedOperationException("");
    }
  }

  private static PgValue createPgValue(
      ColumnCommon.ColumnType columnType, Buffer buffer, int rowIndex) {
    throw new UnsupportedOperationException("");
  }
}
