package com.risingwave.execution.result;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableList;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import com.risingwave.pgwire.database.PgFieldDescriptor;
import com.risingwave.pgwire.database.TypeOid;
import com.risingwave.pgwire.msg.StatementType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

/** Base class for query results */
abstract class AbstractQueryResult extends AbstractResult {

  final ImmutableList<PgFieldDescriptor> fields;

  public AbstractQueryResult(StatementType statementType, RelDataType resultType, int rowCount) {
    super(statementType, rowCount, true);
    this.fields = createFieldDescriptors(resultType);
  }

  private static ImmutableList<PgFieldDescriptor> createFieldDescriptors(RelDataType resultType) {
    checkNotNull(resultType, "Result type can't be null!");
    if (!resultType.isStruct()) {
      throw new PgException(PgErrorCode.INTERNAL_ERROR, "Must be struct type!");
    }

    return resultType.getFieldList().stream()
        .map(AbstractQueryResult::createPgFieldDesc)
        .collect(ImmutableList.toImmutableList());
  }

  private static PgFieldDescriptor createPgFieldDesc(RelDataTypeField field) {
    return new PgFieldDescriptor(field.getName(), TypeOid.of(field.getType()));
  }
}
