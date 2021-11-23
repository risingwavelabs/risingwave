package com.risingwave.common.datatype;

import com.risingwave.proto.data.DataType;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.sql.type.SqlTypeName;

/** Used by calcite to represent literals like: YEAR, DAY */
public class SymbolType extends PrimitiveTypeBase {
  public SymbolType() {
    this(false);
  }

  public SymbolType(boolean nullable) {
    super(nullable, SqlTypeName.SYMBOL);
    resetDigest();
  }

  @Override
  protected PrimitiveTypeBase copyWithNullability(boolean nullable) {
    return new SymbolType(nullable);
  }

  @Override
  public DataType getProtobufType() {
    return DataType.newBuilder()
        .setTypeName(DataType.TypeName.SYMBOL)
        .setIsNullable(nullable)
        // the value of symbol is an enum of TimeUnitRange.
        // We will serialize it in string with max width 11
        // In TimeUnitRange, the longest word is 'MACROSECOND',
        .setPrecision(11)
        .build();
  }

  @Override
  protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    sb.append(getSqlTypeName());
  }

  @Override
  public RelDataTypeComparability getComparability() {
    return RelDataTypeComparability.NONE;
  }
}
