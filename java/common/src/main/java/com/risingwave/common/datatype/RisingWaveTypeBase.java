package com.risingwave.common.datatype;

import static org.apache.calcite.rel.type.RelDataTypeImpl.NON_NULLABLE_SUFFIX;

public abstract class RisingWaveTypeBase implements RisingWaveDataType {
  protected String digest;

  protected RisingWaveTypeBase() {}

  protected void resetDigest() {
    digest = computeDigest();
  }

  protected String computeDigest() {
    StringBuilder sb = new StringBuilder();
    generateTypeString(sb, true);
    if (!isNullable()) {
      sb.append(NON_NULLABLE_SUFFIX);
    }

    return sb.toString();
  }

  @Override
  public String getFullTypeString() {
    return digest;
  }

  protected abstract void generateTypeString(StringBuilder sb, boolean withDetail);
}
