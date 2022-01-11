package com.risingwave.catalog;

import com.google.common.base.MoreObjects;
import com.risingwave.common.datatype.RisingWaveDataType;
import com.risingwave.common.datatype.RisingWaveTypeFactory;

/** Column Description */
public class ColumnDesc {
  private final RisingWaveDataType dataType;
  private final boolean primary;
  private final ColumnEncoding encoding;

  public ColumnDesc(RisingWaveDataType dataType) {
    this(dataType, false, ColumnEncoding.RAW);
  }

  public ColumnDesc(RisingWaveDataType dataType, boolean primary, ColumnEncoding encoding) {
    this.dataType = dataType;
    this.primary = primary;
    this.encoding = encoding;
  }

  public ColumnDesc(com.risingwave.proto.plan.ColumnDesc desc) {
    this(
        RisingWaveTypeFactory.INSTANCE.createDataType(desc.getColumnType()),
        desc.getIsPrimary(),
        ColumnEncoding.valueOf(desc.getEncoding().name()));
  }

  public RisingWaveDataType getDataType() {
    return dataType;
  }

  public boolean isPrimary() {
    return primary;
  }

  public ColumnEncoding getEncoding() {
    return encoding;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("dataType", dataType.getSqlTypeName().name())
        .add("primary", primary)
        .toString();
  }
}
