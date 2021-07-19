package com.risingwave.catalog;

import com.risingwave.common.datatype.RisingWaveDataType;

/** */
public class ColumnDesc {
  private final RisingWaveDataType dataType;
  private final boolean primary;
  private final ColumnEncoding encoding;

  public ColumnDesc(RisingWaveDataType dataType, boolean primary, ColumnEncoding encoding) {
    this.dataType = dataType;
    this.primary = primary;
    this.encoding = encoding;
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
}
