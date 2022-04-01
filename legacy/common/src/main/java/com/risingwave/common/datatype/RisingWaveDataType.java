package com.risingwave.common.datatype;

import com.risingwave.proto.data.DataType;
import org.apache.calcite.rel.type.RelDataType;

public interface RisingWaveDataType extends RelDataType {
  DataType getProtobufType();

  RisingWaveDataType withNullability(boolean nullable);
}
