package com.risingwave.planner.sql;

import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlDelegatingConformance;

public class RisingWaveConformance extends SqlDelegatingConformance {
  public RisingWaveConformance() {
    super(SqlConformanceEnum.DEFAULT);
  }
}
