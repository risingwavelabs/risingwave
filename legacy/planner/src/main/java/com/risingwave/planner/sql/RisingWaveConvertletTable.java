package com.risingwave.planner.sql;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.checkerframework.checker.nullness.qual.Nullable;

public class RisingWaveConvertletTable implements SqlRexConvertletTable {
  @Override
  public @Nullable SqlRexConvertlet get(SqlCall call) {
    return StandardConvertletTable.INSTANCE.get(call);
  }
}
