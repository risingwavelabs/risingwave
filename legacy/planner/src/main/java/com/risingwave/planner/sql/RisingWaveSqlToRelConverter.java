package com.risingwave.planner.sql;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Custom SqlToRelConverter for RisingWave. */
public class RisingWaveSqlToRelConverter extends SqlToRelConverter {
  public RisingWaveSqlToRelConverter(
      RelOptTable.ViewExpander viewExpander,
      SqlValidator validator,
      Prepare.CatalogReader catalogReader,
      RelOptCluster cluster,
      SqlRexConvertletTable convertletTable,
      Config config) {
    super(viewExpander, validator, catalogReader, cluster, convertletTable, config);
  }

  @Override
  protected void convertFrom(
      Blackboard bb, @Nullable SqlNode from, @Nullable List<String> fieldNames) {
    if (from instanceof SqlBasicCall) {
      var call = (SqlBasicCall) from;
      if (call.getOperator() instanceof GenerateSeriesTableFunction) {
        // GenerateSeriesTableFunction is a special table function, it should be
        // first converted to LogicalTableFunctionScan like other table functions,
        // SqlUserDefinedTableFunction, e.g.
        convertCollectionTable(bb, call);
        return;
      }
    }
    super.convertFrom(bb, from, fieldNames);
  }
}
