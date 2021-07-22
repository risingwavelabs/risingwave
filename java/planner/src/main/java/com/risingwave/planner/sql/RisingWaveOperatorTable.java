package com.risingwave.planner.sql;

import java.util.List;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.checkerframework.checker.nullness.qual.Nullable;

public class RisingWaveOperatorTable implements SqlOperatorTable {
  private final SqlStdOperatorTable delegation = SqlStdOperatorTable.instance();

  @Override
  public void lookupOperatorOverloads(
      SqlIdentifier opName,
      @Nullable SqlFunctionCategory category,
      SqlSyntax syntax,
      List<SqlOperator> operatorList,
      SqlNameMatcher nameMatcher) {
    delegation.lookupOperatorOverloads(opName, category, syntax, operatorList, nameMatcher);
  }

  @Override
  public List<SqlOperator> getOperatorList() {
    return delegation.getOperatorList();
  }
}
