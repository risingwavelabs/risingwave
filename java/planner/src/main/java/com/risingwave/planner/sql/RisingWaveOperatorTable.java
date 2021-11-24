package com.risingwave.planner.sql;

import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Implementation of {@link SqlOperatorTable}. */
public class RisingWaveOperatorTable implements SqlOperatorTable {
  private final SqlStdOperatorTable delegation = SqlStdOperatorTable.instance();
  private final RisingWaveOverrideOperatorTable override =
      RisingWaveOverrideOperatorTable.instance();

  @Override
  public void lookupOperatorOverloads(
      SqlIdentifier opName,
      @Nullable SqlFunctionCategory category,
      SqlSyntax syntax,
      List<SqlOperator> operatorList,
      SqlNameMatcher nameMatcher) {
    override.lookupOperatorOverloads(opName, category, syntax, operatorList, nameMatcher);
    if (operatorList.size() > 0) {
      return;
    }
    delegation.lookupOperatorOverloads(opName, category, syntax, operatorList, nameMatcher);
  }

  @Override
  public List<SqlOperator> getOperatorList() {
    return delegation.getOperatorList();
  }

  public SqlOperator lookupOneOperator(SqlIdentifier functionName, SqlSyntax syntax) {
    List<SqlOperator> result = new ArrayList<>();
    SqlNameMatcher nameMatcher = SqlNameMatchers.withCaseSensitive(false);

    this.lookupOperatorOverloads(functionName, null, syntax, result, nameMatcher);
    if (result.size() < 1) {
      throw new PgException(PgErrorCode.SYNTAX_ERROR, "Function not found: %s", functionName);
    } else if (result.size() > 1) {
      throw new PgException(
          PgErrorCode.SYNTAX_ERROR, "Too many function not found: %s", functionName);
    }

    return result.get(0);
  }
}
