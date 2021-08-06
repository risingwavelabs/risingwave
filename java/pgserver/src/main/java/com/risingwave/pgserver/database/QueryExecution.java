package com.risingwave.pgserver.database;

import static java.util.Objects.requireNonNull;

import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.parser.SqlParser;
import com.risingwave.pgwire.database.PgResult;
import java.util.concurrent.Callable;
import org.apache.calcite.sql.SqlNode;

public class QueryExecution implements Callable<PgResult> {
  private final ExecutionContext context;
  private final String sql;

  public QueryExecution(ExecutionContext context, String sql) {
    this.context = requireNonNull(context, "context");
    this.sql = requireNonNull(sql, "Sql can't be null!");
  }

  @Override
  public PgResult call() throws Exception {
    SqlNode ast = SqlParser.createStatement(sql);
    return context.getSqlHandlerFactory().create(ast, context).handle(ast, context);
  }
}
