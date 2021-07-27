package com.risingwave.pgserver.database;

import static java.util.Objects.requireNonNull;

import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.execution.handler.SqlHandlerFactory;
import com.risingwave.parser.SqlParser;
import com.risingwave.pgwire.database.PgResult;
import java.util.concurrent.Callable;
import org.apache.calcite.sql.SqlNode;

public class QueryExecution implements Callable<PgResult> {
  private final ExecutionContext context;
  private final String sql;

  public QueryExecution(ExecutionContext context, String sql) {
    this.context = context;
    this.sql = requireNonNull(sql, "Sql can't be null!");
  }

  @Override
  public PgResult call() throws Exception {
    SqlNode ast = SqlParser.createStatement(sql);
    return SqlHandlerFactory.create(ast, context).handle(ast, context);
  }
}
