package com.risingwave.planner;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.execution.context.FrontendEnv;
import com.risingwave.execution.context.SessionConfiguration;
import com.risingwave.pgwire.database.PgResult;
import com.risingwave.sql.parser.SqlParser;
import org.apache.calcite.sql.SqlNode;

/**
 * To write a json plan tests, the json source should be write in a `xxx.json` under
 * com/risingwave/planner/json. The XML section should have attribute name="json", path="xxx.json".
 * The TestBase will automatically find the files and compare serialize results.
 */
public abstract class SqlTestBase {

  protected static final String TEST_DB_NAME = "test_db";
  protected static final String TEST_SCHEMA_NAME = "test_schema";
  protected ExecutionContext executionContext;

  protected void initEnv() {
    Injector injector = Guice.createInjector(new TestPlannerModule(TEST_DB_NAME, TEST_SCHEMA_NAME));
    FrontendEnv frontendEnv = injector.getInstance(FrontendEnv.class);
    executionContext =
        ExecutionContext.builder()
            .withDatabase(TEST_DB_NAME)
            .withSchema(TEST_SCHEMA_NAME)
            .withFrontendEnv(frontendEnv)
            .withSessionConfig(new SessionConfiguration(frontendEnv.getConfiguration()))
            .build();
  }

  protected static SqlNode parseDdl(String sql) {
    return SqlParser.createCalciteStatement(sql);
  }

  protected static SqlNode parseSql(String sql) {
    return SqlParser.createCalciteStatement(sql);
  }

  protected PgResult executeSql(String sql) {
    SqlNode ast = parseSql(sql);
    return executionContext
        .getSqlHandlerFactory()
        .create(ast, executionContext)
        .handle(ast, executionContext);
  }
}
