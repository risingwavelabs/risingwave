package com.risingwave.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.risingwave.catalog.CatalogService;
import com.risingwave.catalog.SimpleCatalogService;
import com.risingwave.common.config.Configuration;
import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.execution.handler.SqlHandlerFactory;
import com.risingwave.planner.planner.batch.BatchPlanner;
import com.risingwave.planner.rel.physical.batch.BatchPlan;
import com.risingwave.planner.rel.serialization.ExplainWriter;
import com.risingwave.planner.util.PlannerTestCase;
import com.risingwave.planner.util.PlannerTestDdlLoader;
import com.risingwave.sql.parser.SqlParser;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;

public abstract class BatchPlanTestBase {
  private static final String TEST_DB_NAME = "test_db";
  private static final String TEST_SCHEMA_NAME = "test_schema";

  protected BatchPlanner batchPlanner;
  protected CatalogService catalogService;
  protected ExecutionContext executionContext;

  private void initCatalog() {
    catalogService = new SimpleCatalogService();
    catalogService.createDatabase(TEST_DB_NAME);
    catalogService.createSchema(TEST_DB_NAME, TEST_SCHEMA_NAME);
  }

  protected void init() {
    initCatalog();

    executionContext =
        ExecutionContext.builder()
            .withCatalogService(catalogService)
            .withDatabase(TEST_DB_NAME)
            .withSchema(TEST_SCHEMA_NAME)
            .withConfiguration(new Configuration())
            .build();

    initTables();
    batchPlanner = new BatchPlanner();
  }

  private void initTables() {
    List<String> ddls = PlannerTestDdlLoader.load(getClass());

    for (String ddl : ddls) {
      System.out.println("sql: " + ddl);
      SqlNode ddlSql = parseDdl(ddl);
      SqlHandlerFactory.create(ddlSql, executionContext).handle(ddlSql, executionContext);
    }
  }

  protected SqlNode parseDdl(String sql) {
    return SqlParser.createCalciteStatement(sql);
    //      SqlParser config =
    //          SqlParser.Config.DEFAULT
    //              .withCaseSensitive(true)
    //              .withLex(Lex.MYSQL_ANSI)
    //              .withParserFactory(SqlDdlParserImpl.FACTORY);
    //      return SqlParser.create(sql, config).parseQuery();
  }

  protected SqlNode parseSql(String sql) {
    //    try {
    //      SqlParser.Config config =
    //          SqlParser.Config.DEFAULT.withCaseSensitive(true).withLex(Lex.MYSQL_ANSI);
    //      return SqlParser.create(sql, config).parseQuery();
    //    } catch (SqlParseException e) {
    //      throw new RuntimeException(e);
    //    }
    return SqlParser.createCalciteStatement(sql);
  }

  protected void runTestCase(PlannerTestCase testCase) {
    String sql = testCase.getSql();

    SqlNode ast = parseSql(sql);
    BatchPlan plan = batchPlanner.plan(ast, executionContext);

    String explainedPlan = explainBatchPlan(plan.getRoot());
    assertEquals(testCase.getPlan(), explainedPlan, "Plan not match!");
  }

  private static String explainBatchPlan(RelNode relNode) {
    try (StringWriter sw = new StringWriter();
        PrintWriter printer = new PrintWriter(sw); ) {
      ExplainWriter writer = new ExplainWriter(printer);
      relNode.explain(writer);
      return sw.toString().trim();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
