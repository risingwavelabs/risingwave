package com.risingwave.execution.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.risingwave.catalog.CatalogService;
import com.risingwave.catalog.SimpleCatalogService;
import com.risingwave.common.config.Configuration;
import com.risingwave.common.config.LeaderServerConfigurations;
import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.execution.context.FrontendEnv;
import com.risingwave.planner.util.PlannerTestCase;
import com.risingwave.proto.plan.PlanFragment;
import com.risingwave.rpc.Messages;
import com.risingwave.scheduler.TestPlannerModule;
import com.risingwave.sql.parser.SqlParser;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

/** Test base for ddl plan. */
public class DdlPlanTestBase {
  private static final String TEST_DB_NAME = "test_db";
  private static final String TEST_SCHEMA_NAME = "test_schema";

  protected CatalogService catalogService;
  protected ExecutionContext executionContext;
  protected SqlHandlerFactory sqlHandlerFactory;

  private void initCatalog() {
    catalogService = new SimpleCatalogService();
    catalogService.createDatabase(TEST_DB_NAME, TEST_SCHEMA_NAME);
  }

  protected void init() {
    initCatalog();
    sqlHandlerFactory = new DefaultSqlHandlerFactory();

    var cfg = new Configuration();
    cfg.set(LeaderServerConfigurations.COMPUTE_NODES, Lists.newArrayList("127.0.0.1:1234"));
    Injector injector = Guice.createInjector(new TestPlannerModule(TEST_DB_NAME, TEST_SCHEMA_NAME));
    FrontendEnv frontendEnv = injector.getInstance(FrontendEnv.class);
    executionContext =
        ExecutionContext.builder()
            .withDatabase(TEST_DB_NAME)
            .withSchema(TEST_SCHEMA_NAME)
            .withFrontendEnv(frontendEnv)
            .build();
    // Ddl test base do not init tables.
    // initTables();
  }

  protected SqlNode parseSql(String sql) {
    return SqlParser.createCalciteStatement(sql);
  }

  protected void runTestCase(PlannerTestCase testCase) {
    String sql = testCase.getSql();
    SqlNode ast = parseSql(sql);
    PlanFragment ret;
    if (ast.getKind() == SqlKind.CREATE_TABLE) {
      ret =
          ((CreateTableHandler) sqlHandlerFactory.create(ast, executionContext))
              .executeDdl(ast, executionContext);
    } else if (ast.getKind() == SqlKind.DROP_TABLE) {
      ret =
          ((DropTableHandler) sqlHandlerFactory.create(ast, executionContext))
              .executeDdl(ast, executionContext);
    } else {
      throw new UnsupportedOperationException("unsupported ddl in test");
    }

    // Do not error if no json test.
    if (testCase.getJson().isPresent()) {
      String serializedJsonPlan = Messages.jsonFormat(ret);
      assertEquals(testCase.getJson().get(), serializedJsonPlan, "Json not match!");
    }
  }
}
