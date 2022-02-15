package com.risingwave.execution.handler;

import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.risingwave.catalog.CatalogService;
import com.risingwave.catalog.SimpleCatalogService;
import com.risingwave.common.config.Configuration;
import com.risingwave.common.config.LeaderServerConfigurations;
import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.execution.context.FrontendEnv;
import com.risingwave.execution.context.SessionConfiguration;
import com.risingwave.planner.TestPlannerModule;
import com.risingwave.planner.planner.streaming.StreamPlanner;
import com.risingwave.planner.rel.streaming.StreamingPlan;
import com.risingwave.sql.parser.SqlParser;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateMaterializedView;
import org.apache.calcite.sql.ddl.SqlDropMaterializedView;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DropMaterializedViewHandlerTest {
  private static final String TEST_DB_NAME = "test_db";
  private static final String TEST_SCHEMA_NAME = "test_schema";

  protected CatalogService catalogService;
  protected ExecutionContext executionContext;
  protected SqlHandlerFactory sqlHandlerFactory;

  @BeforeAll
  public void initAll() {
    catalogService = new SimpleCatalogService();
    catalogService.createDatabase(TEST_DB_NAME, TEST_SCHEMA_NAME);
    sqlHandlerFactory = new DefaultSqlHandlerFactory(false);

    var cfg = new Configuration();
    cfg.set(LeaderServerConfigurations.COMPUTE_NODES, Lists.newArrayList("127.0.0.1:1234"));
    Injector injector = Guice.createInjector(new TestPlannerModule(TEST_DB_NAME, TEST_SCHEMA_NAME));
    FrontendEnv frontendEnv = injector.getInstance(FrontendEnv.class);
    executionContext =
        ExecutionContext.builder()
            .withDatabase(TEST_DB_NAME)
            .withSchema(TEST_SCHEMA_NAME)
            .withFrontendEnv(frontendEnv)
            .withSessionConfig(new SessionConfiguration(frontendEnv.getConfiguration()))
            .build();
    String sql = "create table t(v1 int not null, v2 int not null, v3 float not null)";
    SqlNode ast = SqlParser.createCalciteStatement(sql);
    var handler = ((CreateTableV1Handler) sqlHandlerFactory.create(ast, executionContext));
    handler.execute(ast, executionContext);

    // Add materialized view.
    String createSql =
        "create materialized view mv1 as select v1, v2, sum(v3) from t group by v1, v2";
    SqlNode createAst = SqlParser.createCalciteStatement(createSql);
    SqlCreateMaterializedView createMaterializedView = (SqlCreateMaterializedView) createAst;
    String tableName = createMaterializedView.name.getSimple();

    var createMaterializedViewHandler =
        ((CreateMaterializedViewHandler) sqlHandlerFactory.create(createAst, executionContext));
    StreamPlanner planner = new StreamPlanner();
    StreamingPlan plan = planner.plan(createAst, executionContext);
    createMaterializedViewHandler.convertPlanToCatalog(tableName, plan, executionContext);
  }

  @Test
  void testCatalogCase1() {
    String sql = "drop materialized view mv1";
    SqlNode ast = SqlParser.createCalciteStatement(sql);
    Assertions.assertTrue(ast instanceof SqlDropMaterializedView);
  }
}
