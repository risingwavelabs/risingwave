package com.risingwave.execution.handler;

import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.risingwave.catalog.CatalogService;
import com.risingwave.catalog.ColumnCatalog;
import com.risingwave.catalog.SimpleCatalogService;
import com.risingwave.catalog.TableCatalog;
import com.risingwave.common.config.Configuration;
import com.risingwave.common.config.LeaderServerConfigurations;
import com.risingwave.common.datatype.NumericTypeBase;
import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.execution.context.FrontendEnv;
import com.risingwave.execution.context.SessionConfiguration;
import com.risingwave.planner.TestPlannerModule;
import com.risingwave.planner.planner.streaming.StreamPlanner;
import com.risingwave.planner.rel.streaming.StreamingPlan;
import com.risingwave.sql.parser.SqlParser;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateMaterializedView;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CreateMaterializedViewHandlerTest {
  private static final String TEST_DB_NAME = "test_db";
  private static final String TEST_SCHEMA_NAME = "test_schema";

  protected CatalogService catalogService;
  protected ExecutionContext executionContext;
  protected SqlHandlerFactory sqlHandlerFactory;

  @BeforeAll
  public void initAll() {
    catalogService = new SimpleCatalogService();
    catalogService.createDatabase(TEST_DB_NAME, TEST_SCHEMA_NAME);
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
            .withSessionConfig(new SessionConfiguration(frontendEnv.getConfiguration()))
            .build();
    String sql = "create table t(v1 int not null, v2 int not null, v3 float not null)";
    SqlNode ast = SqlParser.createCalciteStatement(sql);
    var handler = ((CreateTableHandler) sqlHandlerFactory.create(ast, executionContext));
    handler.execute(ast, executionContext);
  }

  @Test
  void testCatalogCase1() {
    String sql = "create materialized view mv1 as select v1, v2, sum(v3) from t group by v1, v2";
    SqlNode ast = SqlParser.createCalciteStatement(sql);
    var handler = ((CreateMaterializedViewHandler) sqlHandlerFactory.create(ast, executionContext));

    SqlCreateMaterializedView createMaterializedView = (SqlCreateMaterializedView) ast;

    String tableName = createMaterializedView.name.getSimple();
    StreamPlanner planner = new StreamPlanner();
    StreamingPlan plan = planner.plan(ast, executionContext);
    TableCatalog catalog = handler.convertPlanToCatalog(tableName, plan, executionContext, null);

    Assertions.assertEquals(catalog.isMaterializedView(), true);

    Assertions.assertEquals(catalog.getAllColumns().size(), 3);

    ColumnCatalog column0 = catalog.getAllColumns().get(0);
    Assertions.assertEquals(column0.getDesc().getDataType() instanceof NumericTypeBase, true);
    Assertions.assertEquals(
        ((NumericTypeBase) column0.getDesc().getDataType()).getSqlTypeName(), SqlTypeName.INTEGER);

    ColumnCatalog column1 = catalog.getAllColumns().get(1);
    Assertions.assertEquals(column1.getDesc().getDataType() instanceof NumericTypeBase, true);
    Assertions.assertEquals(
        ((NumericTypeBase) column1.getDesc().getDataType()).getSqlTypeName(), SqlTypeName.INTEGER);

    ColumnCatalog column2 = catalog.getAllColumns().get(2);
    Assertions.assertEquals(column2.getDesc().getDataType() instanceof NumericTypeBase, true);
    Assertions.assertEquals(
        ((NumericTypeBase) column2.getDesc().getDataType()).getSqlTypeName(), SqlTypeName.FLOAT);
  }

  @Test
  void testCatalogCase2() {
    String sql = "create materialized view mv2 as select v1+5 from t";
    SqlNode ast = SqlParser.createCalciteStatement(sql);
    var handler = ((CreateMaterializedViewHandler) sqlHandlerFactory.create(ast, executionContext));

    SqlCreateMaterializedView createMaterializedView = (SqlCreateMaterializedView) ast;
    String tableName = createMaterializedView.name.getSimple();
    StreamPlanner planner = new StreamPlanner();
    StreamingPlan plan = planner.plan(ast, executionContext);
    TableCatalog catalog = handler.convertPlanToCatalog(tableName, plan, executionContext, null);

    Assertions.assertEquals(catalog.isMaterializedView(), true);

    Assertions.assertEquals(catalog.getAllColumns().size(), 2);

    ColumnCatalog column0 = catalog.getAllColumns().get(0);
    Assertions.assertEquals(column0.getDesc().getDataType() instanceof NumericTypeBase, true);
    Assertions.assertEquals(
        SqlTypeName.INTEGER, ((NumericTypeBase) column0.getDesc().getDataType()).getSqlTypeName());

    ColumnCatalog column1 = catalog.getAllColumns().get(1);
    Assertions.assertEquals(column1.getDesc().getDataType() instanceof NumericTypeBase, true);
    Assertions.assertEquals(
        SqlTypeName.BIGINT, ((NumericTypeBase) column1.getDesc().getDataType()).getSqlTypeName());
  }

  @Test
  void testAliasedChecking1() {
    String sql = "create materialized view mv3 as select v1, v2, sum(v3) from t group by v1, v2";
    SqlNode ast = SqlParser.createCalciteStatement(sql);
    var handler = ((CreateMaterializedViewHandler) sqlHandlerFactory.create(ast, executionContext));

    StreamPlanner planner = new StreamPlanner();
    StreamingPlan plan = planner.plan(ast, executionContext);

    boolean isAliased = handler.isAllAliased(plan.getStreamingPlan());
    Assertions.assertEquals(false, isAliased);
  }

  @Test
  void testAliasedChecking2() {
    String sql = "create materialized view mv4 as select v1, v2 from t";
    SqlNode ast = SqlParser.createCalciteStatement(sql);
    var handler = ((CreateMaterializedViewHandler) sqlHandlerFactory.create(ast, executionContext));

    StreamPlanner planner = new StreamPlanner();
    StreamingPlan plan = planner.plan(ast, executionContext);

    boolean isAliased = handler.isAllAliased(plan.getStreamingPlan());
    Assertions.assertEquals(true, isAliased);
  }

  @Test
  void testAliasedChecking3() {
    String sql = "create materialized view mv5 as select sum(v1) as v1_sum from t";
    SqlNode ast = SqlParser.createCalciteStatement(sql);
    var handler = ((CreateMaterializedViewHandler) sqlHandlerFactory.create(ast, executionContext));

    StreamPlanner planner = new StreamPlanner();
    StreamingPlan plan = planner.plan(ast, executionContext);

    boolean isAliased = handler.isAllAliased(plan.getStreamingPlan());
    Assertions.assertTrue(isAliased);
  }
}
