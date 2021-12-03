package com.risingwave.execution.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.protobuf.InvalidProtocolBufferException;
import com.risingwave.catalog.CatalogService;
import com.risingwave.catalog.SimpleCatalogService;
import com.risingwave.common.config.Configuration;
import com.risingwave.common.config.LeaderServerConfigurations;
import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.execution.context.FrontendEnv;
import com.risingwave.execution.context.SessionConfiguration;
import com.risingwave.planner.TestPlannerModule;
import com.risingwave.proto.data.DataType;
import com.risingwave.proto.plan.CreateStreamNode;
import com.risingwave.proto.plan.PlanFragment;
import com.risingwave.proto.plan.PlanNode;
import com.risingwave.sql.parser.SqlParser;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/** BasicStreamTest tests the most basic CreateStream and DropStream operations */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BasicStreamTest {
  private static final String TEST_DB_NAME = "test_db";
  private static final String TEST_SCHEMA_NAME = "test_schema";

  protected CatalogService catalogService;
  protected ExecutionContext executionContext;
  protected SqlHandlerFactory sqlHandlerFactory;

  /** prepare execution context */
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
  }

  @Test
  void testCreateStream() throws InvalidProtocolBufferException {
    String sql =
        "create stream s (id int not null, name char(8) not null, age int not null) with ( 'upstream.source' = 'file', 'local.file.path' = '/tmp/data.txt' ) row format json";
    SqlNode ast = SqlParser.createCalciteStatement(sql);
    PlanFragment ret =
        ((CreateStreamHandler) sqlHandlerFactory.create(ast, executionContext))
            .execute(ast, executionContext);
    PlanNode root = ret.getRoot();
    assertEquals(root.getNodeType(), PlanNode.PlanNodeType.CREATE_STREAM);

    CreateStreamNode node = root.getBody().unpack(CreateStreamNode.class);
    assertEquals(node.getColumnDescsCount(), 4);

    assertEquals(node.getColumnDescs(1).getName(), "id");
    assertEquals(node.getColumnDescs(1).getColumnType().getTypeName(), DataType.TypeName.INT32);

    assertEquals(node.getColumnDescs(2).getName(), "name");
    assertEquals(node.getColumnDescs(2).getColumnType().getTypeName(), DataType.TypeName.CHAR);
    assertEquals(node.getColumnDescs(2).getColumnType().getPrecision(), 8);

    assertEquals(node.getColumnDescs(3).getName(), "age");
    assertEquals(node.getColumnDescs(3).getColumnType().getTypeName(), DataType.TypeName.INT32);

    assertEquals(node.getPropertiesMap().get("upstream.source"), "file");
    assertEquals(node.getPropertiesMap().get("local.file.path"), "/tmp/data.txt");
  }

  @Test
  void testDropStream() {
    String sql = "drop stream s";
    SqlNode ast = SqlParser.createCalciteStatement(sql);
    PlanFragment ret =
        ((DropTableHandler) sqlHandlerFactory.create(ast, executionContext))
            .execute(ast, executionContext);
    PlanNode root = ret.getRoot();
    assertEquals(root.getNodeType(), PlanNode.PlanNodeType.DROP_STREAM);
  }
}
