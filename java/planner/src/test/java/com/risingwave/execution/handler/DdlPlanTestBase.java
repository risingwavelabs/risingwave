package com.risingwave.execution.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.Lists;
import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.risingwave.catalog.CatalogService;
import com.risingwave.catalog.SimpleCatalogService;
import com.risingwave.common.config.Configuration;
import com.risingwave.common.config.LeaderServerConfigurations;
import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.execution.context.FrontendEnv;
import com.risingwave.node.DefaultWorkerNodeManager;
import com.risingwave.planner.util.PlannerTestCase;
import com.risingwave.proto.plan.PlanFragment;
import com.risingwave.rpc.TestComputeClientManager;
import com.risingwave.sql.parser.SqlParser;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.reflections.Reflections;
import org.reflections.util.ConfigurationBuilder;

public class DdlPlanTestBase {
  private static final String TEST_DB_NAME = "test_db";
  private static final String TEST_SCHEMA_NAME = "test_schema";

  private static final JsonFormat.TypeRegistry PROTOBUF_JSON_TYPE_REGISTRY = createTypeRegistry();

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
    var frontendEnv =
        new FrontendEnv(
            catalogService,
            sqlHandlerFactory,
            new TestComputeClientManager(),
            new DefaultWorkerNodeManager(cfg),
            cfg);
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

    String serializedJsonPlan = serializePlanToJson(ret);
    assertEquals(testCase.getJson(), serializedJsonPlan, "Plan not match!");
  }

  private static String serializePlanToJson(PlanFragment ret) {
    try {
      return JsonFormat.printer().usingTypeRegistry(PROTOBUF_JSON_TYPE_REGISTRY).print(ret);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Failed to serialize pan to json.", e);
    }
  }

  private static JsonFormat.TypeRegistry createTypeRegistry() {
    try {
      String packageName = "com.risingwave.proto";
      Reflections reflections =
          new Reflections(new ConfigurationBuilder().forPackages(packageName));
      JsonFormat.TypeRegistry.Builder typeRegistry = JsonFormat.TypeRegistry.newBuilder();

      for (Class<?> klass : reflections.getSubTypesOf(GeneratedMessageV3.class)) {
        Descriptors.Descriptor descriptor =
            (Descriptors.Descriptor) klass.getDeclaredMethod("getDescriptor").invoke(null);
        typeRegistry.add(descriptor);
      }

      return typeRegistry.build();
    } catch (Exception e) {
      throw new RuntimeException("Failed to create protobuf type registry!", e);
    }
  }
}
