package com.risingwave.planner;

import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.util.JsonFormat;
import com.risingwave.catalog.CatalogService;
import com.risingwave.catalog.SimpleCatalogService;
import com.risingwave.common.config.Configuration;
import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.execution.context.FrontendEnv;
import com.risingwave.execution.handler.DefaultSqlHandlerFactory;
import com.risingwave.execution.handler.SqlHandlerFactory;
import com.risingwave.sql.parser.SqlParser;
import org.apache.calcite.sql.SqlNode;
import org.reflections.Reflections;
import org.reflections.util.ConfigurationBuilder;

public abstract class SqlTestBase {

  protected static final String TEST_DB_NAME = "test_db";
  protected static final String TEST_SCHEMA_NAME = "test_schema";
  protected static final JsonFormat.TypeRegistry PROTOBUF_JSON_TYPE_REGISTRY = createTypeRegistry();
  protected CatalogService catalogService;
  protected ExecutionContext executionContext;
  protected SqlHandlerFactory sqlHandlerFactory;

  protected static JsonFormat.TypeRegistry createTypeRegistry() {
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

  protected void initCatalog() {
    catalogService = new SimpleCatalogService();
    catalogService.createDatabase(TEST_DB_NAME, TEST_SCHEMA_NAME);
  }

  protected void initEnv() {
    initCatalog();
    sqlHandlerFactory = new DefaultSqlHandlerFactory();

    var cfg = new Configuration();
    var frontendEnv = new FrontendEnv(catalogService, sqlHandlerFactory, null, null, cfg);
    executionContext =
        ExecutionContext.builder()
            .withDatabase(TEST_DB_NAME)
            .withSchema(TEST_SCHEMA_NAME)
            .withFrontendEnv(frontendEnv)
            .build();
  }

  protected static SqlNode parseDdl(String sql) {
    return SqlParser.createCalciteStatement(sql);
  }

  protected static SqlNode parseSql(String sql) {
    return SqlParser.createCalciteStatement(sql);
  }

  protected void executeSql(String sql) {
    SqlNode ast = parseSql(sql);
    sqlHandlerFactory.create(ast, executionContext).handle(ast, executionContext);
  }
}
