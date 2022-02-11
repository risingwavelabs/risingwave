package com.risingwave.execution.handler;

import com.google.common.collect.ImmutableMap;
import com.risingwave.common.error.ExecutionError;
import com.risingwave.common.exception.RisingWaveException;
import com.risingwave.execution.context.ExecutionContext;
import com.risingwave.sql.node.SqlCreateSource;
import com.risingwave.sql.node.SqlCreateTableV2;
import com.risingwave.sql.node.SqlFlush;
import com.risingwave.sql.node.SqlShowParameters;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import javax.inject.Singleton;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factor for creating sql handler.
 *
 * <p>When loaded, this class will scan classpath for {@link SqlHandler} implementations.
 *
 * <p>A {@link SqlHandler} implementation has following requirements:
 *
 * <ul>
 *   <li>Must lie in package {@link com.risingwave.execution.handler}
 *   <li>A public constructor without argument.
 *   <li>Annotated with {@link HandlerSignature} to provides sql kinds it can handle
 *   <li>Implements {@link SqlHandler}.
 * </ul>
 *
 * @see CreateTableHandler
 */
@Singleton
public class DefaultSqlHandlerFactory implements SqlHandlerFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultSqlHandlerFactory.class);
  private static final String PACKAGE_NAME = DefaultSqlHandlerFactory.class.getPackage().getName();

  private static final String FORCE_TABLE_V1_ENV_VAR_KEY = "RW_FORCE_TABLE_V1";

  private static final ImmutableMap<SqlKind, Constructor<? extends SqlHandler>>
      SQL_HANDLER_FACTORY = createSqlHandlerFactory();

  private boolean useV2 = true;

  DefaultSqlHandlerFactory() {
    var forceTableV1 = System.getenv(FORCE_TABLE_V1_ENV_VAR_KEY) != null;
    if (forceTableV1) {
      useV2 = false;
      LOGGER.info("Env var `{}` is set, will not create table_v2.", FORCE_TABLE_V1_ENV_VAR_KEY);
    }
  }

  DefaultSqlHandlerFactory(boolean useV2) {
    this.useV2 = useV2;
  }

  @Override
  public void setUseV2(boolean useV2) {
    this.useV2 = useV2;
  }

  @Override
  public SqlHandler create(SqlNode ast, ExecutionContext context) {
    // TODO(TaoWu): Use operator name to find the handler.
    if (ast instanceof SqlCreateSource) {
      return new CreateSourceHandler();
    }

    // TODO(Bowen): Use operator name to find the handler
    if (ast instanceof SqlShowParameters) {
      return new ShowParameterHandler();
    }

    if (!useV2 && ast instanceof SqlCreateTable && !(ast instanceof SqlCreateTableV2)) {
      return new CreateTableHandler();
    }

    if (ast instanceof SqlFlush) {
      return new FlushHandler();
    }

    Constructor<? extends SqlHandler> constructor = SQL_HANDLER_FACTORY.get(ast.getKind());
    if (constructor == null) {
      throw RisingWaveException.from(ExecutionError.NOT_IMPLEMENTED, ast.getKind());
    }

    try {
      return constructor.newInstance();
    } catch (Exception e) {
      LOGGER.error("Failed to create handler for {}.", ast.getKind(), e);
      throw RisingWaveException.from(ExecutionError.INTERNAL, e);
    }
  }

  private static ImmutableMap<SqlKind, Constructor<? extends SqlHandler>>
      createSqlHandlerFactory() {
    Reflections reflections =
        new Reflections(
            new ConfigurationBuilder()
                .setUrls(ClasspathHelper.forPackage(PACKAGE_NAME))
                .setScanners(new SubTypesScanner(), new TypeAnnotationsScanner())
                .filterInputsBy(new FilterBuilder().includePackage(PACKAGE_NAME)));

    ImmutableMap.Builder<SqlKind, Constructor<? extends SqlHandler>> builder =
        ImmutableMap.builder();

    for (Class<? extends SqlHandler> klass : reflections.getSubTypesOf(SqlHandler.class)) {

      HandlerSignature handlerSignature = klass.getAnnotation(HandlerSignature.class);
      if (handlerSignature != null) {
        try {
          Constructor<? extends SqlHandler> emptyConstructor = klass.getDeclaredConstructor();
          Arrays.stream(handlerSignature.sqlKinds())
              .forEachOrdered(sqlKind -> builder.put(sqlKind, emptyConstructor));
        } catch (Exception e) {
          LOGGER.error("Failed to find no arg constructor for {}", klass.getName(), e);
          throw RisingWaveException.from(ExecutionError.INTERNAL, e);
        }
      }
    }

    return builder.build();
  }
}
