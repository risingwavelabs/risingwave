package com.risingwave.planner;

import com.risingwave.catalog.CatalogService;
import com.risingwave.catalog.SimpleCatalogService;
import com.risingwave.planner.planner.PlannerContext;
import com.risingwave.planner.planner.batch.BatchPlanner;
import com.risingwave.planner.sql.SqlConverter;
import java.util.Collections;
import org.apache.calcite.config.Lex;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

public abstract class PlanTestBase {
  protected PlannerContext plannerContext;
  protected BatchPlanner batchPlanner;
  protected CatalogService catalogService;

  protected void init() {
    catalogService = new SimpleCatalogService();
    plannerContext = new PlannerContext(catalogService);
    SchemaPlus rootSchema = initSchema();

    SqlConverter sqlConverter =
        SqlConverter.builder(plannerContext, rootSchema)
            .withDefaultSchema(Collections.singletonList("sc"))
            .build();
    batchPlanner = new BatchPlanner(sqlConverter);
  }

  protected abstract SchemaPlus initSchema();

  protected SqlNode parseSql(String sql) {
    try {
      SqlParser.Config config =
          SqlParser.Config.DEFAULT.withCaseSensitive(true).withLex(Lex.MYSQL_ANSI);
      return SqlParser.create(sql, config).parseQuery();
    } catch (SqlParseException e) {
      throw new RuntimeException(e);
    }
  }
}
