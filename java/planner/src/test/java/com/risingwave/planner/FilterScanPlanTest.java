package com.risingwave.planner;

import com.risingwave.catalog.ColumnDesc;
import com.risingwave.catalog.CreateTableInfo;
import com.risingwave.common.datatype.RisingWaveDataType;
import com.risingwave.common.datatype.RisingWaveTypeFactory;
import com.risingwave.planner.rel.physical.batch.BatchPlan;
import com.risingwave.planner.util.PlanTestCaseLoader;
import com.risingwave.planner.util.PlannerTestCase;
import com.risingwave.planner.util.ToPlannerTestCase;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

public class FilterScanPlanTest extends PlanTestBase {

  @ParameterizedTest(name = "{index} => {0}")
  @DisplayName("Filter scan plan tests")
  @ArgumentsSource(PlanTestCaseLoader.class)
  public void testFilterScanPlan(@ToPlannerTestCase PlannerTestCase testCase) {
    init();
    String sql = testCase.getSql();
    System.out.println("sql: " + testCase.getSql());

    SqlNode ast = parseSql(sql);

    BatchPlan plan = batchPlanner.plan(ast, plannerContext);

    System.out.println("plan: " + plan.getRoot());
  }

  @Override
  public SchemaPlus initSchema() {
    catalogService.createDatabase("db");
    catalogService.createSchema("db", "sc");

    RelDataTypeFactory dataTypeFactory = new RisingWaveTypeFactory();
    RisingWaveDataType integerType =
        (RisingWaveDataType) dataTypeFactory.createSqlType(SqlTypeName.INTEGER);

    CreateTableInfo createTableInfo =
        CreateTableInfo.builder("table1")
            .addColumn("a", new ColumnDesc(integerType))
            .addColumn("b", new ColumnDesc(integerType))
            .build();

    catalogService.createTable("db", "sc", createTableInfo);
    return catalogService.getSchemaChecked("db", "sc").plus();
  }
}
