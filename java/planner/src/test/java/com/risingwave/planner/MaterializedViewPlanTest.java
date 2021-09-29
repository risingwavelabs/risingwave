package com.risingwave.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.risingwave.planner.rel.physical.streaming.StreamingPlan;
import com.risingwave.planner.rel.serialization.ExplainWriter;
import org.apache.calcite.sql.SqlNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class MaterializedViewPlanTest extends StreamPlanTestBase {
  @BeforeAll
  public void initAll() {
    super.init();
  }

  @Test
  void testStreamPlanCase1() {
    // TODO: implement serializer and migrate this test case to TestBase implementation.
    String sql = "create materialized view T1 as select sum(v2) as V from t where v1>1";
    System.out.println("query sql: \n" + sql);
    SqlNode ast = parseSql(sql);
    StreamingPlan plan = streamPlanner.plan(ast, executionContext);
    String explainPlan = ExplainWriter.explainPlan(plan.getStreamingPlan());

    String expectedPlan =
        "RwStreamMaterializedView\n"
            + "  RwStreamAgg(group=[{}], v=[SUM($0)])\n"
            + "    RwStreamProject(v2=[$1])\n"
            + "      RwStreamFilter(condition=[>($0, 1)])\n"
            + "        RwStreamTableSource(table=[[test_schema, t]])";
    assertEquals(explainPlan, expectedPlan, "Plan not match!");
  }
}
