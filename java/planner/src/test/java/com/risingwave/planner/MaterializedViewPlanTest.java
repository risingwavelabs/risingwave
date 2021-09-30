package com.risingwave.planner;

import com.risingwave.execution.handler.CreateMaterializedViewHandler;
import com.risingwave.planner.rel.physical.streaming.StreamingPlan;
import com.risingwave.planner.rel.serialization.ExplainWriter;
import com.risingwave.planner.util.PlanTestCaseLoader;
import com.risingwave.planner.util.PlannerTestCase;
import com.risingwave.planner.util.ToPlannerTestCase;
import com.risingwave.proto.streaming.plan.StreamNode;
import com.risingwave.rpc.Messages;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateMaterializedView;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class MaterializedViewPlanTest extends StreamPlanTestBase {
  @BeforeAll
  public void initAll() {
    super.init();
  }

  @Test
  void testStreamPlanCase1() {
    // A sample code for stream testing. Keep this for debugging.
    String sql = "create materialized view T_test as select sum(v1)+1 as V from t where v1>v2";
    System.out.println("query sql: \n" + sql);
    SqlNode ast = parseSql(sql);
    StreamingPlan plan = streamPlanner.plan(ast, executionContext);
    String explainPlan = ExplainWriter.explainPlan(plan.getStreamingPlan());

    CreateMaterializedViewHandler handler = new CreateMaterializedViewHandler();
    SqlCreateMaterializedView createMaterializedView = (SqlCreateMaterializedView) ast;
    String tableName = createMaterializedView.name.getSimple();
    handler.convertPlanToCatalog(tableName, plan, executionContext);

    StreamNode serializedProto = serializer.serialize(plan);
    String serializedJsonPlan = Messages.jsonFormat(serializedProto);
    System.out.println(serializedJsonPlan);
  }

  @ParameterizedTest(name = "{index} => {0}")
  @DisplayName("Streaming plan tests")
  @ArgumentsSource(PlanTestCaseLoader.class)
  public void testStreamingPlan(@ToPlannerTestCase PlannerTestCase testCase) {
    // Comment the line below if you want to make quick proto changes and this test block the
    // progress.
    runTestCase(testCase);
  }
}
