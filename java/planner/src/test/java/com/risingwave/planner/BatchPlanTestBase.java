package com.risingwave.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.risingwave.planner.planner.batch.BatchPlanner;
import com.risingwave.planner.rel.physical.batch.BatchPlan;
import com.risingwave.planner.rel.serialization.ExplainWriter;
import com.risingwave.planner.util.PlannerTestCase;
import com.risingwave.planner.util.PlannerTestDdlLoader;
import com.risingwave.rpc.Messages;
import java.util.List;
import org.apache.calcite.sql.SqlNode;

public abstract class BatchPlanTestBase extends SqlTestBase {

  protected BatchPlanner batchPlanner;

  protected void init() {
    super.initEnv();

    initTables();
    batchPlanner = new BatchPlanner();
  }

  private void initTables() {
    List<String> ddls = PlannerTestDdlLoader.load(getClass());

    for (String ddl : ddls) {
      System.out.println("sql: " + ddl);
      SqlNode ddlSql = parseDdl(ddl);
      executionContext
          .getSqlHandlerFactory()
          .create(ddlSql, executionContext)
          .handle(ddlSql, executionContext);
    }
  }

  protected void runTestCase(PlannerTestCase testCase) {
    String sql = testCase.getSql();

    SqlNode ast = parseSql(sql);
    BatchPlan plan = batchPlanner.plan(ast, executionContext);

    String explainedPlan = ExplainWriter.explainPlan(plan.getRoot());
    assertEquals(testCase.getPlan(), explainedPlan, "Plan not match!");

    // Comment out this to wait for all plan serializaton to be ready
    if (testCase.getJson() != null) {
      String serializedJsonPlan = Messages.jsonFormat(plan.getRoot().serialize());
      assertEquals(testCase.getJson(), serializedJsonPlan, "Json not match!");
    }
  }
}
