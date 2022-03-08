package com.risingwave.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.risingwave.planner.planner.batch.BatchPlanner;
import com.risingwave.planner.program.OptimizerProgram;
import com.risingwave.planner.rel.physical.BatchPlan;
import com.risingwave.planner.rel.serialization.ExplainWriter;
import com.risingwave.planner.util.PlannerTestCase;
import com.risingwave.planner.util.PlannerTestDdlLoader;
import com.risingwave.rpc.Messages;
import java.util.List;
import org.apache.calcite.sql.SqlNode;

/** Test base for Batch plan. */
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
      baseLogger.debug("create table sql: " + ddl);
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
    BatchPlan phyPlan = batchPlanner.planPhysical(ast, executionContext);
    ast = parseSql(sql);
    BatchPlan distPlan = batchPlanner.planDistributed(ast, executionContext);

    if (testCase.getPlan().isPresent()) {
      String explainedPlan = ExplainWriter.explainPlan(phyPlan.getRoot());
      assertEquals(testCase.getPlan().get(), explainedPlan, "Plan not match!");
    }

    if (testCase.getPhyPlan().isPresent()) {
      String explainedPlan = ExplainWriter.explainPlan(phyPlan.getRoot());
      assertEquals(testCase.getPhyPlan().get(), explainedPlan, "Plan not match!");
    }

    if (testCase.getDistPlan().isPresent()) {
      String explainedPlan = ExplainWriter.explainPlan(distPlan.getRoot());
      assertEquals(testCase.getDistPlan().get(), explainedPlan, "Plan not match!");
    }

    if (testCase.getJson().isPresent()) {
      // We still assume that the parsed json test case contains json value only.
      String serializedJsonPlan = Messages.jsonFormat(phyPlan.getRoot().serialize());
      String ans = testCase.getJson().get().stripTrailing();
      assertEquals(ans, serializedJsonPlan, "Json not match!");
    }
  }

  protected void runTestCaseWithProgram(PlannerTestCase testCase, OptimizerProgram program) {
    String sql = testCase.getSql();

    SqlNode ast = parseSql(sql);

    verifyPlanWithProgram(testCase, ast, program);
  }

  protected void verifyPlanWithProgram(
      PlannerTestCase testCase, SqlNode ast, OptimizerProgram program) {
    var root = batchPlanner.plan(ast, executionContext, (c, relCollation) -> program);
    if (testCase.getPlan().isPresent()) {
      String explainedPlan = ExplainWriter.explainPlan(root);
      assertEquals(testCase.getPlan().get(), explainedPlan, "Plan not match!");
    }
  }
}
