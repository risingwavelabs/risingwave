package com.risingwave.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.risingwave.planner.planner.batch.BatchPlanner;
import com.risingwave.planner.rel.physical.batch.BatchPlan;
import com.risingwave.planner.rel.serialization.ExplainWriter;
import com.risingwave.planner.util.PlannerTestCase;
import com.risingwave.planner.util.PlannerTestDdlLoader;
import com.risingwave.rpc.Messages;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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

    // Do not error if no json test.
    if (testCase.getJson().isPresent()) {
      try {
        String serializedJsonPlan = Messages.jsonFormat(plan.getRoot().serialize());
        String ans =
            Files.readString(
                Path.of(
                    getClass()
                        .getClassLoader()
                        .getResource(jsonFilesPathPrefix + testCase.getJson().get() + ".json")
                        .toURI()),
                StandardCharsets.UTF_8);
        assertEquals(ans, serializedJsonPlan, "Json not match!");
      } catch (Exception e) {
        throw new RuntimeException("Json load fail");
      }
    }
  }
}
