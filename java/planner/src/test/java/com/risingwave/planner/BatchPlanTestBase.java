package com.risingwave.planner;

import static com.google.common.base.Preconditions.checkArgument;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.risingwave.planner.planner.batch.BatchPlanner;
import com.risingwave.planner.rel.physical.batch.BatchPlan;
import com.risingwave.planner.rel.physical.batch.RisingWaveBatchPhyRel;
import com.risingwave.planner.rel.serialization.ExplainWriter;
import com.risingwave.planner.util.PlannerTestCase;
import com.risingwave.planner.util.PlannerTestDdlLoader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import org.apache.calcite.rel.RelNode;
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
      sqlHandlerFactory.create(ddlSql, executionContext).handle(ddlSql, executionContext);
    }
  }

  protected void runTestCase(PlannerTestCase testCase) {
    String sql = testCase.getSql();

    SqlNode ast = parseSql(sql);
    BatchPlan plan = batchPlanner.plan(ast, executionContext);

    String explainedPlan = explainBatchPlan(plan.getRoot());
    assertEquals(testCase.getPlan(), explainedPlan, "Plan not match!");

    // Comment out this to wait for all plan serializaton to be ready
    if (testCase.getJson() != null) {
      String serializedJsonPlan = serializePlanToJson(plan.getRoot());
      assertEquals(testCase.getJson(), serializedJsonPlan, "Json not match!");
    }
  }

  private static String explainBatchPlan(RelNode relNode) {
    try (StringWriter sw = new StringWriter();
        PrintWriter printer = new PrintWriter(sw); ) {
      ExplainWriter writer = new ExplainWriter(printer);
      relNode.explain(writer);
      return sw.toString().trim();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static String serializePlanToJson(RelNode relNode) {
    checkArgument(relNode instanceof RisingWaveBatchPhyRel, "relNode is not batch physical plan!");
    RisingWaveBatchPhyRel batchPhyRel = (RisingWaveBatchPhyRel) relNode;

    try {
      return JsonFormat.printer()
          .usingTypeRegistry(PROTOBUF_JSON_TYPE_REGISTRY)
          .print(batchPhyRel.serialize());
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Failed to serialize pan to json.", e);
    }
  }
}
