package com.risingwave.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.risingwave.common.config.LeaderServerConfigurations;
import com.risingwave.execution.handler.CreateMaterializedViewHandler;
import com.risingwave.planner.planner.streaming.StreamPlanner;
import com.risingwave.planner.rel.physical.streaming.StreamingPlan;
import com.risingwave.planner.rel.serialization.ExplainWriter;
import com.risingwave.planner.rel.serialization.StreamPlanSerializer;
import com.risingwave.planner.util.PlannerTestCase;
import com.risingwave.planner.util.PlannerTestDdlLoader;
import com.risingwave.proto.streaming.plan.StreamNode;
import com.risingwave.rpc.Messages;
import java.util.List;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateMaterializedView;

public abstract class StreamPlanTestBase extends SqlTestBase {
  protected StreamPlanner streamPlanner;
  protected StreamPlanSerializer serializer;

  protected void init() {
    super.initEnv();
    executionContext
        .getConf()
        .set(
            LeaderServerConfigurations.CLUSTER_MODE,
            LeaderServerConfigurations.ClusterMode.Distributed);
    initTables();
    streamPlanner = new StreamPlanner();
    serializer = new StreamPlanSerializer(executionContext);
  }

  private void initTables() {
    List<String> ddls = PlannerTestDdlLoader.load(getClass());

    for (String ddl : ddls) {
      System.out.println("create table ddl: " + ddl);
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
    StreamingPlan plan = streamPlanner.plan(ast, executionContext);

    String explainedPlan = ExplainWriter.explainPlan(plan.getStreamingPlan());
    assertEquals(testCase.getPlan(), explainedPlan, "Plans do not match!");

    // We must pass the stream plan through MV handler to assign a TableId.
    CreateMaterializedViewHandler handler = new CreateMaterializedViewHandler();
    SqlCreateMaterializedView createMaterializedView = (SqlCreateMaterializedView) ast;
    String tableName = createMaterializedView.name.getSimple();
    handler.convertPlanToCatalog(tableName, plan, executionContext);

    // Do not error if no json test.
    if (testCase.getJson() != null) {
      StreamNode serializedProto = serializer.serialize(plan);
      String serializedJsonPlan = Messages.jsonFormat(serializedProto);
      assertEquals(testCase.getJson(), serializedJsonPlan, "Proto json not match!");
    }
  }
}
