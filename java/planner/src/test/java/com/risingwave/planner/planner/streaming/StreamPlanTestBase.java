package com.risingwave.planner.planner.streaming;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.risingwave.common.config.LeaderServerConfigurations;
import com.risingwave.execution.handler.CreateMaterializedViewHandler;
import com.risingwave.planner.SqlTestBase;
import com.risingwave.planner.rel.physical.streaming.StreamingPlan;
import com.risingwave.planner.rel.serialization.ExplainWriter;
import com.risingwave.planner.rel.serialization.StreamingStageSerializer;
import com.risingwave.planner.util.PlannerTestCase;
import com.risingwave.planner.util.PlannerTestDdlLoader;
import com.risingwave.proto.streaming.plan.StreamNode;
import com.risingwave.rpc.Messages;
import com.risingwave.scheduler.streaming.StreamFragmenter;
import java.util.List;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateMaterializedView;

/** Test base for stream plan. */
public abstract class StreamPlanTestBase extends SqlTestBase {
  protected StreamPlanner streamPlanner;
  protected StreamFragmenter streamFragmenter;

  protected void init() {
    super.initEnv();
    executionContext
        .getConf()
        .set(
            LeaderServerConfigurations.CLUSTER_MODE,
            LeaderServerConfigurations.ClusterMode.Distributed);
    initTables();
    streamPlanner = new StreamPlanner(true);
    streamFragmenter = new StreamFragmenter();
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
    assertEquals(testCase.getPlan().get(), explainedPlan, "Plans do not match!");

    // Do not error if no primary key test.
    // TODO: Finally all should have primary key test, but we gradually support it
    if (testCase.getPrimaryKey().isPresent()) {
      String primaryKey = testCase.getPrimaryKey().get();
      assertEquals(
          primaryKey,
          plan.getStreamingPlan().getPrimaryKeyIndices().toString(),
          "Primary key not match!");
    }

    // We must pass the stream plan through MV handler to assign a TableId.
    CreateMaterializedViewHandler handler = new CreateMaterializedViewHandler();
    SqlCreateMaterializedView createMaterializedView = (SqlCreateMaterializedView) ast;
    String tableName = createMaterializedView.name.getSimple();
    handler.convertPlanToCatalog(tableName, plan, executionContext);

    // Do not error if no json test.
    if (testCase.getJson().isPresent()) {
      StreamNode serializedProto = StreamingStageSerializer.serialize(plan.getStreamingPlan());
      String serializedJsonPlan = Messages.jsonFormat(serializedProto);
      assertEquals(testCase.getJson().get(), serializedJsonPlan, "Proto json not match!");
    }
  }
}
