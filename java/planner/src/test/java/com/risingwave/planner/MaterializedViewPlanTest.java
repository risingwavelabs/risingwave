package com.risingwave.planner;

import com.risingwave.common.config.LeaderServerConfigurations;
import com.risingwave.execution.handler.CreateMaterializedViewHandler;
import com.risingwave.planner.planner.batch.BatchPlanner;
import com.risingwave.planner.planner.streaming.StreamFragmenter;
import com.risingwave.planner.rel.physical.batch.BatchPlan;
import com.risingwave.planner.rel.physical.streaming.StreamingPlan;
import com.risingwave.planner.rel.serialization.ExplainWriter;
import com.risingwave.planner.util.PlanTestCaseLoader;
import com.risingwave.planner.util.PlannerTestCase;
import com.risingwave.planner.util.ToPlannerTestCase;
import com.risingwave.proto.streaming.plan.StreamNode;
import com.risingwave.rpc.Messages;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateMaterializedView;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/** Tests for streaming job */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MaterializedViewPlanTest extends StreamPlanTestBase {
  @BeforeAll
  public void initAll() {
    super.init();
    executionContext
        .getConf()
        .set(
            LeaderServerConfigurations.CLUSTER_MODE, LeaderServerConfigurations.ClusterMode.Single);
  }

  @ParameterizedTest(name = "{index} => {0}")
  @DisplayName("Streaming plan tests")
  @ArgumentsSource(PlanTestCaseLoader.class)
  @Order(0)
  public void testStreamingPlan(@ToPlannerTestCase PlannerTestCase testCase) {
    // Comment the line below if you want to make quick proto changes and this test block the
    // progress.
    runTestCase(testCase);
  }

  @Test
  @Order(1)
  void testCreateMaterializedView() {
    // A sample code for stream testing. Keep this for debugging.
    String sql = "create materialized view T_test as select sum(v1)+1 as V from t where v1>v2";
    SqlNode ast = parseSql(sql);
    StreamingPlan plan = streamPlanner.plan(ast, executionContext);
    String resultPlan = ExplainWriter.explainPlan(plan.getStreamingPlan());
    Assertions.assertEquals(
        "RwStreamMaterializedView(v=[$0])\n"
            + "  RwStreamProject(v=[+($STREAM_NULL_BY_ROW_COUNT($1, $0), 1)])\n"
            + "    RwStreamAgg(group=[{}], agg#0=[SUM($0)], Row Count=[COUNT()])\n"
            + "      RwStreamFilter(condition=[>($0, $1)])\n"
            + "        RwStreamTableSource(table=[[test_schema, t]], columns=[v1,v2])",
        resultPlan);

    CreateMaterializedViewHandler handler = new CreateMaterializedViewHandler();
    SqlCreateMaterializedView createMaterializedView = (SqlCreateMaterializedView) ast;
    String tableName = createMaterializedView.name.getSimple();
    handler.convertPlanToCatalog(tableName, plan, executionContext);

    StreamNode serializedProto = plan.getStreamingPlan().serialize();
    String serializedJsonPlan = Messages.jsonFormat(serializedProto);
    // System.out.println(serializedJsonPlan);
  }

  @Test
  @Order(2)
  void testSelectFromMaterializedView() {
    String sql = "select * from T_test"; // T_test was created in previous case

    BatchPlanner batchPlanner = new BatchPlanner();
    BatchPlan plan = batchPlanner.plan(parseSql(sql), executionContext);
    String explain = ExplainWriter.explainPlan(plan.getRoot());

    Assertions.assertTrue(explain.contains("RwBatchMaterializedViewScan"));
  }

  @Test
  @Order(3)
  void testDistributedStreamingPlan() {
    executionContext
        .getConf()
        .set(
            LeaderServerConfigurations.CLUSTER_MODE,
            LeaderServerConfigurations.ClusterMode.Distributed);
    // A sample code for distributed streaming plan.
    String sql =
        "create materialized view T_distributed as select sum(v1)+1 as V from t where v1>v2";

    SqlNode ast = parseSql(sql);
    StreamingPlan plan = streamPlanner.plan(ast, executionContext);

    CreateMaterializedViewHandler handler = new CreateMaterializedViewHandler();
    SqlCreateMaterializedView createMaterializedView = (SqlCreateMaterializedView) ast;
    String tableName = createMaterializedView.name.getSimple();
    handler.convertPlanToCatalog(tableName, plan, executionContext);

    StreamingPlan exchangePlan = StreamFragmenter.generateGraph(plan, executionContext);
    String explainExchangePlan = ExplainWriter.explainPlan(exchangePlan.getStreamingPlan());
    String expectedPlan =
        "RwStreamMaterializedView(v=[$0])\n"
            + "  RwStreamProject(v=[+($STREAM_NULL_BY_ROW_COUNT($1, $0), 1)])\n"
            + "    RwStreamAgg(group=[{}], agg#0=[SUM($0)], Row Sum0=[$SUM0($1)])\n"
            + "      RwStreamExchange(distribution=[RwDistributionTrait{type=SINGLETON, keys=[]}], collation=[[]])\n"
            + "        RwStreamProject($f0=[$STREAM_NULL_BY_ROW_COUNT($1, $0)], Row Count=[$1])\n"
            + "          RwStreamAgg(group=[{}], agg#0=[SUM($0)], Row Count=[COUNT()])\n"
            + "            RwStreamFilter(condition=[>($0, $1)])\n"
            + "              RwStreamExchange(distribution=[RwDistributionTrait{type=HASH_DISTRIBUTED, keys=[0]}], collation=[[]])\n"
            + "                RwStreamTableSource(table=[[test_schema, t]], columns=[v1,v2], dispatched=[true])";
    System.out.println(explainExchangePlan);
    Assertions.assertEquals(expectedPlan, explainExchangePlan);
  }

  @Test
  @Order(4)
  void testDistributedAggPlan() {
    executionContext
        .getConf()
        .set(
            LeaderServerConfigurations.CLUSTER_MODE,
            LeaderServerConfigurations.ClusterMode.Distributed);
    // A sample code for distributed streaming aggregation plan.
    String sql = "create materialized view T_agg as select v1, sum(v2) as V from t group by v1";

    SqlNode ast = parseSql(sql);
    StreamingPlan plan = streamPlanner.plan(ast, executionContext);

    CreateMaterializedViewHandler handler = new CreateMaterializedViewHandler();
    SqlCreateMaterializedView createMaterializedView = (SqlCreateMaterializedView) ast;
    String tableName = createMaterializedView.name.getSimple();
    handler.convertPlanToCatalog(tableName, plan, executionContext);

    StreamingPlan exchangePlan = StreamFragmenter.generateGraph(plan, executionContext);
    String explainExchangePlan = ExplainWriter.explainPlan(exchangePlan.getStreamingPlan());
    String expectedPlan =
        "RwStreamMaterializedView(v1=[$0], v=[$1])\n"
            + "  RwStreamProject(v1=[$0], v=[$1])\n"
            + "    RwStreamFilter(condition=[<>($2, 0)])\n"
            + "      RwStreamAgg(group=[{0}], v=[SUM($1)], Row Sum0=[$SUM0($2)])\n"
            + "        RwStreamExchange(distribution=[RwDistributionTrait{type=HASH_DISTRIBUTED, keys=[0]}], collation=[[]])\n"
            + "          RwStreamProject(v1=[$0], v=[$STREAM_NULL_BY_ROW_COUNT($2, $1)], Row Count=[$2])\n"
            + "            RwStreamAgg(group=[{0}], v=[SUM($1)], Row Count=[COUNT()])\n"
            + "              RwStreamExchange(distribution=[RwDistributionTrait{type=HASH_DISTRIBUTED, keys=[0]}], collation=[[]])\n"
            + "                RwStreamTableSource(table=[[test_schema, t]], columns=[v1,v2], dispatched=[true])";
    System.out.println(explainExchangePlan);
    Assertions.assertEquals(expectedPlan, explainExchangePlan);
  }
}
