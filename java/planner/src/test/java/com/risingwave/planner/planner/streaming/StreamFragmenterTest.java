package com.risingwave.planner.planner.streaming;

import com.risingwave.common.config.LeaderServerConfigurations;
import com.risingwave.execution.handler.CreateMaterializedViewHandler;
import com.risingwave.planner.rel.serialization.ExplainWriter;
import com.risingwave.planner.rel.serialization.StreamingStageSerializer;
import com.risingwave.planner.rel.streaming.StreamingPlan;
import com.risingwave.rpc.Messages;
import java.util.List;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateMaterializedView;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests for stream fragmenter. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class StreamFragmenterTest extends StreamPlanTestBase {
  private final Logger testLogger = LoggerFactory.getLogger(StreamFragmenterTest.class);

  @BeforeAll
  public void initAll() {
    super.init();
    executionContext
        .getConf()
        .set(
            LeaderServerConfigurations.CLUSTER_MODE, LeaderServerConfigurations.ClusterMode.Single);
  }

  @Test
  @Order(1)
  void testStreamingFragmenter() {
    executionContext
        .getConf()
        .set(
            LeaderServerConfigurations.CLUSTER_MODE,
            LeaderServerConfigurations.ClusterMode.Distributed);
    // A sample code copied from MaterializedViewPlanTest.testDistributedStreamingPlan().
    String sql =
        "create materialized view T_distributed as select sum(v1)+1 as V from t where v1>v2";

    // Generate distributed plan.
    SqlNode ast = parseSql(sql);
    StreamingPlan plan = streamPlanner.plan(ast, executionContext);

    // Generate storage catalog.
    CreateMaterializedViewHandler handler = new CreateMaterializedViewHandler();
    SqlCreateMaterializedView createMaterializedView = (SqlCreateMaterializedView) ast;
    String tableName = createMaterializedView.name.getSimple();
    handler.convertPlanToCatalog(tableName, plan, executionContext);

    // A sample plan.
    String explainExchangePlan = ExplainWriter.explainPlan(plan.getStreamingPlan());
    String expectedPlan =
        "RwStreamMaterializedView(name=[t_distributed])\n"
            + "  RwStreamProject(v=[+($STREAM_NULL_BY_ROW_COUNT($0, $1), 1)], $f0_copy=[$0], $f1_copy=[$1])\n"
            + "    RwStreamAgg(group=[{}], agg#0=[$SUM0($0)], agg#1=[SUM($1)])\n"
            + "      RwStreamExchange(distribution=[RwDistributionTrait{type=SINGLETON, keys=[]}], collation=[[]])\n"
            + "        RwStreamAgg(group=[{}], agg#0=[COUNT()], agg#1=[SUM($0)])\n"
            + "          RwStreamFilter(condition=[>($0, $1)])\n"
            + "            RwStreamExchange(distribution=[RwDistributionTrait{type=HASH_DISTRIBUTED, keys=[0]}], collation=[[]])\n"
            + "              RwStreamTableSource(table=[[test_schema, t]], columns=[v1,v2])";
    Assertions.assertEquals(expectedPlan, explainExchangePlan);

    // Test building stages in the fragmenter.
    String expectedStage0 =
        "RwStreamMaterializedView(name=[t_distributed])\n"
            + "  RwStreamProject(v=[+($STREAM_NULL_BY_ROW_COUNT($0, $1), 1)], $f0_copy=[$0], $f1_copy=[$1])\n"
            + "    RwStreamAgg(group=[{}], agg#0=[$SUM0($0)], agg#1=[SUM($1)])";
    String expectedStage1 =
        "RwStreamExchange(distribution=[RwDistributionTrait{type=SINGLETON, keys=[]}], collation=[[]])\n"
            + "  RwStreamAgg(group=[{}], agg#0=[COUNT()], agg#1=[SUM($0)])\n"
            + "    RwStreamFilter(condition=[>($0, $1)])";
    String expectedStage2 =
        "RwStreamExchange(distribution=[RwDistributionTrait{type=HASH_DISTRIBUTED, keys=[0]}], collation=[[]])\n"
            + "  RwStreamTableSource(table=[[test_schema, t]], columns=[v1,v2])";
    String[] expectedStages = {expectedStage0, expectedStage1, expectedStage2};
    var rootStage = streamFragmenter.generateStageGraph(plan, executionContext);
    var stages = rootStage.getStages();
    for (var stage : stages) {
      testLogger.debug("id:" + stage.getStageId());
      String actualStage = StreamingStageSerializer.explainStage(stage.getRoot(), 0);
      testLogger.debug("explain:\n" + actualStage);
      Assertions.assertEquals(expectedStages[stage.getStageId()], actualStage);
    }

    // Test building streaming DAG from the stage graph.
    var streamGraph = streamFragmenter.buildGraphFromStageGraph(rootStage, executionContext);
    var fragments = streamGraph.getAllFragments();
    List<String> expectedFragments = StreamFragmenterTestLoader.loadSample(getClass());

    for (int i = 0; i < fragments.size(); i++) {
      var fragment = fragments.get(i);
      String serializedFragment = Messages.jsonFormat(fragment.serialize());
      String expectedFragment = expectedFragments.get(i).trim();
      testLogger.debug("serialized fragment:\n" + serializedFragment);
      Assertions.assertEquals(expectedFragment, serializedFragment);
    }
  }
}
