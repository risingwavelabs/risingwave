package com.risingwave.planner;

import com.risingwave.planner.planner.streaming.StreamPlanner;
import com.risingwave.planner.rel.serialization.StreamPlanSerializer;
import com.risingwave.planner.util.PlannerTestDdlLoader;
import java.util.List;
import org.apache.calcite.sql.SqlNode;

public abstract class StreamPlanTestBase extends SqlTestBase {
  protected StreamPlanner streamPlanner;
  protected StreamPlanSerializer serializer;

  protected void init() {
    super.initEnv();

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
}
