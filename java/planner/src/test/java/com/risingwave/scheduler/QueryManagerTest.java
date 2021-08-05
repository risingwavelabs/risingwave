package com.risingwave.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.risingwave.node.EndPoint;
import com.risingwave.planner.rel.physical.batch.BatchPlan;
import com.risingwave.planner.rel.physical.batch.RisingWaveBatchPhyRel;
import com.risingwave.proto.plan.PlanNode;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class QueryManagerTest {

  @Test
  @DisplayName("Schedule one task")
  public void testGetResultLocation()
      throws ExecutionException, InterruptedException, TimeoutException {
    Injector injector = Guice.createInjector(new TestSchedulerModule());

    QueryManager queryManager = injector.getInstance(QueryManager.class);

    QueryResultLocation resultLocation =
        queryManager.schedule(createBatchPlan()).get(10, TimeUnit.SECONDS);

    assertEquals(
        new EndPoint("localhost", 1234),
        resultLocation.getNodeEndPoint(),
        "Result endpoint not match!");
  }

  private static BatchPlan createBatchPlan() {
    PlanNode planNode =
        PlanNode.newBuilder().setNodeType(PlanNode.PlanNodeType.CREATE_TABLE).build();

    RisingWaveBatchPhyRel root = mock(RisingWaveBatchPhyRel.class);
    when(root.serialize()).thenReturn(planNode);
    BatchPlan batchPlan = mock(BatchPlan.class);
    when(batchPlan.getRoot()).thenReturn(root);

    return batchPlan;
  }
}
