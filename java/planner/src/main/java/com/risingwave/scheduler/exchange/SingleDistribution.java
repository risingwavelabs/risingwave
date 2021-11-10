package com.risingwave.scheduler.exchange;

import com.google.common.base.Verify;
import com.risingwave.proto.plan.ExchangeInfo;

/** Used by tasks of root stage. */
public class SingleDistribution implements Distribution {
  @Override
  public ExchangeInfo toExchangeInfo(int outputCount) {
    Verify.verify(outputCount == 1, "Single Distribution must have only one output");
    return ExchangeInfo.newBuilder().setMode(ExchangeInfo.DistributionMode.SINGLE).build();
  }
}
