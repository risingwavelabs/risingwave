package com.risingwave.scheduler.exchange;

import com.risingwave.proto.plan.ExchangeInfo;

/** Used by tasks of root stage. */
public class SingleDistributionSchema implements DistributionSchema {
  @Override
  public ExchangeInfo toExchangeInfo() {
    return ExchangeInfo.newBuilder().setMode(ExchangeInfo.DistributionMode.SINGLE).build();
  }
}
