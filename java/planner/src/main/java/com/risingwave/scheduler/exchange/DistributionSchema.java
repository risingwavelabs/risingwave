package com.risingwave.scheduler.exchange;

import com.risingwave.proto.plan.ExchangeInfo;

/** Describe distribution schema of stage output. */
public interface DistributionSchema {
  ExchangeInfo toExchangeInfo();
}
