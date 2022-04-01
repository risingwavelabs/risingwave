package com.risingwave.scheduler.exchange;

import com.risingwave.proto.plan.ExchangeInfo;

/** BroadcastDistribution */
public class BroadcastDistribution implements Distribution {

  @Override
  public ExchangeInfo toExchangeInfo(int outputCount) {
    var broadcastInfo = ExchangeInfo.BroadcastInfo.newBuilder().setCount(outputCount).build();
    return ExchangeInfo.newBuilder()
        .setMode(ExchangeInfo.DistributionMode.BROADCAST)
        .setBroadcastInfo(broadcastInfo)
        .build();
  }
}
