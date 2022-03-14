package com.risingwave.scheduler.exchange;

import com.google.common.collect.ImmutableList;
import com.risingwave.proto.plan.ExchangeInfo;

/** HashDistribution */
public class HashDistribution implements Distribution {

  private final ImmutableList<Integer> keys;

  public HashDistribution(ImmutableList<Integer> keys) {
    this.keys = keys;
  }

  @Override
  public ExchangeInfo toExchangeInfo(int outputCount) {
    var hashInfo = ExchangeInfo.HashInfo.newBuilder()
            .addAllKeys(this.keys)
            .setOutputCount(outputCount)
            .build();
    return ExchangeInfo.newBuilder()
        .setMode(ExchangeInfo.DistributionMode.HASH)
        .setHashInfo(hashInfo)
        .build();
  }
}
