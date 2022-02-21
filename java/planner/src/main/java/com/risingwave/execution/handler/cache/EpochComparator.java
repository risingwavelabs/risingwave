package com.risingwave.execution.handler.cache;

import java.util.Comparator;

/** Compare epoch as unsigned long. */
public class EpochComparator implements Comparator<Long> {

  @Override
  public int compare(Long o1, Long o2) {
    return Long.compareUnsigned(o1, o2);
  }
}
