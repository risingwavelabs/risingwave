package com.risingwave.common.util;

import java.util.concurrent.Callable;
import java.util.function.Consumer;

public class Utils {
  public static <T> T runWithTime(Callable<T> callable, Consumer<Long> timeConsumer)
      throws Exception {
    long current = System.nanoTime();
    try {
      return callable.call();
    } finally {
      timeConsumer.accept(System.nanoTime() - current);
    }
  }
}
