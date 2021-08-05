package com.risingwave.scheduler;

public interface EventListener<T> {
  void onEvent(T event);
}
