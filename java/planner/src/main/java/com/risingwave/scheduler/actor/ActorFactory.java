package com.risingwave.scheduler.actor;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;

public interface ActorFactory {
  <T> ActorRef<T> createActor(Behavior<T> behavior, String name);
}
