package com.risingwave.scheduler.actor;

import static java.util.Objects.requireNonNull;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.Props;
import akka.actor.typed.SpawnProtocol;
import akka.actor.typed.javadsl.AskPattern;
import com.risingwave.common.exception.PgErrorCode;
import com.risingwave.common.exception.PgException;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class DefaultActorFactory implements ActorFactory {
  private final ActorSystem<SpawnProtocol.Command> akkaSystem;

  @Inject
  public DefaultActorFactory(ActorSystem<SpawnProtocol.Command> akkaSystem) {
    this.akkaSystem = requireNonNull(akkaSystem, "akkaSystem");
  }

  @Override
  public <T> ActorRef<T> createActor(Behavior<T> behavior, String name) {
    try {
      CompletionStage<ActorRef<T>> result =
          AskPattern.ask(
              akkaSystem,
              replyTo -> new SpawnProtocol.Spawn<>(behavior, name, Props.empty(), replyTo),
              Duration.ofSeconds(3),
              akkaSystem.scheduler());

      return result.toCompletableFuture().get(3, TimeUnit.SECONDS);
    } catch (Exception e) {
      throw new PgException(PgErrorCode.INTERNAL_ERROR, e);
    }
  }
}
