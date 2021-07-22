package com.risingwave.common.entity;

import static java.util.Objects.requireNonNull;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class EntityBase<IdT extends PathLike<Integer>, NameT extends PathLike<String>> {
  private final IdT id;
  private final NameT entityName;

  public EntityBase(IdT id, NameT entityName) {
    this.id = requireNonNull(id, "id can't be null!");
    this.entityName = requireNonNull(entityName, "entity name can't be null!");
  }

  public IdT getId() {
    return id;
  }

  public NameT getEntityName() {
    return entityName;
  }

  public static <K, E extends EntityBase<?, ?>> ConcurrentMap<K, E> groupBy(
      Collection<E> entities, Function<E, K> keyMapping) {
    return entities.stream().collect(Collectors.toConcurrentMap(keyMapping, Function.identity()));
  }
}
