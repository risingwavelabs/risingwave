package com.risingwave.catalog;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Base class of catalog container entities, e.g., table, schema and database.
 *
 * <p>In current implementation, the container is mutable. However, we should never expose these
 * methods outside of this package, and mutations of these containers should come from
 * {@link CatalogService}.
 *
 * @param <C> Type of child.
 */
abstract class AbstractNonLeafEntity<C extends BaseEntity> extends BaseEntity {
  private final ConcurrentMap<String, C> childByName;
  private final ConcurrentMap<Integer, C> childById;

  AbstractNonLeafEntity(int id, String name, Collection<C> children) {
    super(id, name);
    childByName = children
        .stream()
        .collect(Collectors.toConcurrentMap(BaseEntity::getName, Function.identity()));

    childById = children
        .stream()
        .collect(Collectors.toConcurrentMap(BaseEntity::getId, Function.identity()));
  }

  C getChildById(int id) {
    return childById.get(id);
  }

  C getChildByName(String name) {
    return childByName.get(name);
  }

  Collection<C> getChildren() {
    return Collections.unmodifiableCollection(childById.values());
  }

  Set<String> getChildrenNames() {
    return Collections.unmodifiableSet(childByName.keySet());
  }
}
