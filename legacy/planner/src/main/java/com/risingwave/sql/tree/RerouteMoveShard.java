/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package com.risingwave.sql.tree;

import java.util.Objects;
import java.util.function.Function;

public class RerouteMoveShard<T> extends RerouteOption {

  private final T shardId;
  private final T fromNodeIdOrName;
  private final T toNodeIdOrName;

  public RerouteMoveShard(T shardId, T fromNodeIdOrName, T toNodeIdOrName) {
    this.shardId = shardId;
    this.fromNodeIdOrName = fromNodeIdOrName;
    this.toNodeIdOrName = toNodeIdOrName;
  }

  public T shardId() {
    return shardId;
  }

  public T fromNodeIdOrName() {
    return fromNodeIdOrName;
  }

  public T toNodeIdOrName() {
    return toNodeIdOrName;
  }

  public <U> RerouteMoveShard<U> map(Function<? super T, ? extends U> mapper) {
    return new RerouteMoveShard<>(
        mapper.apply(shardId), mapper.apply(fromNodeIdOrName), mapper.apply(toNodeIdOrName));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RerouteMoveShard<?> that = (RerouteMoveShard<?>) o;
    return Objects.equals(shardId, that.shardId)
        && Objects.equals(fromNodeIdOrName, that.fromNodeIdOrName)
        && Objects.equals(toNodeIdOrName, that.toNodeIdOrName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(shardId, fromNodeIdOrName, toNodeIdOrName);
  }

  @Override
  public String toString() {
    return "RerouteMoveShard{"
        + "shardId="
        + shardId
        + ", fromNodeId="
        + fromNodeIdOrName
        + ", toNodeId="
        + toNodeIdOrName
        + '}';
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitRerouteMoveShard(this, context);
  }
}
