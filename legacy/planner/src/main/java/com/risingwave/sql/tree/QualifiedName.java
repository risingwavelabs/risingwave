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

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import com.google.common.base.Joiner;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.StreamSupport;

public class QualifiedName {

  private final List<String> parts;

  public static QualifiedName of(String first, String... rest) {
    requireNonNull(first, "first is null");
    ArrayList<String> parts = new ArrayList<>();
    parts.add(first);
    parts.addAll(Arrays.asList(rest));
    return new QualifiedName(parts);
  }

  public static QualifiedName of(Iterable<String> parts) {
    return new QualifiedName(parts);
  }

  public QualifiedName(String name) {
    this(Collections.singletonList(name));
  }

  public QualifiedName(Iterable<String> parts) {
    List<String> partsList = StreamSupport.stream(parts.spliterator(), false).collect(toList());
    if (partsList.isEmpty()) {
      throw new IllegalArgumentException("parts is empty");
    }
    this.parts = partsList;
  }

  public List<String> getParts() {
    return parts;
  }

  @Override
  public String toString() {
    return Joiner.on('.').join(parts);
  }

  /**
   * For an identifier of the form "a.b.c.d", returns "a.b.c" For an identifier of the form "a",
   * returns absent
   */
  public Optional<QualifiedName> getPrefix() {
    if (parts.size() == 1) {
      return Optional.empty();
    }

    return Optional.of(QualifiedName.of(parts.subList(0, parts.size() - 1)));
  }

  public String getSuffix() {
    return parts.get(parts.size() - 1);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    QualifiedName that = (QualifiedName) o;
    return Objects.equals(parts, that.parts);
  }

  @Override
  public int hashCode() {
    return Objects.hash(parts);
  }
}
