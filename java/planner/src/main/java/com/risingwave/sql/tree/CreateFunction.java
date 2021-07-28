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

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public class CreateFunction<T> extends Statement {

  private final QualifiedName name;
  private final boolean replace;
  private final List<FunctionArgument> arguments;
  private final ColumnType<T> returnType;
  private final T language;
  private final T definition;

  public CreateFunction(
      QualifiedName name,
      boolean replace,
      List<FunctionArgument> arguments,
      ColumnType<T> returnType,
      T language,
      T definition) {
    this.name = name;
    this.replace = replace;
    this.arguments = arguments;
    this.returnType = returnType;
    this.language = language;
    this.definition = definition;
  }

  public QualifiedName name() {
    return name;
  }

  public boolean replace() {
    return replace;
  }

  public List<FunctionArgument> arguments() {
    return arguments;
  }

  public ColumnType<T> returnType() {
    return returnType;
  }

  public T language() {
    return language;
  }

  public T definition() {
    return definition;
  }

  public <U> CreateFunction<U> map(Function<? super T, ? extends U> mapper) {
    return new CreateFunction<>(
        name,
        replace,
        arguments,
        returnType.map(mapper),
        mapper.apply(language),
        mapper.apply(definition));
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitCreateFunction(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final CreateFunction that = (CreateFunction) o;
    return Objects.equals(this.name, that.name)
        && Objects.equals(this.replace, that.replace)
        && Objects.equals(this.arguments, that.arguments)
        && Objects.equals(this.returnType, that.returnType)
        && Objects.equals(this.language, that.language);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, replace, arguments, returnType, language);
  }

  @Override
  public String toString() {
    return "CreateFunction{"
        + "name="
        + name
        + ", replace="
        + replace
        + ", arguments="
        + arguments
        + ", returnType="
        + returnType
        + ", language="
        + language
        + ", definition="
        + definition
        + '}';
  }
}
