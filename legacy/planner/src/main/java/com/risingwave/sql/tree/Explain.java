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

import java.util.Objects;

public class Explain extends Statement {

  private final Statement statement;
  private final boolean analyze;

  public Explain(Statement statement, boolean analyze) {
    this.statement = requireNonNull(statement, "statement is null");
    this.analyze = analyze;
  }

  public Statement getStatement() {
    return statement;
  }

  public boolean isAnalyze() {
    return analyze;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitExplain(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Explain explain = (Explain) o;
    return analyze == explain.analyze && Objects.equals(statement, explain.statement);
  }

  @Override
  public int hashCode() {
    return Objects.hash(statement, analyze);
  }

  @Override
  public String toString() {
    return "Explain{" + "statement=" + statement + ", analyze=" + analyze + '}';
  }
}
