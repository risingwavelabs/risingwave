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

import com.risingwave.common.collections.Lists2;
import java.util.List;
import java.util.Objects;

public class SetTransactionStatement extends Statement {

  public interface TransactionMode {}

  public enum IsolationLevel implements TransactionMode {
    SERIALIZABLE,
    REPEATABLE_READ,
    READ_COMMITTED,
    READ_UNCOMMITTED;
  }

  public enum ReadMode implements TransactionMode {
    READ_WRITE,
    READ_ONLY;
  }

  public static class Deferrable implements TransactionMode {

    private final boolean not;

    public Deferrable(boolean not) {
      this.not = not;
    }

    @Override
    public String toString() {
      return not ? "NOT DEFERRABLE" : "DEFERRABLE";
    }
  }

  private final List<TransactionMode> transactionModes;

  public SetTransactionStatement(List<TransactionMode> transactionModes) {
    this.transactionModes = transactionModes;
  }

  public List<TransactionMode> transactionModes() {
    return transactionModes;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(transactionModes);
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof SetTransactionStatement
        && ((SetTransactionStatement) obj).transactionModes.equals(this.transactionModes);
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return (R) visitor.visitSetTransaction(this, context);
  }

  @Override
  public String toString() {
    return "SET TRANSACTION " + Lists2.joinOn(", ", transactionModes, TransactionMode::toString);
  }
}
