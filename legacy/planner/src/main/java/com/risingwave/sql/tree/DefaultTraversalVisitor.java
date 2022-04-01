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

public abstract class DefaultTraversalVisitor<R, C> extends AstVisitor<R, C> {

  @Override
  protected R visitExtract(Extract node, C context) {
    return node.getExpression().accept(this, context);
  }

  @Override
  protected R visitCast(Cast node, C context) {
    return node.getExpression().accept(this, context);
  }

  @Override
  protected R visitTryCast(TryCast node, C context) {
    return node.getExpression().accept(this, context);
  }

  @Override
  protected R visitArithmeticExpression(ArithmeticExpression node, C context) {
    node.getLeft().accept(this, context);
    node.getRight().accept(this, context);

    return null;
  }

  @Override
  protected R visitBetweenPredicate(BetweenPredicate node, C context) {
    node.getValue().accept(this, context);
    node.getMin().accept(this, context);
    node.getMax().accept(this, context);

    return null;
  }

  @Override
  protected R visitComparisonExpression(ComparisonExpression node, C context) {
    node.getLeft().accept(this, context);
    node.getRight().accept(this, context);

    return null;
  }

  @Override
  protected R visitQuery(Query node, C context) {
    node.getQueryBody().accept(this, context);
    for (SortItem sortItem : node.getOrderBy()) {
      sortItem.accept(this, context);
    }

    return null;
  }

  @Override
  protected R visitSelect(Select node, C context) {
    for (SelectItem item : node.getSelectItems()) {
      item.accept(this, context);
    }

    return null;
  }

  @Override
  protected R visitSingleColumn(SingleColumn node, C context) {
    node.getExpression().accept(this, context);

    return null;
  }

  @Override
  protected R visitWhenClause(WhenClause node, C context) {
    node.getOperand().accept(this, context);
    node.getResult().accept(this, context);

    return null;
  }

  @Override
  protected R visitInPredicate(InPredicate node, C context) {
    node.getValue().accept(this, context);
    node.getValueList().accept(this, context);

    return null;
  }

  @Override
  protected R visitFunctionCall(FunctionCall node, C context) {
    for (Expression argument : node.getArguments()) {
      argument.accept(this, context);
    }

    return null;
  }

  @Override
  protected R visitSimpleCaseExpression(SimpleCaseExpression node, C context) {
    node.getOperand().accept(this, context);
    for (WhenClause clause : node.getWhenClauses()) {
      clause.accept(this, context);
    }
    if (node.getDefaultValue() != null) {
      node.getDefaultValue().accept(this, context);
    }

    return null;
  }

  @Override
  protected R visitInListExpression(InListExpression node, C context) {
    for (Expression value : node.getValues()) {
      value.accept(this, context);
    }

    return null;
  }

  @Override
  protected R visitIfExpression(IfExpression node, C context) {
    node.getCondition().accept(this, context);
    node.getTrueValue().accept(this, context);
    if (node.getFalseValue().isPresent()) {
      node.getFalseValue().get().accept(this, context);
    }

    return null;
  }

  @Override
  protected R visitNegativeExpression(NegativeExpression node, C context) {
    return node.getValue().accept(this, context);
  }

  @Override
  protected R visitNotExpression(NotExpression node, C context) {
    return node.getValue().accept(this, context);
  }

  @Override
  protected R visitSearchedCaseExpression(SearchedCaseExpression node, C context) {
    for (WhenClause clause : node.getWhenClauses()) {
      clause.accept(this, context);
    }
    Expression defaultValue = node.getDefaultValue();
    if (defaultValue != null) {
      defaultValue.accept(this, context);
    }

    return null;
  }

  @Override
  protected R visitLikePredicate(LikePredicate node, C context) {
    node.getValue().accept(this, context);
    node.getPattern().accept(this, context);
    Expression escape = node.getEscape();
    if (escape != null) {
      escape.accept(this, context);
    }

    return null;
  }

  @Override
  protected R visitIsNotNullPredicate(IsNotNullPredicate node, C context) {
    return node.getValue().accept(this, context);
  }

  @Override
  protected R visitIsNullPredicate(IsNullPredicate node, C context) {
    return node.getValue().accept(this, context);
  }

  @Override
  protected R visitLogicalBinaryExpression(LogicalBinaryExpression node, C context) {
    node.getLeft().accept(this, context);
    node.getRight().accept(this, context);

    return null;
  }

  @Override
  protected R visitSubqueryExpression(SubqueryExpression node, C context) {
    return node.getQuery().accept(this, context);
  }

  @Override
  protected R visitSortItem(SortItem node, C context) {
    return node.getSortKey().accept(this, context);
  }

  @Override
  protected R visitQuerySpecification(QuerySpecification node, C context) {

    // visit the from first, since this qualifies the select
    for (Relation relation : node.getFrom()) {
      relation.accept(this, context);
    }

    node.getSelect().accept(this, context);
    if (node.getWhere().isPresent()) {
      node.getWhere().get().accept(this, context);
    }
    for (Expression expression : node.getGroupBy()) {
      expression.accept(this, context);
    }
    if (node.getHaving().isPresent()) {
      node.getHaving().get().accept(this, context);
    }
    for (SortItem sortItem : node.getOrderBy()) {
      sortItem.accept(this, context);
    }
    return null;
  }

  @Override
  protected R visitUnion(Union node, C context) {
    node.getLeft().accept(this, context);
    node.getRight().accept(this, context);
    return null;
  }

  @Override
  protected R visitIntersect(Intersect node, C context) {
    node.getLeft().accept(this, context);
    node.getRight().accept(this, context);
    return null;
  }

  @Override
  protected R visitExcept(Except node, C context) {
    node.getLeft().accept(this, context);
    node.getRight().accept(this, context);
    return null;
  }

  @Override
  protected R visitTableSubquery(TableSubquery node, C context) {
    return node.getQuery().accept(this, context);
  }

  @Override
  protected R visitAliasedRelation(AliasedRelation node, C context) {
    return node.getRelation().accept(this, context);
  }

  @Override
  protected R visitJoin(Join node, C context) {
    node.getLeft().accept(this, context);
    node.getRight().accept(this, context);

    if (node.getCriteria().isPresent() && node.getCriteria().get() instanceof JoinOn) {
      ((JoinOn) node.getCriteria().get()).getExpression().accept(this, context);
    }

    return null;
  }

  @Override
  public R visitValuesList(ValuesList node, C context) {
    for (Expression value : node.values()) {
      value.accept(this, context);
    }
    return null;
  }

  @Override
  public R visitUpdate(Update node, C context) {
    node.relation().accept(this, context);
    for (Assignment assignment : node.assignments()) {
      assignment.accept(this, context);
    }

    if (node.whereClause().isPresent()) {
      node.whereClause().get().accept(this, context);
    }

    node.returningClause().forEach(x -> x.accept(this, context));
    return null;
  }

  @Override
  public R visitDelete(Delete node, C context) {
    node.getRelation().accept(this, context);
    return null;
  }

  @Override
  public R visitCopyFrom(CopyFrom<?> node, C context) {
    node.table().accept(this, context);
    return null;
  }

  @Override
  public R visitCopyTo(CopyTo<?> node, C context) {
    node.table().accept(this, context);
    return null;
  }

  @Override
  public R visitAlterTable(AlterTable<?> node, C context) {
    node.table().accept(this, context);
    return null;
  }

  @Override
  public R visitInsert(Insert<?> node, C context) {
    node.table().accept(this, context);
    node.insertSource().accept(this, context);
    node.returningClause().forEach(x -> x.accept(this, context));
    return null;
  }

  @Override
  public R visitDropTable(DropTable<?> node, C context) {
    node.table().accept(this, context);
    return super.visitDropTable(node, context);
  }

  @Override
  public R visitCreateTable(CreateTable<?> node, C context) {
    node.name().accept(this, context);
    return null;
  }

  @Override
  public R visitShowCreateTable(ShowCreateTable<?> node, C context) {
    node.table().accept(this, context);
    return null;
  }

  @Override
  public R visitRefreshStatement(RefreshStatement<?> node, C context) {
    for (Table nodeTable : node.tables()) {
      nodeTable.accept(this, context);
    }
    return null;
  }

  @Override
  public R visitMatchPredicate(MatchPredicate node, C context) {
    for (MatchPredicateColumnIdent columnIdent : node.idents()) {
      columnIdent.columnIdent().accept(this, context);
      columnIdent.boost().accept(this, context);
    }
    node.value().accept(this, context);

    return null;
  }
}
