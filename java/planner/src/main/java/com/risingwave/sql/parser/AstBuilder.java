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

package com.risingwave.sql.parser;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

import com.google.common.collect.ImmutableList;
import com.risingwave.common.collections.Lists2;
import com.risingwave.sql.ExpressionFormatter;
import com.risingwave.sql.parser.antlr.v4.SqlBaseBaseVisitor;
import com.risingwave.sql.parser.antlr.v4.SqlBaseLexer;
import com.risingwave.sql.parser.antlr.v4.SqlBaseParser;
import com.risingwave.sql.parser.antlr.v4.SqlBaseParser.BitStringContext;
import com.risingwave.sql.parser.antlr.v4.SqlBaseParser.ConflictTargetContext;
import com.risingwave.sql.parser.antlr.v4.SqlBaseParser.DiscardContext;
import com.risingwave.sql.parser.antlr.v4.SqlBaseParser.IsolationLevelContext;
import com.risingwave.sql.parser.antlr.v4.SqlBaseParser.SetTransactionContext;
import com.risingwave.sql.parser.antlr.v4.SqlBaseParser.TransactionModeContext;
import com.risingwave.sql.tree.AddColumnDefinition;
import com.risingwave.sql.tree.AliasedRelation;
import com.risingwave.sql.tree.AllColumns;
import com.risingwave.sql.tree.AlterClusterRerouteRetryFailed;
import com.risingwave.sql.tree.AlterTable;
import com.risingwave.sql.tree.AlterTableAddColumn;
import com.risingwave.sql.tree.AlterTableOpenClose;
import com.risingwave.sql.tree.AlterTableRename;
import com.risingwave.sql.tree.AlterTableReroute;
import com.risingwave.sql.tree.AlterUser;
import com.risingwave.sql.tree.AnalyzeStatement;
import com.risingwave.sql.tree.AnalyzerElement;
import com.risingwave.sql.tree.ArithmeticExpression;
import com.risingwave.sql.tree.ArrayComparisonExpression;
import com.risingwave.sql.tree.ArrayLikePredicate;
import com.risingwave.sql.tree.ArrayLiteral;
import com.risingwave.sql.tree.ArraySubQueryExpression;
import com.risingwave.sql.tree.Assignment;
import com.risingwave.sql.tree.BeginStatement;
import com.risingwave.sql.tree.BetweenPredicate;
import com.risingwave.sql.tree.BitString;
import com.risingwave.sql.tree.BooleanLiteral;
import com.risingwave.sql.tree.Cast;
import com.risingwave.sql.tree.CharFilters;
import com.risingwave.sql.tree.CheckColumnConstraint;
import com.risingwave.sql.tree.CheckConstraint;
import com.risingwave.sql.tree.ClusteredBy;
import com.risingwave.sql.tree.CollectionColumnType;
import com.risingwave.sql.tree.ColumnConstraint;
import com.risingwave.sql.tree.ColumnDefinition;
import com.risingwave.sql.tree.ColumnStorageDefinition;
import com.risingwave.sql.tree.ColumnType;
import com.risingwave.sql.tree.CommitStatement;
import com.risingwave.sql.tree.ComparisonExpression;
import com.risingwave.sql.tree.CopyFrom;
import com.risingwave.sql.tree.CopyTo;
import com.risingwave.sql.tree.CreateAnalyzer;
import com.risingwave.sql.tree.CreateFunction;
import com.risingwave.sql.tree.CreateSnapshot;
import com.risingwave.sql.tree.CreateStream;
import com.risingwave.sql.tree.CreateTable;
import com.risingwave.sql.tree.CreateTableAs;
import com.risingwave.sql.tree.CreateUser;
import com.risingwave.sql.tree.CreateView;
import com.risingwave.sql.tree.CurrentTime;
import com.risingwave.sql.tree.DeallocateStatement;
import com.risingwave.sql.tree.DecommissionNodeStatement;
import com.risingwave.sql.tree.Delete;
import com.risingwave.sql.tree.DenyPrivilege;
import com.risingwave.sql.tree.DiscardStatement;
import com.risingwave.sql.tree.DoubleLiteral;
import com.risingwave.sql.tree.DropAnalyzer;
import com.risingwave.sql.tree.DropCheckConstraint;
import com.risingwave.sql.tree.DropFunction;
import com.risingwave.sql.tree.DropSnapshot;
import com.risingwave.sql.tree.DropTable;
import com.risingwave.sql.tree.DropUser;
import com.risingwave.sql.tree.DropView;
import com.risingwave.sql.tree.EscapedCharStringLiteral;
import com.risingwave.sql.tree.Except;
import com.risingwave.sql.tree.ExistsPredicate;
import com.risingwave.sql.tree.Explain;
import com.risingwave.sql.tree.Expression;
import com.risingwave.sql.tree.Extract;
import com.risingwave.sql.tree.FrameBound;
import com.risingwave.sql.tree.FunctionArgument;
import com.risingwave.sql.tree.FunctionCall;
import com.risingwave.sql.tree.GcDanglingArtifacts;
import com.risingwave.sql.tree.GenericProperties;
import com.risingwave.sql.tree.GenericProperty;
import com.risingwave.sql.tree.GrantPrivilege;
import com.risingwave.sql.tree.IfExpression;
import com.risingwave.sql.tree.InListExpression;
import com.risingwave.sql.tree.InPredicate;
import com.risingwave.sql.tree.IndexColumnConstraint;
import com.risingwave.sql.tree.IndexDefinition;
import com.risingwave.sql.tree.Insert;
import com.risingwave.sql.tree.IntegerLiteral;
import com.risingwave.sql.tree.Intersect;
import com.risingwave.sql.tree.IntervalLiteral;
import com.risingwave.sql.tree.IsNotNullPredicate;
import com.risingwave.sql.tree.IsNullPredicate;
import com.risingwave.sql.tree.Join;
import com.risingwave.sql.tree.JoinCriteria;
import com.risingwave.sql.tree.JoinOn;
import com.risingwave.sql.tree.JoinUsing;
import com.risingwave.sql.tree.KillStatement;
import com.risingwave.sql.tree.LikePredicate;
import com.risingwave.sql.tree.LogicalBinaryExpression;
import com.risingwave.sql.tree.LongLiteral;
import com.risingwave.sql.tree.MatchPredicate;
import com.risingwave.sql.tree.MatchPredicateColumnIdent;
import com.risingwave.sql.tree.NamedProperties;
import com.risingwave.sql.tree.NaturalJoin;
import com.risingwave.sql.tree.NegativeExpression;
import com.risingwave.sql.tree.Node;
import com.risingwave.sql.tree.NotExpression;
import com.risingwave.sql.tree.NotNullColumnConstraint;
import com.risingwave.sql.tree.NullLiteral;
import com.risingwave.sql.tree.ObjectColumnType;
import com.risingwave.sql.tree.ObjectLiteral;
import com.risingwave.sql.tree.OptimizeStatement;
import com.risingwave.sql.tree.ParameterExpression;
import com.risingwave.sql.tree.PartitionedBy;
import com.risingwave.sql.tree.PrimaryKeyColumnConstraint;
import com.risingwave.sql.tree.PrimaryKeyConstraint;
import com.risingwave.sql.tree.PromoteReplica;
import com.risingwave.sql.tree.QualifiedName;
import com.risingwave.sql.tree.QualifiedNameReference;
import com.risingwave.sql.tree.Query;
import com.risingwave.sql.tree.QueryBody;
import com.risingwave.sql.tree.QuerySpecification;
import com.risingwave.sql.tree.RecordSubscript;
import com.risingwave.sql.tree.RefreshStatement;
import com.risingwave.sql.tree.Relation;
import com.risingwave.sql.tree.RerouteAllocateReplicaShard;
import com.risingwave.sql.tree.RerouteCancelShard;
import com.risingwave.sql.tree.RerouteMoveShard;
import com.risingwave.sql.tree.RerouteOption;
import com.risingwave.sql.tree.ResetStatement;
import com.risingwave.sql.tree.RestoreSnapshot;
import com.risingwave.sql.tree.RevokePrivilege;
import com.risingwave.sql.tree.SearchedCaseExpression;
import com.risingwave.sql.tree.Select;
import com.risingwave.sql.tree.SelectItem;
import com.risingwave.sql.tree.SetSessionAuthorizationStatement;
import com.risingwave.sql.tree.SetStatement;
import com.risingwave.sql.tree.SetTransactionStatement;
import com.risingwave.sql.tree.SetTransactionStatement.TransactionMode;
import com.risingwave.sql.tree.ShowColumns;
import com.risingwave.sql.tree.ShowCreateTable;
import com.risingwave.sql.tree.ShowSchemas;
import com.risingwave.sql.tree.ShowSessionParameter;
import com.risingwave.sql.tree.ShowTables;
import com.risingwave.sql.tree.ShowTransaction;
import com.risingwave.sql.tree.SimpleCaseExpression;
import com.risingwave.sql.tree.SingleColumn;
import com.risingwave.sql.tree.SortItem;
import com.risingwave.sql.tree.Statement;
import com.risingwave.sql.tree.StringLiteral;
import com.risingwave.sql.tree.SubqueryExpression;
import com.risingwave.sql.tree.SubscriptExpression;
import com.risingwave.sql.tree.SwapTable;
import com.risingwave.sql.tree.Table;
import com.risingwave.sql.tree.TableElement;
import com.risingwave.sql.tree.TableFunction;
import com.risingwave.sql.tree.TableSubquery;
import com.risingwave.sql.tree.TokenFilters;
import com.risingwave.sql.tree.Tokenizer;
import com.risingwave.sql.tree.TrimMode;
import com.risingwave.sql.tree.TryCast;
import com.risingwave.sql.tree.Union;
import com.risingwave.sql.tree.Update;
import com.risingwave.sql.tree.Values;
import com.risingwave.sql.tree.ValuesList;
import com.risingwave.sql.tree.WhenClause;
import com.risingwave.sql.tree.Window;
import com.risingwave.sql.tree.WindowFrame;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

class AstBuilder extends SqlBaseBaseVisitor<Node> {

  private int parameterPosition = 1;
  private static final String CLUSTER = "CLUSTER";
  private static final String SCHEMA = "SCHEMA";
  private static final String TABLE = "TABLE";
  private static final String VIEW = "VIEW";

  @Override
  public Node visitSingleStatement(SqlBaseParser.SingleStatementContext context) {
    return visit(context.statement());
  }

  @Override
  public Node visitSingleExpression(SqlBaseParser.SingleExpressionContext context) {
    return visit(context.expr());
  }

  //  Statements

  @Override
  public Node visitBegin(SqlBaseParser.BeginContext context) {
    return new BeginStatement();
  }

  @Override
  public Node visitStartTransaction(SqlBaseParser.StartTransactionContext context) {
    return new BeginStatement();
  }

  @Override
  public Node visitAnalyze(SqlBaseParser.AnalyzeContext ctx) {
    return new AnalyzeStatement();
  }

  @Override
  public Node visitDiscard(DiscardContext ctx) {
    final DiscardStatement.Target target;
    if (ctx.ALL() != null) {
      target = DiscardStatement.Target.ALL;
    } else if (ctx.PLANS() != null) {
      target = DiscardStatement.Target.PLANS;
    } else if (ctx.SEQUENCES() != null) {
      target = DiscardStatement.Target.SEQUENCES;
    } else if (ctx.TEMP() != null || ctx.TEMPORARY() != null) {
      target = DiscardStatement.Target.TEMPORARY;
    } else {
      throw new IllegalStateException("Unexpected DiscardContext: " + ctx);
    }
    return new DiscardStatement(target);
  }

  @Override
  public Node visitIntervalLiteral(SqlBaseParser.IntervalLiteralContext context) {
    IntervalLiteral.IntervalField startField =
        getIntervalFieldType((Token) context.from.getChild(0).getPayload());
    IntervalLiteral.IntervalField endField = null;
    if (context.to != null) {
      Token token = (Token) context.to.getChild(0).getPayload();
      endField = getIntervalFieldType(token);
    }

    if (endField != null) {
      if (startField.compareTo(endField) > 0) {
        throw new IllegalArgumentException("Startfield must be less significant than Endfield");
      }
    }

    IntervalLiteral.Sign sign = IntervalLiteral.Sign.PLUS;
    if (context.sign != null) {
      sign = getIntervalSign(context.sign);
    }

    return new IntervalLiteral(
        ((StringLiteral) visit(context.stringLiteral())).getValue(), sign, startField, endField);
  }

  private static IntervalLiteral.Sign getIntervalSign(Token token) {
    switch (token.getType()) {
      case SqlBaseLexer.MINUS:
        return IntervalLiteral.Sign.MINUS;
      case SqlBaseLexer.PLUS:
        return IntervalLiteral.Sign.PLUS;
      default:
        throw new IllegalArgumentException("Unsupported sign: " + token.getText());
    }
  }

  private static IntervalLiteral.IntervalField getIntervalFieldType(Token token) {
    switch (token.getType()) {
      case SqlBaseLexer.YEAR:
        return IntervalLiteral.IntervalField.YEAR;
      case SqlBaseLexer.MONTH:
        return IntervalLiteral.IntervalField.MONTH;
      case SqlBaseLexer.DAY:
        return IntervalLiteral.IntervalField.DAY;
      case SqlBaseLexer.HOUR:
        return IntervalLiteral.IntervalField.HOUR;
      case SqlBaseLexer.MINUTE:
        return IntervalLiteral.IntervalField.MINUTE;
      case SqlBaseLexer.SECOND:
        return IntervalLiteral.IntervalField.SECOND;
      default:
        throw new IllegalArgumentException("Unsupported interval field: " + token.getText());
    }
  }

  @Override
  public Node visitCommit(SqlBaseParser.CommitContext context) {
    return new CommitStatement();
  }

  @Override
  public Node visitOptimize(SqlBaseParser.OptimizeContext context) {
    return new OptimizeStatement(
        visitCollection(context.tableWithPartitions().tableWithPartition(), Table.class),
        extractGenericProperties(context.withProperties()));
  }

  @Override
  public Node visitCreateTable(SqlBaseParser.CreateTableContext context) {
    boolean notExists = context.EXISTS() != null;
    SqlBaseParser.PartitionedByOrClusteredIntoContext tableOptsCtx =
        context.partitionedByOrClusteredInto();
    Optional<ClusteredBy> clusteredBy =
        visitIfPresent(tableOptsCtx.clusteredBy(), ClusteredBy.class);
    Optional<PartitionedBy> partitionedBy =
        visitIfPresent(tableOptsCtx.partitionedBy(), PartitionedBy.class);
    List tableElements =
        Lists2.map(context.tableElement(), x -> (TableElement<Expression>) visit(x));
    return new CreateTable(
        (Table) visit(context.table()),
        tableElements,
        partitionedBy,
        clusteredBy,
        extractGenericProperties(context.withProperties()),
        notExists);
  }

  @Override
  public Node visitCreateStream(SqlBaseParser.CreateStreamContext context) {
    List<Node> tableElements = Lists2.map(context.tableElement(), this::visit);

    String rowSchemaLocation = "";

    if (context.rowSchemaLocation != null) {
      rowSchemaLocation = getText(context.rowSchemaLocation);
    }

    return new CreateStream(
        context.name.getText(),
        tableElements,
        extractGenericProperties(context.withProperties()),
        getText(context.rowFormat),
        rowSchemaLocation);
  }

  @Override
  public Node visitCreateTableAs(SqlBaseParser.CreateTableAsContext context) {
    return new CreateTableAs(
        (Table) visit(context.table()), (Query) visit(context.insertSource().query()));
  }

  @Override
  public Node visitAlterClusterSwapTable(SqlBaseParser.AlterClusterSwapTableContext ctx) {
    return new SwapTable<>(
        getQualifiedName(ctx.source),
        getQualifiedName(ctx.target),
        extractGenericProperties(ctx.withProperties()));
  }

  @Override
  public Node visitAlterClusterGCDanglingArtifacts(
      SqlBaseParser.AlterClusterGCDanglingArtifactsContext ctx) {
    return GcDanglingArtifacts.INSTANCE;
  }

  @Override
  public Node visitAlterClusterDecommissionNode(
      SqlBaseParser.AlterClusterDecommissionNodeContext ctx) {
    return new DecommissionNodeStatement<>(visit(ctx.node));
  }

  @Override
  public Node visitCreateView(SqlBaseParser.CreateViewContext ctx) {
    return new CreateView(
        getQualifiedName(ctx.qname()),
        (Query) visit(ctx.query()),
        ctx.REPLACE() != null,
        ctx.MATERIALIZED() != null);
  }

  @Override
  public Node visitDropView(SqlBaseParser.DropViewContext ctx) {
    return new DropView(getQualifiedNames(ctx.qnames()), ctx.EXISTS() != null);
  }

  @Override
  public Node visitCreateSnapshot(SqlBaseParser.CreateSnapshotContext context) {
    if (context.ALL() != null) {
      return new CreateSnapshot(
          getQualifiedName(context.qname()), extractGenericProperties(context.withProperties()));
    }
    return new CreateSnapshot(
        getQualifiedName(context.qname()),
        visitCollection(context.tableWithPartitions().tableWithPartition(), Table.class),
        extractGenericProperties(context.withProperties()));
  }

  @Override
  public Node visitCreateAnalyzer(SqlBaseParser.CreateAnalyzerContext context) {
    return new CreateAnalyzer(
        getIdentText(context.name),
        getIdentText(context.extendedName),
        visitCollection(context.analyzerElement(), AnalyzerElement.class));
  }

  @Override
  public Node visitDropAnalyzer(SqlBaseParser.DropAnalyzerContext ctx) {
    return new DropAnalyzer(getIdentText(ctx.name));
  }

  @Override
  public Node visitCreateUser(SqlBaseParser.CreateUserContext context) {
    return new CreateUser(
        getIdentText(context.name), extractGenericProperties(context.withProperties()));
  }

  @Override
  public Node visitDropUser(SqlBaseParser.DropUserContext context) {
    return new DropUser(getIdentText(context.name), context.EXISTS() != null);
  }

  @Override
  public Node visitGrantPrivilege(SqlBaseParser.GrantPrivilegeContext context) {
    List<String> usernames = identsToStrings(context.users.ident());
    ClassAndIdent clazzAndIdent =
        getClassAndIdentsForPrivileges(context.ON() == null, context.clazz(), context.qnames());
    if (context.ALL() != null) {
      return new GrantPrivilege(usernames, clazzAndIdent.clazz, clazzAndIdent.idents);
    } else {
      List<String> privilegeTypes = identsToStrings(context.priviliges.ident());
      return new GrantPrivilege(
          usernames, privilegeTypes, clazzAndIdent.clazz, clazzAndIdent.idents);
    }
  }

  @Override
  public Node visitDenyPrivilege(SqlBaseParser.DenyPrivilegeContext context) {
    List<String> usernames = identsToStrings(context.users.ident());
    ClassAndIdent clazzAndIdent =
        getClassAndIdentsForPrivileges(context.ON() == null, context.clazz(), context.qnames());
    if (context.ALL() != null) {
      return new DenyPrivilege(usernames, clazzAndIdent.clazz, clazzAndIdent.idents);
    } else {
      List<String> privilegeTypes = identsToStrings(context.priviliges.ident());
      return new DenyPrivilege(
          usernames, privilegeTypes, clazzAndIdent.clazz, clazzAndIdent.idents);
    }
  }

  @Override
  public Node visitRevokePrivilege(SqlBaseParser.RevokePrivilegeContext context) {
    List<String> usernames = identsToStrings(context.users.ident());
    ClassAndIdent clazzAndIdent =
        getClassAndIdentsForPrivileges(context.ON() == null, context.clazz(), context.qnames());
    if (context.ALL() != null) {
      return new RevokePrivilege(usernames, clazzAndIdent.clazz, clazzAndIdent.idents);
    } else {
      List<String> privilegeTypes = identsToStrings(context.privileges.ident());
      return new RevokePrivilege(
          usernames, privilegeTypes, clazzAndIdent.clazz, clazzAndIdent.idents);
    }
  }

  @Override
  public Node visitCharFilters(SqlBaseParser.CharFiltersContext context) {
    return new CharFilters(visitCollection(context.namedProperties(), NamedProperties.class));
  }

  @Override
  public Node visitTokenFilters(SqlBaseParser.TokenFiltersContext context) {
    return new TokenFilters(visitCollection(context.namedProperties(), NamedProperties.class));
  }

  @Override
  public Node visitTokenizer(SqlBaseParser.TokenizerContext context) {
    return new Tokenizer<>((NamedProperties<Expression>) visit(context.namedProperties()));
  }

  @Override
  public Node visitNamedProperties(SqlBaseParser.NamedPropertiesContext context) {
    return new NamedProperties<>(
        getIdentText(context.ident()), extractGenericProperties(context.withProperties()));
  }

  @Override
  public Node visitRestore(SqlBaseParser.RestoreContext context) {
    if (context.ALL() != null) {
      return new RestoreSnapshot<>(
          getQualifiedName(context.qname()),
          RestoreSnapshot.Mode.ALL,
          extractGenericProperties(context.withProperties()));
    }
    if (context.METADATA() != null) {
      return new RestoreSnapshot<>(
          getQualifiedName(context.qname()),
          RestoreSnapshot.Mode.METADATA,
          extractGenericProperties(context.withProperties()));
    }
    if (context.TABLE() != null) {
      return new RestoreSnapshot(
          getQualifiedName(context.qname()),
          RestoreSnapshot.Mode.TABLE,
          extractGenericProperties(context.withProperties()),
          Collections.emptyList(),
          visitCollection(context.tableWithPartitions().tableWithPartition(), Table.class));
    }
    return new RestoreSnapshot<>(
        getQualifiedName(context.qname()),
        RestoreSnapshot.Mode.CUSTOM,
        extractGenericProperties(context.withProperties()),
        identsToStrings(context.metatypes.ident()));
  }

  @Override
  public Node visitShowCreateTable(SqlBaseParser.ShowCreateTableContext context) {
    return new ShowCreateTable((Table) visit(context.table()));
  }

  @Override
  public Node visitShowTransaction(SqlBaseParser.ShowTransactionContext context) {
    return new ShowTransaction();
  }

  @Override
  public Node visitShowSessionParameter(SqlBaseParser.ShowSessionParameterContext ctx) {
    if (ctx.ALL() != null) {
      return new ShowSessionParameter(null);
    } else {
      return new ShowSessionParameter(getQualifiedName(ctx.qname()));
    }
  }

  @Override
  public Node visitDropTable(SqlBaseParser.DropTableContext context) {
    return new DropTable((Table) visit(context.table()), context.EXISTS() != null);
  }

  @Override
  public Node visitDropSnapshot(SqlBaseParser.DropSnapshotContext context) {
    return new DropSnapshot(getQualifiedName(context.qname()));
  }

  @Override
  public Node visitCopyFrom(SqlBaseParser.CopyFromContext context) {
    boolean returnSummary = context.SUMMARY() != null;
    return new CopyFrom(
        (Table) visit(context.tableWithPartition()),
        visit(context.path),
        extractGenericProperties(context.withProperties()),
        returnSummary);
  }

  @Override
  public Node visitCopyTo(SqlBaseParser.CopyToContext context) {
    return new CopyTo(
        (Table) visit(context.tableWithPartition()),
        context.columns() == null
            ? emptyList()
            : visitCollection(context.columns().primaryExpression(), Expression.class),
        visitIfPresent(context.where(), Expression.class),
        context.DIRECTORY() != null,
        visit(context.path),
        extractGenericProperties(context.withProperties()));
  }

  @Override
  public Node visitInsert(SqlBaseParser.InsertContext context) {
    List<String> columns = identsToStrings(context.ident());

    Table table;
    try {
      table = (Table) visit(context.table());
    } catch (ClassCastException e) {
      TableFunction tf = (TableFunction) visit(context.table());
      for (Expression ex : tf.functionCall().getArguments()) {
        if (!(ex instanceof QualifiedNameReference)) {
          throw new IllegalArgumentException(
              String.format(Locale.ENGLISH, "invalid table column reference %s", ex.toString()));
        }
      }
      throw e;
    }
    return new Insert<>(
        table,
        (Query) visit(context.insertSource().query()),
        columns,
        getReturningItems(context.returning()),
        createDuplicateKeyContext(context));
  }

  /** Creates a {@link com.risingwave.sql.tree.Insert.DuplicateKeyContext} based on the Insert */
  private Insert.DuplicateKeyContext createDuplicateKeyContext(
      SqlBaseParser.InsertContext context) {
    if (context.onConflict() != null) {
      SqlBaseParser.OnConflictContext onConflictContext = context.onConflict();
      ConflictTargetContext conflictTarget = onConflictContext.conflictTarget();
      final List<Expression> conflictColumns;
      if (conflictTarget == null) {
        conflictColumns = Collections.emptyList();
      } else {
        conflictColumns = visitCollection(conflictTarget.subscriptSafe(), Expression.class);
      }
      if (onConflictContext.NOTHING() != null) {
        return new Insert.DuplicateKeyContext<>(
            Insert.DuplicateKeyContext.Type.ON_CONFLICT_DO_NOTHING, emptyList(), conflictColumns);
      } else {
        if (conflictColumns.isEmpty()) {
          throw new IllegalStateException(
              "ON CONFLICT <conflict_target> <- conflict_target missing");
        }
        List assignments =
            Lists2.map(onConflictContext.assignment(), x -> (Assignment<Expression>) visit(x));
        return new Insert.DuplicateKeyContext<>(
            Insert.DuplicateKeyContext.Type.ON_CONFLICT_DO_UPDATE_SET,
            assignments,
            conflictColumns);
      }
    } else {
      return Insert.DuplicateKeyContext.none();
    }
  }

  @Override
  public Node visitValues(SqlBaseParser.ValuesContext context) {
    return new ValuesList(visitCollection(context.expr(), Expression.class));
  }

  @Override
  public Node visitDelete(SqlBaseParser.DeleteContext context) {
    return new Delete(
        (Relation) visit(context.aliasedRelation()),
        visitIfPresent(context.where(), Expression.class));
  }

  @Override
  public Node visitUpdate(SqlBaseParser.UpdateContext context) {
    List assignments = Lists2.map(context.assignment(), x -> (Assignment<Expression>) visit(x));
    return new Update(
        (Relation) visit(context.aliasedRelation()),
        assignments,
        visitIfPresent(context.where(), Expression.class),
        getReturningItems(context.returning()));
  }

  @Override
  public Node visitSet(SqlBaseParser.SetContext context) {
    Assignment<?> setAssignment = prepareSetAssignment(context);
    if (context.LOCAL() != null) {
      return new SetStatement<>(SetStatement.Scope.LOCAL, setAssignment);
    }
    return new SetStatement<>(SetStatement.Scope.SESSION, setAssignment);
  }

  private Assignment<?> prepareSetAssignment(SqlBaseParser.SetContext context) {
    Expression settingName = new QualifiedNameReference(getQualifiedName(context.qname()));
    if (context.DEFAULT() != null) {
      return new Assignment<>(settingName, emptyList());
    }
    return new Assignment<>(settingName, visitCollection(context.setExpr(), Expression.class));
  }

  @Override
  public Node visitSetGlobal(SqlBaseParser.SetGlobalContext context) {
    List assignments =
        Lists2.map(context.setGlobalAssignment(), x -> (Assignment<Expression>) visit(x));
    if (context.PERSISTENT() != null) {
      return new SetStatement<>(
          SetStatement.Scope.GLOBAL, SetStatement.SettingType.PERSISTENT, assignments);
    }
    return new SetStatement<>(SetStatement.Scope.GLOBAL, assignments);
  }

  @Override
  public Node visitSetLicense(SqlBaseParser.SetLicenseContext ctx) {
    Assignment<Expression> assignment =
        new Assignment<>(new StringLiteral("license"), (Expression) visit(ctx.stringLiteral()));

    return new SetStatement<>(
        SetStatement.Scope.LICENSE,
        SetStatement.SettingType.PERSISTENT,
        Collections.singletonList(assignment));
  }

  @Override
  public Node visitSetTransaction(SetTransactionContext ctx) {
    List<TransactionMode> modes = Lists2.map(ctx.transactionMode(), AstBuilder::getTransactionMode);
    return new SetTransactionStatement(modes);
  }

  private static TransactionMode getTransactionMode(TransactionModeContext transactionModeCtx) {
    if (transactionModeCtx.ISOLATION() != null) {
      IsolationLevelContext isolationLevel = transactionModeCtx.isolationLevel();
      if (isolationLevel.COMMITTED() != null) {
        return SetTransactionStatement.IsolationLevel.READ_COMMITTED;
      } else if (isolationLevel.UNCOMMITTED() != null) {
        return SetTransactionStatement.IsolationLevel.READ_UNCOMMITTED;
      } else if (isolationLevel.REPEATABLE() != null) {
        return SetTransactionStatement.IsolationLevel.REPEATABLE_READ;
      } else {
        return SetTransactionStatement.IsolationLevel.SERIALIZABLE;
      }
    } else if (transactionModeCtx.READ() != null) {
      return SetTransactionStatement.ReadMode.READ_ONLY;
    } else if (transactionModeCtx.WRITE() != null) {
      return SetTransactionStatement.ReadMode.READ_WRITE;
    } else if (transactionModeCtx.NOT() != null) {
      return new SetTransactionStatement.Deferrable(true);
    } else if (transactionModeCtx.DEFERRABLE() != null) {
      return new SetTransactionStatement.Deferrable(false);
    } else {
      throw new IllegalStateException("Unexpected TransactionModeContext: " + transactionModeCtx);
    }
  }

  @Override
  public Node visitResetGlobal(SqlBaseParser.ResetGlobalContext context) {
    return new ResetStatement<>(visitCollection(context.primaryExpression(), Expression.class));
  }

  @Override
  public Node visitSetSessionAuthorization(SqlBaseParser.SetSessionAuthorizationContext context) {
    SetSessionAuthorizationStatement.Scope scope;
    if (context.LOCAL() != null) {
      scope = SetSessionAuthorizationStatement.Scope.LOCAL;
    } else {
      scope = SetSessionAuthorizationStatement.Scope.SESSION;
    }

    if (context.DEFAULT() != null) {
      return new SetSessionAuthorizationStatement(scope);
    } else {
      Node userNameLiteral = visit(context.username);
      assert userNameLiteral instanceof StringLiteral
          : "username must be a StringLiteral because "
              + "the parser grammar is restricted to string literals";
      String userName = ((StringLiteral) userNameLiteral).getValue();
      return new SetSessionAuthorizationStatement(userName, scope);
    }
  }

  @Override
  public Node visitResetSessionAuthorization(SqlBaseParser.ResetSessionAuthorizationContext ctx) {
    return new SetSessionAuthorizationStatement(SetSessionAuthorizationStatement.Scope.SESSION);
  }

  @Override
  public Node visitKill(SqlBaseParser.KillContext context) {
    return new KillStatement<>(visitIfPresent(context.jobId, Expression.class).orElse(null));
  }

  @Override
  public Node visitDeallocate(SqlBaseParser.DeallocateContext context) {
    if (context.ALL() != null) {
      return new DeallocateStatement();
    }
    return new DeallocateStatement((Expression) visit(context.prepStmt));
  }

  @Override
  public Node visitExplain(SqlBaseParser.ExplainContext context) {
    return new Explain((Statement) visit(context.statement()), context.ANALYZE() != null);
  }

  @Override
  public Node visitShowTables(SqlBaseParser.ShowTablesContext context) {
    return new ShowTables(
        context.qname() == null ? null : getQualifiedName(context.qname()),
        getUnquotedText(context.pattern),
        visitIfPresent(context.where(), Expression.class));
  }

  @Override
  public Node visitShowSchemas(SqlBaseParser.ShowSchemasContext context) {
    return new ShowSchemas(
        getUnquotedText(context.pattern), visitIfPresent(context.where(), Expression.class));
  }

  @Override
  public Node visitShowColumns(SqlBaseParser.ShowColumnsContext context) {
    return new ShowColumns(
        getQualifiedName(context.tableName),
        context.schema == null ? null : getQualifiedName(context.schema),
        visitIfPresent(context.where(), Expression.class),
        getUnquotedText(context.pattern));
  }

  @Override
  public Node visitRefreshTable(SqlBaseParser.RefreshTableContext context) {
    return new RefreshStatement(
        visitCollection(context.tableWithPartitions().tableWithPartition(), Table.class));
  }

  @Override
  public Node visitTableOnly(SqlBaseParser.TableOnlyContext context) {
    return new Table(getQualifiedName(context.qname()));
  }

  @Override
  public Node visitTableWithPartition(SqlBaseParser.TableWithPartitionContext context) {
    return new Table(
        getQualifiedName(context.qname()), visitCollection(context.assignment(), Assignment.class));
  }

  @Override
  public Node visitCreateFunction(SqlBaseParser.CreateFunctionContext context) {
    QualifiedName functionName = getQualifiedName(context.name);
    validateFunctionName(functionName);
    return new CreateFunction(
        functionName,
        context.REPLACE() != null,
        visitCollection(context.functionArgument(), FunctionArgument.class),
        (ColumnType) visit(context.returnType),
        (Expression) visit(context.language),
        (Expression) visit(context.body));
  }

  @Override
  public Node visitDropFunction(SqlBaseParser.DropFunctionContext context) {
    QualifiedName functionName = getQualifiedName(context.name);
    validateFunctionName(functionName);

    return new DropFunction(
        functionName,
        context.EXISTS() != null,
        visitCollection(context.functionArgument(), FunctionArgument.class));
  }

  // Column / Table definition

  @Override
  public Node visitColumnDefinition(SqlBaseParser.ColumnDefinitionContext context) {
    return new ColumnDefinition(
        getIdentText(context.ident()),
        visitOptionalContext(context.defaultExpr, Expression.class),
        visitOptionalContext(context.generatedExpr, Expression.class),
        visitOptionalContext(context.dataType(), ColumnType.class),
        visitCollection(context.columnConstraint(), ColumnConstraint.class));
  }

  @Override
  public Node visitColumnConstraintPrimaryKey(
      SqlBaseParser.ColumnConstraintPrimaryKeyContext context) {
    return new PrimaryKeyColumnConstraint();
  }

  @Override
  public Node visitColumnConstraintNotNull(SqlBaseParser.ColumnConstraintNotNullContext context) {
    return new NotNullColumnConstraint();
  }

  @Override
  public Node visitPrimaryKeyConstraint(SqlBaseParser.PrimaryKeyConstraintContext context) {
    return new PrimaryKeyConstraint<>(
        visitCollection(context.columns().primaryExpression(), Expression.class));
  }

  @Override
  public Node visitTableCheckConstraint(SqlBaseParser.TableCheckConstraintContext context) {
    SqlBaseParser.CheckConstraintContext ctx = context.checkConstraint();
    String name = ctx.CONSTRAINT() != null ? getIdentText(ctx.name) : null;
    Expression expression = (Expression) visit(ctx.expression);
    String expressionStr = ExpressionFormatter.formatStandaloneExpression(expression);
    return new CheckConstraint<>(name, null, expression, expressionStr);
  }

  @Override
  public Node visitColumnCheckConstraint(SqlBaseParser.ColumnCheckConstraintContext context) {
    SqlBaseParser.CheckConstraintContext ctx = context.checkConstraint();
    String name = ctx.CONSTRAINT() != null ? getIdentText(ctx.name) : null;
    Expression expression = (Expression) visit(ctx.expression);
    String expressionStr = ExpressionFormatter.formatStandaloneExpression(expression);
    // column name is obtained during analysis
    return new CheckColumnConstraint<>(name, null, expression, expressionStr);
  }

  @Override
  public Node visitDropCheckConstraint(SqlBaseParser.DropCheckConstraintContext context) {
    Table<?> table = (Table<?>) visit(context.alterTableDefinition());
    return new DropCheckConstraint<>(table, getIdentText(context.ident()));
  }

  @Override
  public Node visitColumnIndexOff(SqlBaseParser.ColumnIndexOffContext context) {
    return IndexColumnConstraint.off();
  }

  @Override
  public Node visitColumnIndexConstraint(SqlBaseParser.ColumnIndexConstraintContext context) {
    return new IndexColumnConstraint<>(
        getIdentText(context.method), extractGenericProperties(context.withProperties()));
  }

  @Override
  public Node visitIndexDefinition(SqlBaseParser.IndexDefinitionContext context) {
    return new IndexDefinition<>(
        getIdentText(context.name),
        getIdentText(context.method),
        visitCollection(context.columns().primaryExpression(), Expression.class),
        extractGenericProperties(context.withProperties()));
  }

  @Override
  public Node visitColumnStorageDefinition(SqlBaseParser.ColumnStorageDefinitionContext ctx) {
    return new ColumnStorageDefinition<>(extractGenericProperties(ctx.withProperties()));
  }

  @Override
  public Node visitPartitionedBy(SqlBaseParser.PartitionedByContext context) {
    return new PartitionedBy<>(
        visitCollection(context.columns().primaryExpression(), Expression.class));
  }

  @Override
  public Node visitClusteredBy(SqlBaseParser.ClusteredByContext context) {
    return new ClusteredBy<>(
        visitIfPresent(context.routing, Expression.class),
        visitIfPresent(context.numShards, Expression.class));
  }

  @Override
  public Node visitFunctionArgument(SqlBaseParser.FunctionArgumentContext context) {
    return new FunctionArgument(
        getIdentText(context.ident()), (ColumnType) visit(context.dataType()));
  }

  @Override
  public Node visitRerouteMoveShard(SqlBaseParser.RerouteMoveShardContext context) {
    return new RerouteMoveShard<>(
        visit(context.shardId), visit(context.fromNodeId), visit(context.toNodeId));
  }

  @Override
  public Node visitReroutePromoteReplica(SqlBaseParser.ReroutePromoteReplicaContext ctx) {
    return new PromoteReplica(
        visit(ctx.nodeId), visit(ctx.shardId), extractGenericProperties(ctx.withProperties()));
  }

  @Override
  public Node visitRerouteAllocateReplicaShard(
      SqlBaseParser.RerouteAllocateReplicaShardContext context) {
    return new RerouteAllocateReplicaShard<>(visit(context.shardId), visit(context.nodeId));
  }

  @Override
  public Node visitRerouteCancelShard(SqlBaseParser.RerouteCancelShardContext context) {
    return new RerouteCancelShard(
        visit(context.shardId),
        visit(context.nodeId),
        extractGenericProperties(context.withProperties()));
  }

  // Properties
  private GenericProperties<Expression> extractGenericProperties(ParserRuleContext context) {
    return visitIfPresent(context, GenericProperties.class).orElse(GenericProperties.empty());
  }

  @Override
  public Node visitWithGenericProperties(SqlBaseParser.WithGenericPropertiesContext context) {
    return visitGenericProperties(context.genericProperties());
  }

  @Override
  public Node visitGenericProperties(SqlBaseParser.GenericPropertiesContext context) {
    GenericProperties<Expression> properties = new GenericProperties<>();
    context.genericProperty().forEach(p -> properties.add(visitGenericProperty(p)));
    return properties;
  }

  @Override
  public GenericProperty<Expression> visitGenericProperty(
      SqlBaseParser.GenericPropertyContext context) {
    return new GenericProperty<>(
        getText(context.stringLiteralOrIdentifierOrQname()), (Expression) visit(context.expr()));
  }

  // Amending tables

  @Override
  public Node visitAlterTableProperties(SqlBaseParser.AlterTablePropertiesContext context) {
    Table name = (Table) visit(context.alterTableDefinition());
    if (context.SET() != null) {
      return new AlterTable(name, extractGenericProperties(context.genericProperties()));
    }
    return new AlterTable(name, identsToStrings(context.ident()));
  }

  @Override
  public Node visitAddColumn(SqlBaseParser.AddColumnContext context) {
    return new AlterTableAddColumn(
        (Table) visit(context.alterTableDefinition()),
        (AddColumnDefinition) visit(context.addColumnDefinition()));
  }

  @Override
  public Node visitAddColumnDefinition(SqlBaseParser.AddColumnDefinitionContext context) {
    return new AddColumnDefinition(
        (Expression) visit(context.subscriptSafe()),
        visitOptionalContext(context.expr(), Expression.class),
        visitOptionalContext(context.dataType(), ColumnType.class),
        visitCollection(context.columnConstraint(), ColumnConstraint.class));
  }

  @Override
  public Node visitAlterTableOpenClose(SqlBaseParser.AlterTableOpenCloseContext context) {
    return new AlterTableOpenClose(
        (Table) visit(context.alterTableDefinition()), context.OPEN() != null);
  }

  @Override
  public Node visitAlterTableRename(SqlBaseParser.AlterTableRenameContext context) {
    return new AlterTableRename(
        (Table) visit(context.alterTableDefinition()), getQualifiedName(context.qname()));
  }

  @Override
  public Node visitAlterTableReroute(SqlBaseParser.AlterTableRerouteContext context) {
    return new AlterTableReroute<>(
        (Table) visit(context.alterTableDefinition()),
        (RerouteOption) visit(context.rerouteOption()));
  }

  @Override
  public Node visitAlterClusterRerouteRetryFailed(
      SqlBaseParser.AlterClusterRerouteRetryFailedContext context) {
    return new AlterClusterRerouteRetryFailed();
  }

  @Override
  public Node visitAlterUser(SqlBaseParser.AlterUserContext context) {
    return new AlterUser(
        getIdentText(context.name), extractGenericProperties(context.genericProperties()));
  }

  @Override
  public Node visitSetGlobalAssignment(SqlBaseParser.SetGlobalAssignmentContext context) {
    return new Assignment(
        (Expression) visit(context.primaryExpression()), (Expression) visit(context.expr()));
  }

  @Override
  public Node visitAssignment(SqlBaseParser.AssignmentContext context) {
    Expression column = (Expression) visit(context.primaryExpression());
    // such as it is currently hard to restrict a left side of an assignment to subscript and
    // qname in the grammar, because of our current grammar structure which causes the
    // indirect left-side recursion when attempting to do so. We restrict it before initializing
    // an Assignment.
    if (column instanceof SubscriptExpression || column instanceof QualifiedNameReference) {
      return new Assignment<>(column, (Expression) visit(context.expr()));
    }
    throw new IllegalArgumentException(
        String.format(
            Locale.ENGLISH, "cannot use expression %s as a left side of an assignment", column));
  }

  // Query specification

  @Override
  public Node visitQuery(SqlBaseParser.QueryContext context) {
    QueryBody term = (QueryBody) visit(context.queryTerm());
    if (term instanceof QuerySpecification) {
      // When we have a simple query specification
      // followed by order by limit, fold the order by and limit
      // clauses into the query specification (analyzer/planner
      // expects this structure to resolve references with respect
      // to columns defined in the query specification)
      QuerySpecification query = (QuerySpecification) term;

      return new Query(
          new QuerySpecification(
              query.getSelect(),
              query.getFrom(),
              query.getWhere(),
              query.getGroupBy(),
              query.getHaving(),
              query.getWindows(),
              visitCollection(context.sortItem(), SortItem.class),
              visitIfPresent(context.limit, Expression.class),
              visitIfPresent(context.offset, Expression.class)),
          emptyList(),
          Optional.empty(),
          Optional.empty());
    }
    return new Query(
        term,
        visitCollection(context.sortItem(), SortItem.class),
        visitIfPresent(context.limit, Expression.class),
        visitIfPresent(context.offset, Expression.class));
  }

  @Override
  public Node visitDefaultQuerySpec(SqlBaseParser.DefaultQuerySpecContext context) {
    List<SelectItem> selectItems = visitCollection(context.selectItem(), SelectItem.class);
    return new QuerySpecification(
        new Select(isDistinct(context.setQuant()), selectItems),
        visitCollection(context.relation(), Relation.class),
        visitIfPresent(context.where(), Expression.class),
        visitCollection(context.expr(), Expression.class),
        visitIfPresent(context.having, Expression.class),
        getWindowDefinitions(context.windows),
        emptyList(),
        Optional.empty(),
        Optional.empty());
  }

  @Override
  public Node visitValuesRelation(SqlBaseParser.ValuesRelationContext ctx) {
    return new Values(visitCollection(ctx.values(), ValuesList.class));
  }

  private Map<String, Window> getWindowDefinitions(
      List<SqlBaseParser.NamedWindowContext> windowContexts) {
    HashMap<String, Window> windows = new HashMap<>(windowContexts.size());
    for (SqlBaseParser.NamedWindowContext windowContext : windowContexts) {
      String name = getIdentText(windowContext.name);
      if (windows.containsKey(name)) {
        throw new IllegalArgumentException("Window " + name + " is already defined");
      }
      Window window = (Window) visit(windowContext.windowDefinition());

      // It prevents circular references in the window definitions
      // by failing if the window was referenced before it was defined
      // E.g. WINDOW w AS (ww), ww AS (w)
      if (window.windowRef() != null && !windows.containsKey(window.windowRef())) {
        throw new IllegalArgumentException("Window " + window.windowRef() + " does not exist");
      }
      windows.put(name, window);
    }
    return windows;
  }

  @Override
  public Node visitWhere(SqlBaseParser.WhereContext context) {
    return visit(context.condition);
  }

  @Override
  public Node visitSortItem(SqlBaseParser.SortItemContext context) {
    return new SortItem(
        (Expression) visit(context.expr()),
        Optional.ofNullable(context.ordering)
            .map(AstBuilder::getOrderingType)
            .orElse(SortItem.Ordering.ASCENDING),
        Optional.ofNullable(context.nullOrdering)
            .map(AstBuilder::getNullOrderingType)
            .orElse(SortItem.NullOrdering.UNDEFINED));
  }

  @Override
  public Node visitSetOperation(SqlBaseParser.SetOperationContext context) {
    switch (context.operator.getType()) {
      case SqlBaseLexer.UNION:
        QueryBody left = (QueryBody) visit(context.left);
        QueryBody right = (QueryBody) visit(context.right);
        boolean isDistinct = context.setQuant() == null || context.setQuant().ALL() == null;
        return new Union(left, right, isDistinct);
      case SqlBaseLexer.INTERSECT:
        QuerySpecification first = (QuerySpecification) visit(context.first);
        QuerySpecification second = (QuerySpecification) visit(context.second);
        return new Intersect(first, second);
      case SqlBaseLexer.EXCEPT:
        first = (QuerySpecification) visit(context.first);
        second = (QuerySpecification) visit(context.second);
        return new Except(first, second);
      default:
        throw new IllegalArgumentException(
            "Unsupported set operation: " + context.operator.getText());
    }
  }

  @Override
  public Node visitSelectAll(SqlBaseParser.SelectAllContext context) {
    if (context.qname() != null) {
      return new AllColumns(getQualifiedName(context.qname()));
    }
    return new AllColumns();
  }

  @Override
  public Node visitSelectSingle(SqlBaseParser.SelectSingleContext context) {
    return new SingleColumn((Expression) visit(context.expr()), getIdentText(context.ident()));
  }

  @Override
  public Node visitArraySubquery(SqlBaseParser.ArraySubqueryContext ctx) {
    return new ArraySubQueryExpression((SubqueryExpression) visit(ctx.subqueryExpression()));
  }

  /*
   * case sensitivity like it is in postgres
   * see also http://www.thenextage.com/wordpress/postgresql-case-sensitivity-part-1-the-ddl/
   *
   * unfortunately this has to be done in the parser because afterwards the
   * knowledge of the IDENT / QUOTED_IDENT difference is lost
   */
  @Override
  public Node visitUnquotedIdentifier(SqlBaseParser.UnquotedIdentifierContext context) {
    return new StringLiteral(context.getText().toLowerCase(Locale.ENGLISH));
  }

  @Override
  public Node visitQuotedIdentifier(SqlBaseParser.QuotedIdentifierContext context) {
    String token = context.getText();
    String identifier = token.substring(1, token.length() - 1).replace("\"\"", "\"");
    return new StringLiteral(identifier);
  }

  @Nullable
  private String getIdentText(@Nullable SqlBaseParser.IdentContext ident) {
    if (ident != null) {
      StringLiteral literal = (StringLiteral) visit(ident);
      return literal.getValue();
    }
    return null;
  }

  @Nullable
  private String getText(@Nullable ParseTree ident) {
    if (ident != null) {
      StringLiteral literal = (StringLiteral) visit(ident);
      return literal.getValue();
    }
    return null;
  }

  @Override
  public Node visitTableName(SqlBaseParser.TableNameContext ctx) {
    return new Table<>(getQualifiedName(ctx.qname()), false);
  }

  @Override
  public Node visitTableFunction(SqlBaseParser.TableFunctionContext ctx) {
    QualifiedName qualifiedName = getQualifiedName(ctx.qname());
    List<Expression> arguments = visitCollection(ctx.valueExpression(), Expression.class);
    return new TableFunction(new FunctionCall(qualifiedName, arguments));
  }

  // Boolean expressions

  @Override
  public Node visitLogicalNot(SqlBaseParser.LogicalNotContext context) {
    return new NotExpression((Expression) visit(context.booleanExpression()));
  }

  @Override
  public Node visitLogicalBinary(SqlBaseParser.LogicalBinaryContext context) {
    return new LogicalBinaryExpression(
        getLogicalBinaryOperator(context.operator),
        (Expression) visit(context.left),
        (Expression) visit(context.right));
  }

  // From clause

  @Override
  public Node visitJoinRelation(SqlBaseParser.JoinRelationContext context) {
    Relation left = (Relation) visit(context.left);
    Relation right;

    if (context.CROSS() != null) {
      right = (Relation) visit(context.right);
      return new Join(Join.Type.CROSS, left, right, Optional.empty());
    }

    JoinCriteria criteria;
    if (context.NATURAL() != null) {
      right = (Relation) visit(context.right);
      criteria = new NaturalJoin();
    } else {
      right = (Relation) visit(context.rightRelation);
      if (context.joinCriteria().ON() != null) {
        criteria = new JoinOn((Expression) visit(context.joinCriteria().booleanExpression()));
      } else if (context.joinCriteria().USING() != null) {
        List<String> columns = identsToStrings(context.joinCriteria().ident());
        criteria = new JoinUsing(columns);
      } else {
        throw new IllegalArgumentException("Unsupported join criteria");
      }
    }
    return new Join(getJoinType(context.joinType()), left, right, Optional.of(criteria));
  }

  private static Join.Type getJoinType(SqlBaseParser.JoinTypeContext joinTypeContext) {
    Join.Type joinType;
    if (joinTypeContext.LEFT() != null) {
      joinType = Join.Type.LEFT;
    } else if (joinTypeContext.RIGHT() != null) {
      joinType = Join.Type.RIGHT;
    } else if (joinTypeContext.FULL() != null) {
      joinType = Join.Type.FULL;
    } else {
      joinType = Join.Type.INNER;
    }
    return joinType;
  }

  @Override
  public Node visitAliasedRelation(SqlBaseParser.AliasedRelationContext context) {
    Relation child = (Relation) visit(context.relationPrimary());

    if (context.ident() == null) {
      return child;
    }
    return new AliasedRelation(
        child, getIdentText(context.ident()), getColumnAliases(context.aliasedColumns()));
  }

  @Override
  public Node visitSubqueryRelation(SqlBaseParser.SubqueryRelationContext context) {
    return new TableSubquery((Query) visit(context.query()));
  }

  @Override
  public Node visitParenthesizedRelation(SqlBaseParser.ParenthesizedRelationContext context) {
    return visit(context.relation());
  }

  // Predicates

  @Override
  public Node visitPredicated(SqlBaseParser.PredicatedContext context) {
    if (context.predicate() != null) {
      return visit(context.predicate());
    }
    return visit(context.valueExpression);
  }

  @Override
  public Node visitComparison(SqlBaseParser.ComparisonContext context) {
    return new ComparisonExpression(
        getComparisonOperator(((TerminalNode) context.cmpOp().getChild(0)).getSymbol()),
        (Expression) visit(context.value),
        (Expression) visit(context.right));
  }

  @Override
  public Node visitDistinctFrom(SqlBaseParser.DistinctFromContext context) {
    Expression expression =
        new ComparisonExpression(
            ComparisonExpression.Type.IS_DISTINCT_FROM,
            (Expression) visit(context.value),
            (Expression) visit(context.right));

    if (context.NOT() != null) {
      expression = new NotExpression(expression);
    }
    return expression;
  }

  @Override
  public Node visitBetween(SqlBaseParser.BetweenContext context) {
    Expression expression =
        new BetweenPredicate(
            (Expression) visit(context.value),
            (Expression) visit(context.lower),
            (Expression) visit(context.upper));

    if (context.NOT() != null) {
      expression = new NotExpression(expression);
    }
    return expression;
  }

  @Override
  public Node visitNullPredicate(SqlBaseParser.NullPredicateContext context) {
    Expression child = (Expression) visit(context.value);

    if (context.NOT() == null) {
      return new IsNullPredicate(child);
    }
    return new IsNotNullPredicate(child);
  }

  @Override
  public Node visitLike(SqlBaseParser.LikeContext context) {
    Expression escape = null;
    if (context.escape != null) {
      escape = (Expression) visit(context.escape);
    }

    boolean ignoreCase = context.LIKE() == null && context.ILIKE() != null;

    Expression result =
        new LikePredicate(
            (Expression) visit(context.value),
            (Expression) visit(context.pattern),
            escape,
            ignoreCase);

    if (context.NOT() != null) {
      result = new NotExpression(result);
    }
    return result;
  }

  @Override
  public Node visitArrayLike(SqlBaseParser.ArrayLikeContext context) {
    boolean inverse = context.NOT() != null;
    boolean ignoreCase = context.LIKE() == null && context.ILIKE() != null;
    return new ArrayLikePredicate(
        getComparisonQuantifier(
            ((TerminalNode) context.setCmpQuantifier().getChild(0)).getSymbol()),
        (Expression) visit(context.value),
        (Expression) visit(context.v),
        visitOptionalContext(context.escape, Expression.class),
        inverse,
        ignoreCase);
  }

  @Override
  public Node visitInList(SqlBaseParser.InListContext context) {
    Expression result =
        new InPredicate(
            (Expression) visit(context.value),
            new InListExpression(visitCollection(context.expr(), Expression.class)));

    if (context.NOT() != null) {
      result = new NotExpression(result);
    }
    return result;
  }

  @Override
  public Node visitInSubquery(SqlBaseParser.InSubqueryContext context) {
    Expression result =
        new InPredicate(
            (Expression) visit(context.value), (Expression) visit(context.subqueryExpression()));

    if (context.NOT() != null) {
      result = new NotExpression(result);
    }
    return result;
  }

  @Override
  public Node visitExists(SqlBaseParser.ExistsContext context) {
    return new ExistsPredicate((Query) visit(context.query()));
  }

  @Override
  public Node visitQuantifiedComparison(SqlBaseParser.QuantifiedComparisonContext context) {
    return new ArrayComparisonExpression(
        getComparisonOperator(((TerminalNode) context.cmpOp().getChild(0)).getSymbol()),
        getComparisonQuantifier(
            ((TerminalNode) context.setCmpQuantifier().getChild(0)).getSymbol()),
        (Expression) visit(context.value),
        (Expression) visit(context.primaryExpression()));
  }

  @Override
  public Node visitMatch(SqlBaseParser.MatchContext context) {
    SqlBaseParser.MatchPredicateIdentsContext predicateIdents = context.matchPredicateIdents();
    List<MatchPredicateColumnIdent> idents;

    if (predicateIdents.matchPred != null) {
      idents = singletonList((MatchPredicateColumnIdent) visit(predicateIdents.matchPred));
    } else {
      idents =
          visitCollection(predicateIdents.matchPredicateIdent(), MatchPredicateColumnIdent.class);
    }
    return new MatchPredicate(
        idents,
        (Expression) visit(context.term),
        getIdentText(context.matchType),
        extractGenericProperties(context.withProperties()));
  }

  @Override
  public Node visitMatchPredicateIdent(SqlBaseParser.MatchPredicateIdentContext context) {
    return new MatchPredicateColumnIdent(
        (Expression) visit(context.subscriptSafe()),
        visitOptionalContext(context.boost, Expression.class));
  }

  // Value expressions

  @Override
  public Node visitArithmeticUnary(SqlBaseParser.ArithmeticUnaryContext context) {
    switch (context.operator.getType()) {
      case SqlBaseLexer.MINUS:
        return new NegativeExpression((Expression) visit(context.valueExpression()));
      case SqlBaseLexer.PLUS:
        return visit(context.valueExpression());
      default:
        throw new UnsupportedOperationException("Unsupported sign: " + context.operator.getText());
    }
  }

  @Override
  public Node visitArithmeticBinary(SqlBaseParser.ArithmeticBinaryContext context) {
    return new ArithmeticExpression(
        getArithmeticBinaryOperator(context.operator),
        (Expression) visit(context.left),
        (Expression) visit(context.right));
  }

  @Override
  public Node visitConcatenation(SqlBaseParser.ConcatenationContext context) {
    return new FunctionCall(
        QualifiedName.of("concat"),
        ImmutableList.of((Expression) visit(context.left), (Expression) visit(context.right)));
  }

  @Override
  public Node visitOver(SqlBaseParser.OverContext context) {
    return visit(context.windowDefinition());
  }

  @Override
  public Node visitWindowDefinition(SqlBaseParser.WindowDefinitionContext context) {
    return new Window(
        getIdentText(context.windowRef),
        visitCollection(context.partition, Expression.class),
        visitCollection(context.sortItem(), SortItem.class),
        visitIfPresent(context.windowFrame(), WindowFrame.class));
  }

  @Override
  public Node visitWindowFrame(SqlBaseParser.WindowFrameContext ctx) {
    return new WindowFrame(
        getFrameType(ctx.frameType),
        (FrameBound) visit(ctx.start),
        visitIfPresent(ctx.end, FrameBound.class));
  }

  @Override
  public Node visitUnboundedFrame(SqlBaseParser.UnboundedFrameContext context) {
    return new FrameBound(getUnboundedFrameBoundType(context.boundType));
  }

  @Override
  public Node visitBoundedFrame(SqlBaseParser.BoundedFrameContext context) {
    return new FrameBound(
        getBoundedFrameBoundType(context.boundType), (Expression) visit(context.expr()));
  }

  @Override
  public Node visitCurrentRowBound(SqlBaseParser.CurrentRowBoundContext context) {
    return new FrameBound(FrameBound.Type.CURRENT_ROW);
  }

  @Override
  public Node visitDoubleColonCast(SqlBaseParser.DoubleColonCastContext context) {
    return new Cast(
        (Expression) visit(context.primaryExpression()), (ColumnType) visit(context.dataType()));
  }

  @Override
  public Node visitFromStringLiteralCast(SqlBaseParser.FromStringLiteralCastContext context) {
    ColumnType targetType = (ColumnType) visit(context.dataType());

    if (targetType instanceof CollectionColumnType || targetType instanceof ObjectColumnType) {
      throw new UnsupportedOperationException(
          "type 'string' cast notation only supports primitive types. "
              + "Use '::' or cast() operator instead.");
    }
    return new Cast((Expression) visit(context.stringLiteral()), targetType);
  }

  // Primary expressions

  @Override
  public Node visitCast(SqlBaseParser.CastContext context) {
    if (context.TRY_CAST() != null) {
      return new TryCast(
          (Expression) visit(context.expr()), (ColumnType) visit(context.dataType()));
    } else {
      return new Cast((Expression) visit(context.expr()), (ColumnType) visit(context.dataType()));
    }
  }

  @Override
  public Node visitSpecialDateTimeFunction(SqlBaseParser.SpecialDateTimeFunctionContext context) {
    CurrentTime.Type type = getDateTimeFunctionType(context.name);

    if (context.precision != null) {
      return new CurrentTime(type, Integer.parseInt(context.precision.getText()));
    }
    return new CurrentTime(type);
  }

  @Override
  public Node visitExtract(SqlBaseParser.ExtractContext context) {
    return new Extract(
        (Expression) visit(context.expr()),
        (StringLiteral) visit(context.stringLiteralOrIdentifier()));
  }

  @Override
  public Node visitSubstring(SqlBaseParser.SubstringContext context) {
    return new FunctionCall(
        QualifiedName.of("substr"), visitCollection(context.expr(), Expression.class));
  }

  @Override
  public Node visitAtTimezone(SqlBaseParser.AtTimezoneContext context) {
    Expression zone = (Expression) visit(context.zone);
    Expression timestamp = (Expression) visit(context.timestamp);
    return new FunctionCall(QualifiedName.of("timezone"), ImmutableList.of(zone, timestamp));
  }

  @Override
  public Node visitLeft(SqlBaseParser.LeftContext context) {
    Expression strOrColName = (Expression) visit(context.strOrColName);
    Expression len = (Expression) visit(context.len);
    return new FunctionCall(QualifiedName.of("left"), ImmutableList.of(strOrColName, len));
  }

  @Override
  public Node visitRight(SqlBaseParser.RightContext context) {
    Expression strOrColName = (Expression) visit(context.strOrColName);
    Expression len = (Expression) visit(context.len);
    return new FunctionCall(QualifiedName.of("right"), ImmutableList.of(strOrColName, len));
  }

  @Override
  public Node visitTrim(SqlBaseParser.TrimContext ctx) {
    Expression target = (Expression) visit(ctx.target);

    if (ctx.charsToTrim == null && ctx.trimMode == null) {
      return new FunctionCall(QualifiedName.of("trim"), ImmutableList.of(target));
    }

    Expression charsToTrim =
        visitIfPresent(ctx.charsToTrim, Expression.class).orElse(new StringLiteral(" "));
    StringLiteral trimMode = new StringLiteral(getTrimMode(ctx.trimMode).value());

    return new FunctionCall(
        QualifiedName.of("trim"), ImmutableList.of(target, charsToTrim, trimMode));
  }

  @Override
  public Node visitCurrentSchema(SqlBaseParser.CurrentSchemaContext context) {
    return new FunctionCall(QualifiedName.of("current_schema"), ImmutableList.of());
  }

  @Override
  public Node visitCurrentUser(SqlBaseParser.CurrentUserContext ctx) {
    return new FunctionCall(QualifiedName.of("current_user"), ImmutableList.of());
  }

  @Override
  public Node visitSessionUser(SqlBaseParser.SessionUserContext ctx) {
    return new FunctionCall(QualifiedName.of("session_user"), ImmutableList.of());
  }

  @Override
  public Node visitNestedExpression(SqlBaseParser.NestedExpressionContext context) {
    return visit(context.expr());
  }

  @Override
  public Node visitSubqueryExpression(SqlBaseParser.SubqueryExpressionContext context) {
    return new SubqueryExpression((Query) visit(context.query()));
  }

  @Override
  public Node visitDereference(SqlBaseParser.DereferenceContext context) {
    return new QualifiedNameReference(QualifiedName.of(identsToStrings(context.ident())));
  }

  @Override
  public Node visitRecordSubscript(SqlBaseParser.RecordSubscriptContext ctx) {
    return new RecordSubscript((Expression) visit(ctx.base), getIdentText(ctx.fieldName));
  }

  @Override
  public Node visitColumnReference(SqlBaseParser.ColumnReferenceContext context) {
    return new QualifiedNameReference(QualifiedName.of(getIdentText(context.ident())));
  }

  @Override
  public Node visitSubscript(SqlBaseParser.SubscriptContext context) {
    return new SubscriptExpression(
        (Expression) visit(context.value), (Expression) visit(context.index));
  }

  @Override
  public Node visitSubscriptSafe(SqlBaseParser.SubscriptSafeContext context) {
    if (context.qname() != null) {
      return new QualifiedNameReference(getQualifiedName(context.qname()));
    }
    return new SubscriptExpression(
        (Expression) visit(context.value), (Expression) visit(context.index));
  }

  @Override
  public Node visitQname(SqlBaseParser.QnameContext context) {
    return new QualifiedNameReference(getQualifiedName(context));
  }

  @Override
  public Node visitSimpleCase(SqlBaseParser.SimpleCaseContext context) {
    return new SimpleCaseExpression(
        (Expression) visit(context.operand),
        visitCollection(context.whenClause(), WhenClause.class),
        visitOptionalContext(context.elseExpr, Expression.class));
  }

  @Override
  public Node visitSearchedCase(SqlBaseParser.SearchedCaseContext context) {
    return new SearchedCaseExpression(
        visitCollection(context.whenClause(), WhenClause.class),
        visitOptionalContext(context.elseExpr, Expression.class));
  }

  @Override
  public Node visitIfCase(SqlBaseParser.IfCaseContext context) {
    return new IfExpression(
        (Expression) visit(context.condition),
        (Expression) visit(context.trueValue),
        visitIfPresent(context.falseValue, Expression.class));
  }

  @Override
  public Node visitWhenClause(SqlBaseParser.WhenClauseContext context) {
    return new WhenClause(
        (Expression) visit(context.condition), (Expression) visit(context.result));
  }

  @Override
  public Node visitFilter(SqlBaseParser.FilterContext context) {
    return visit(context.where());
  }

  @Override
  public Node visitFunctionCall(SqlBaseParser.FunctionCallContext context) {
    return new FunctionCall(
        getQualifiedName(context.qname()),
        isDistinct(context.setQuant()),
        visitCollection(context.expr(), Expression.class),
        visitIfPresent(context.over(), Window.class),
        visitIfPresent(context.filter(), Expression.class));
  }

  // Literals

  @Override
  public Node visitNullLiteral(SqlBaseParser.NullLiteralContext context) {
    return NullLiteral.INSTANCE;
  }

  @Override
  public Node visitBitString(BitStringContext ctx) {
    String text = ctx.BIT_STRING().getText();
    return BitString.ofBitString(text);
  }

  @Override
  public Node visitStringLiteral(SqlBaseParser.StringLiteralContext context) {
    return new StringLiteral(unquote(context.STRING().getText()));
  }

  @Override
  public Node visitEscapedCharsStringLiteral(SqlBaseParser.EscapedCharsStringLiteralContext ctx) {
    return new EscapedCharStringLiteral(unquoteEscaped(ctx.ESCAPED_STRING().getText()));
  }

  @Override
  public Node visitIntegerLiteral(SqlBaseParser.IntegerLiteralContext context) {
    long value = Long.parseLong(context.getText());
    if (value < Integer.MAX_VALUE + 1L) {
      return new IntegerLiteral((int) value);
    }
    return new LongLiteral(value);
  }

  @Override
  public Node visitDecimalLiteral(SqlBaseParser.DecimalLiteralContext context) {
    return new DoubleLiteral(context.getText());
  }

  @Override
  public Node visitBooleanLiteral(SqlBaseParser.BooleanLiteralContext context) {
    return context.TRUE() != null ? BooleanLiteral.TRUE_LITERAL : BooleanLiteral.FALSE_LITERAL;
  }

  @Override
  public Node visitArrayLiteral(SqlBaseParser.ArrayLiteralContext context) {
    return new ArrayLiteral(visitCollection(context.expr(), Expression.class));
  }

  @Override
  public Node visitEmptyArray(SqlBaseParser.EmptyArrayContext ctx) {
    return new ArrayLiteral(ImmutableList.of());
  }

  @Override
  public Node visitObjectLiteral(SqlBaseParser.ObjectLiteralContext context) {
    LinkedHashMap<String, Expression> expressions = new LinkedHashMap<>();
    context
        .objectKeyValue()
        .forEach(
            attr -> {
              String key = getIdentText(attr.key);
              Expression prevEntry = expressions.put(key, (Expression) visit(attr.value));
              if (prevEntry != null) {
                throw new IllegalArgumentException(
                    "Object literal cannot contain duplicate keys (`" + key + "`)");
              }
            });
    return new ObjectLiteral(expressions);
  }

  @Override
  public Node visitParameterPlaceholder(SqlBaseParser.ParameterPlaceholderContext context) {
    return new ParameterExpression(parameterPosition++);
  }

  @Override
  public Node visitPositionalParameter(SqlBaseParser.PositionalParameterContext context) {
    return new ParameterExpression(Integer.parseInt(context.integerLiteral().getText()));
  }

  @Override
  public Node visitOn(SqlBaseParser.OnContext context) {
    return BooleanLiteral.TRUE_LITERAL;
  }

  @Override
  public Node visitArrayDataType(SqlBaseParser.ArrayDataTypeContext ctx) {
    return new CollectionColumnType<>((ColumnType<?>) visit(ctx.dataType()));
  }

  @Override
  public Node visitObjectTypeDefinition(SqlBaseParser.ObjectTypeDefinitionContext context) {
    return new ObjectColumnType(
        getObjectType(context.type),
        visitCollection(context.columnDefinition(), ColumnDefinition.class));
  }

  @Override
  public Node visitMaybeParametrizedDataType(
      SqlBaseParser.MaybeParametrizedDataTypeContext context) {
    StringLiteral name = (StringLiteral) visit(context.baseDataType());
    List<Integer> parameters = new ArrayList<Integer>(context.integerLiteral().size());
    for (SqlBaseParser.IntegerLiteralContext param : context.integerLiteral()) {
      Node literal = visit(param);
      int val;
      if (literal instanceof LongLiteral) {
        val = Math.toIntExact(((LongLiteral) literal).getValue());
      } else {
        val = ((IntegerLiteral) literal).getValue();
      }
      parameters.add(val);
    }
    return new ColumnType<>(name.getValue(), parameters);
  }

  @Override
  public Node visitIdentDataType(SqlBaseParser.IdentDataTypeContext context) {
    return StringLiteral.fromObject(getIdentText(context.ident()));
  }

  @Override
  public Node visitDefinedDataType(SqlBaseParser.DefinedDataTypeContext context) {
    return StringLiteral.fromObject(
        context.children.stream()
            .map(c -> c.getText().toLowerCase(Locale.ENGLISH))
            .collect(Collectors.joining(" ")));
  }

  private static String getObjectType(Token type) {
    if (type == null) {
      return null;
    }
    switch (type.getType()) {
      case SqlBaseLexer.DYNAMIC:
        return type.getText().toLowerCase(Locale.ENGLISH);
      case SqlBaseLexer.STRICT:
        return type.getText().toLowerCase(Locale.ENGLISH);
      case SqlBaseLexer.IGNORED:
        return type.getText().toLowerCase(Locale.ENGLISH);
      default:
        throw new UnsupportedOperationException("Unsupported object type: " + type.getText());
    }
  }

  // Helpers

  @Override
  protected Node defaultResult() {
    return null;
  }

  @Override
  protected Node aggregateResult(Node aggregate, Node nextResult) {
    if (nextResult == null) {
      throw new UnsupportedOperationException("not yet implemented");
    }
    if (aggregate == null) {
      return nextResult;
    }

    throw new UnsupportedOperationException("not yet implemented");
  }

  @Nullable
  private <T> T visitOptionalContext(@Nullable ParserRuleContext context, Class<T> clazz) {
    if (context != null) {
      return clazz.cast(visit(context));
    }
    return null;
  }

  private <T> Optional<T> visitIfPresent(@Nullable ParserRuleContext context, Class<T> clazz) {
    return Optional.ofNullable(context).map(this::visit).map(clazz::cast);
  }

  private <T> List<T> visitCollection(List<? extends ParserRuleContext> contexts, Class<T> clazz) {
    return contexts.stream().map(this::visit).map(clazz::cast).collect(toList());
  }

  private static String unquote(String value) {
    return value.substring(1, value.length() - 1).replace("''", "'");
  }

  private static String unquoteEscaped(String value) {
    // start from index: 2 to account for the 'E' literal
    // single quote escaping is handled at later stage
    // as we require more context on the surrounding characters
    return value.substring(2, value.length() - 1);
  }

  private List<SelectItem> getReturningItems(@Nullable SqlBaseParser.ReturningContext context) {
    return context == null ? emptyList() : visitCollection(context.selectItem(), SelectItem.class);
  }

  private QualifiedName getQualifiedName(SqlBaseParser.QnameContext context) {
    return QualifiedName.of(identsToStrings(context.ident()));
  }

  private QualifiedName getQualifiedName(SqlBaseParser.IdentContext context) {
    return QualifiedName.of(getIdentText(context));
  }

  private List<QualifiedName> getQualifiedNames(SqlBaseParser.QnamesContext context) {
    ArrayList<QualifiedName> names = new ArrayList<>(context.qname().size());
    for (SqlBaseParser.QnameContext qnameContext : context.qname()) {
      names.add(getQualifiedName(qnameContext));
    }
    return names;
  }

  private List<String> identsToStrings(List<SqlBaseParser.IdentContext> idents) {
    return idents.stream().map(this::getIdentText).collect(toList());
  }

  private static boolean isDistinct(SqlBaseParser.SetQuantContext setQuantifier) {
    return setQuantifier != null && setQuantifier.DISTINCT() != null;
  }

  @Nullable
  private static String getUnquotedText(@Nullable ParserRuleContext context) {
    return context != null ? unquote(context.getText()) : null;
  }

  private List<String> getColumnAliases(SqlBaseParser.AliasedColumnsContext columnAliasesContext) {
    if (columnAliasesContext == null) {
      return emptyList();
    }
    return identsToStrings(columnAliasesContext.ident());
  }

  private static ArithmeticExpression.Type getArithmeticBinaryOperator(Token operator) {
    switch (operator.getType()) {
      case SqlBaseLexer.PLUS:
        return ArithmeticExpression.Type.ADD;
      case SqlBaseLexer.MINUS:
        return ArithmeticExpression.Type.SUBTRACT;
      case SqlBaseLexer.ASTERISK:
        return ArithmeticExpression.Type.MULTIPLY;
      case SqlBaseLexer.SLASH:
        return ArithmeticExpression.Type.DIVIDE;
      case SqlBaseLexer.PERCENT:
        return ArithmeticExpression.Type.MODULUS;
      default:
        throw new UnsupportedOperationException("Unsupported operator: " + operator.getText());
    }
  }

  private TrimMode getTrimMode(Token type) {
    if (type == null) {
      return TrimMode.BOTH;
    }
    switch (type.getType()) {
      case SqlBaseLexer.BOTH:
        return TrimMode.BOTH;
      case SqlBaseLexer.LEADING:
        return TrimMode.LEADING;
      case SqlBaseLexer.TRAILING:
        return TrimMode.TRAILING;
      default:
        throw new UnsupportedOperationException("Unsupported trim mode: " + type.getText());
    }
  }

  private static ComparisonExpression.Type getComparisonOperator(Token symbol) {
    switch (symbol.getType()) {
      case SqlBaseLexer.EQ:
        return ComparisonExpression.Type.EQUAL;
      case SqlBaseLexer.NEQ:
        return ComparisonExpression.Type.NOT_EQUAL;
      case SqlBaseLexer.LT:
        return ComparisonExpression.Type.LESS_THAN;
      case SqlBaseLexer.LTE:
        return ComparisonExpression.Type.LESS_THAN_OR_EQUAL;
      case SqlBaseLexer.GT:
        return ComparisonExpression.Type.GREATER_THAN;
      case SqlBaseLexer.GTE:
        return ComparisonExpression.Type.GREATER_THAN_OR_EQUAL;
      case SqlBaseLexer.LLT:
        return ComparisonExpression.Type.CONTAINED_WITHIN;
      case SqlBaseLexer.REGEX_MATCH:
        return ComparisonExpression.Type.REGEX_MATCH;
      case SqlBaseLexer.REGEX_NO_MATCH:
        return ComparisonExpression.Type.REGEX_NO_MATCH;
      case SqlBaseLexer.REGEX_MATCH_CI:
        return ComparisonExpression.Type.REGEX_MATCH_CI;
      case SqlBaseLexer.REGEX_NO_MATCH_CI:
        return ComparisonExpression.Type.REGEX_NO_MATCH_CI;
      default:
        throw new UnsupportedOperationException("Unsupported operator: " + symbol.getText());
    }
  }

  private static CurrentTime.Type getDateTimeFunctionType(Token token) {
    switch (token.getType()) {
      case SqlBaseLexer.CURRENT_DATE:
        return CurrentTime.Type.DATE;
      case SqlBaseLexer.CURRENT_TIME:
        return CurrentTime.Type.TIME;
      case SqlBaseLexer.CURRENT_TIMESTAMP:
        return CurrentTime.Type.TIMESTAMP;
      default:
        throw new UnsupportedOperationException("Unsupported special function: " + token.getText());
    }
  }

  private static LogicalBinaryExpression.Type getLogicalBinaryOperator(Token token) {
    switch (token.getType()) {
      case SqlBaseLexer.AND:
        return LogicalBinaryExpression.Type.AND;
      case SqlBaseLexer.OR:
        return LogicalBinaryExpression.Type.OR;
      default:
        throw new IllegalArgumentException("Unsupported operator: " + token.getText());
    }
  }

  private static SortItem.NullOrdering getNullOrderingType(Token token) {
    switch (token.getType()) {
      case SqlBaseLexer.FIRST:
        return SortItem.NullOrdering.FIRST;
      case SqlBaseLexer.LAST:
        return SortItem.NullOrdering.LAST;
      default:
        throw new IllegalArgumentException("Unsupported ordering: " + token.getText());
    }
  }

  private static SortItem.Ordering getOrderingType(Token token) {
    switch (token.getType()) {
      case SqlBaseLexer.ASC:
        return SortItem.Ordering.ASCENDING;
      case SqlBaseLexer.DESC:
        return SortItem.Ordering.DESCENDING;
      default:
        throw new IllegalArgumentException("Unsupported ordering: " + token.getText());
    }
  }

  private static String getClazz(Token token) {
    switch (token.getType()) {
      case SqlBaseLexer.SCHEMA:
        return SCHEMA;
      case SqlBaseLexer.TABLE:
        return TABLE;
      case SqlBaseLexer.VIEW:
        return VIEW;
      default:
        throw new IllegalArgumentException("Unsupported privilege class: " + token.getText());
    }
  }

  private static ArrayComparisonExpression.Quantifier getComparisonQuantifier(Token symbol) {
    switch (symbol.getType()) {
      case SqlBaseLexer.ALL:
        return ArrayComparisonExpression.Quantifier.ALL;
      case SqlBaseLexer.ANY:
        return ArrayComparisonExpression.Quantifier.ANY;
      case SqlBaseLexer.SOME:
        return ArrayComparisonExpression.Quantifier.ANY;
      default:
        throw new IllegalArgumentException("Unsupported quantifier: " + symbol.getText());
    }
  }

  private List<QualifiedName> getIdents(List<SqlBaseParser.QnameContext> qnames) {
    return qnames.stream().map(this::getQualifiedName).collect(toList());
  }

  private static void validateFunctionName(QualifiedName functionName) {
    if (functionName.getParts().size() > 2) {
      throw new IllegalArgumentException(
          String.format(
              Locale.ENGLISH,
              "The function name is not correct! "
                  + "name [%s] does not conform the [[schema_name .] function_name] format.",
              functionName));
    }
  }

  private ClassAndIdent getClassAndIdentsForPrivileges(
      boolean onCluster,
      SqlBaseParser.ClazzContext clazz,
      SqlBaseParser.QnamesContext qnamesContext) {
    if (onCluster) {
      return new ClassAndIdent(CLUSTER, emptyList());
    } else {
      return new ClassAndIdent(getClazz(clazz.getStart()), getIdents(qnamesContext.qname()));
    }
  }

  private static class ClassAndIdent {
    private final String clazz;
    private final List<QualifiedName> idents;

    ClassAndIdent(String clazz, List<QualifiedName> idents) {
      this.clazz = clazz;
      this.idents = idents;
    }
  }

  private static WindowFrame.Mode getFrameType(Token type) {
    switch (type.getType()) {
      case SqlBaseLexer.RANGE:
        return WindowFrame.Mode.RANGE;
      case SqlBaseLexer.ROWS:
        return WindowFrame.Mode.ROWS;
      default:
        throw new IllegalArgumentException("Unsupported frame type: " + type.getText());
    }
  }

  private static FrameBound.Type getBoundedFrameBoundType(Token token) {
    switch (token.getType()) {
      case SqlBaseLexer.PRECEDING:
        return FrameBound.Type.PRECEDING;
      case SqlBaseLexer.FOLLOWING:
        return FrameBound.Type.FOLLOWING;
      default:
        throw new IllegalArgumentException("Unsupported bound type: " + token.getText());
    }
  }

  private static FrameBound.Type getUnboundedFrameBoundType(Token token) {
    switch (token.getType()) {
      case SqlBaseLexer.PRECEDING:
        return FrameBound.Type.UNBOUNDED_PRECEDING;
      case SqlBaseLexer.FOLLOWING:
        return FrameBound.Type.UNBOUNDED_FOLLOWING;

      default:
        throw new IllegalArgumentException("Unsupported bound type: " + token.getText());
    }
  }
}
