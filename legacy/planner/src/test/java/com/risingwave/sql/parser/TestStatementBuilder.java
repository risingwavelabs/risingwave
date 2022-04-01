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

import static com.risingwave.sql.parser.TreeAssertions.assertFormattedSql;
import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.risingwave.sql.Literals;
import com.risingwave.sql.SqlFormatter;
import com.risingwave.sql.tree.ArrayComparisonExpression;
import com.risingwave.sql.tree.ArrayLikePredicate;
import com.risingwave.sql.tree.ArrayLiteral;
import com.risingwave.sql.tree.Assignment;
import com.risingwave.sql.tree.ComparisonExpression;
import com.risingwave.sql.tree.CopyFrom;
import com.risingwave.sql.tree.CreateFunction;
import com.risingwave.sql.tree.CreateTable;
import com.risingwave.sql.tree.CreateUser;
import com.risingwave.sql.tree.DeallocateStatement;
import com.risingwave.sql.tree.DefaultTraversalVisitor;
import com.risingwave.sql.tree.DenyPrivilege;
import com.risingwave.sql.tree.DropAnalyzer;
import com.risingwave.sql.tree.DropFunction;
import com.risingwave.sql.tree.DropSnapshot;
import com.risingwave.sql.tree.DropTable;
import com.risingwave.sql.tree.DropUser;
import com.risingwave.sql.tree.DropView;
import com.risingwave.sql.tree.EscapedCharStringLiteral;
import com.risingwave.sql.tree.Expression;
import com.risingwave.sql.tree.FunctionCall;
import com.risingwave.sql.tree.GrantPrivilege;
import com.risingwave.sql.tree.Insert;
import com.risingwave.sql.tree.IntegerLiteral;
import com.risingwave.sql.tree.KillStatement;
import com.risingwave.sql.tree.MatchPredicate;
import com.risingwave.sql.tree.NegativeExpression;
import com.risingwave.sql.tree.ObjectLiteral;
import com.risingwave.sql.tree.ParameterExpression;
import com.risingwave.sql.tree.QualifiedName;
import com.risingwave.sql.tree.QualifiedNameReference;
import com.risingwave.sql.tree.Query;
import com.risingwave.sql.tree.RevokePrivilege;
import com.risingwave.sql.tree.SetSessionAuthorizationStatement;
import com.risingwave.sql.tree.SetStatement;
import com.risingwave.sql.tree.ShowCreateTable;
import com.risingwave.sql.tree.Statement;
import com.risingwave.sql.tree.StringLiteral;
import com.risingwave.sql.tree.SubqueryExpression;
import com.risingwave.sql.tree.SubscriptExpression;
import com.risingwave.sql.tree.SwapTable;
import com.risingwave.sql.tree.Update;
import com.risingwave.sql.tree.Window;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;

/**
 * The test class for testing parsing statements from strings. Note that the statements here will
 * later be translated to Calcite statements.
 */
public class TestStatementBuilder {

  private static void printStatement(String sql) {
    println(sql.trim());
    println("");

    Statement statement = SqlParser.createStatement(sql);
    println(statement.toString());
    println("");

    // TODO: support formatting all statement types
    if (statement instanceof Query
        || statement instanceof CreateTable
        || statement instanceof CopyFrom
        || statement instanceof SwapTable
        || statement instanceof CreateFunction
        || statement instanceof CreateUser
        || statement instanceof GrantPrivilege
        || statement instanceof DenyPrivilege
        || statement instanceof RevokePrivilege
        || statement instanceof DropUser
        || statement instanceof DropAnalyzer
        || statement instanceof DropFunction
        || statement instanceof DropTable
        || statement instanceof DropView
        || statement instanceof DropSnapshot
        || statement instanceof Update
        || statement instanceof Insert
        || statement instanceof SetSessionAuthorizationStatement
        || statement instanceof Window) {
      println(SqlFormatter.formatSql(statement));
      println("");
      assertFormattedSql(statement);
    }

    println("=".repeat(60));
    println("");
  }

  private static void println(String s) {
    if (Boolean.parseBoolean(System.getProperty("printParse"))) {
      System.out.print(s + "\n");
    }
  }

  private static String getTpchQuery(int q) throws IOException {
    return readResource("com/risingwave/tpch/queries/" + q + ".sql");
  }

  private static void printTpchQuery(int query, Object... values) throws IOException {
    String sql = getTpchQuery(query);

    for (int i = values.length - 1; i >= 0; i--) {
      sql = sql.replaceAll(format(":%s", i + 1), String.valueOf(values[i]));
    }

    assertFalse("Not all bind parameters were replaced: " + sql, sql.matches("(?s).*:[0-9].*"));

    sql = fixTpchQuery(sql);
    printStatement(sql);
  }

  private static String readResource(String name) throws IOException {
    var inputStream = TestStatementBuilder.class.getClassLoader().getResourceAsStream(name);
    if (inputStream == null) {
      throw new IOException("'" + name + "' resource is not found");
    }
    return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
  }

  private static String fixTpchQuery(String s) {
    s = s.replaceFirst("(?m);$", "");
    s = s.replaceAll("(?m)^:[xo]$", "");
    s = s.replaceAll("(?m)^:n -1$", "");
    s = s.replaceAll("(?m)^:n ([0-9]+)$", "LIMIT $1");
    s = s.replace("day (3)", "day"); // for query 1
    return s;
  }

  @Test
  public void testBegin() {
    printStatement("BEGIN");
    printStatement("BEGIN WORK");
    printStatement("BEGIN WORK DEFERRABLE");
    printStatement("BEGIN TRANSACTION");
    printStatement("BEGIN TRANSACTION DEFERRABLE");
    printStatement(
        "BEGIN ISOLATION LEVEL SERIALIZABLE, " + "      READ WRITE," + "      NOT DEFERRABLE");
  }

  @Test
  public void testSetTransaction() throws Exception {
    printStatement("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");
    printStatement("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
    printStatement("SET TRANSACTION ISOLATION LEVEL READ COMMITTED");
    printStatement("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
    printStatement("SET TRANSACTION READ WRITE");
    printStatement("SET TRANSACTION READ ONLY");
    printStatement("SET TRANSACTION DEFERRABLE");
    printStatement("SET TRANSACTION NOT DEFERRABLE");
    printStatement("SET TRANSACTION READ WRITE, ISOLATION LEVEL SERIALIZABLE");
  }

  @Test
  public void test_analyze_statement_can_be_parsed() {
    printStatement("ANALYZE");
  }

  @Test
  public void test_discard_statement_parsing() throws Exception {
    printStatement("DISCARD ALL");
    printStatement("DISCARD PLANS");
    printStatement("DISCARD SEQUENCES");
    printStatement("DISCARD TEMPORARY");
    printStatement("DISCARD TEMP");
  }

  @Test
  public void testEmptyOverClauseAfterFunction() {
    printStatement("SELECT avg(x) OVER () FROM t");
  }

  @Test
  public void testOverClauseWithOrderBy() {
    printStatement("SELECT avg(x) OVER (ORDER BY x) FROM t");
    printStatement("SELECT avg(x) OVER (ORDER BY x DESC) FROM t");
    printStatement("SELECT avg(x) OVER (ORDER BY x DESC NULLS FIRST) FROM t");
  }

  @Test
  public void testOverClauseWithPartition() {
    printStatement("SELECT avg(x) OVER (PARTITION BY p) FROM t");
    printStatement("SELECT avg(x) OVER (PARTITION BY p1, p2, p3) FROM t");
  }

  @Test
  public void testOverClauseWithPartitionAndOrderBy() {
    printStatement("SELECT avg(x) OVER (PARTITION BY p ORDER BY x ASC) FROM t");
    printStatement("SELECT avg(x) OVER (PARTITION BY p1, p2, p3 ORDER BY x, y) FROM t");
  }

  @Test
  public void testOverClauseWithFrameClause() {
    printStatement("SELECT avg(x) OVER (ROWS BETWEEN 5 PRECEDING AND 10 FOLLOWING) FROM t");
    printStatement("SELECT avg(x) OVER (RANGE BETWEEN 5 PRECEDING AND 10 FOLLOWING) FROM t");
    printStatement("SELECT avg(x) OVER (ROWS UNBOUND PRECEDING) FROM t");
    printStatement("SELECT avg(x) OVER (ROWS BETWEEN UNBOUND PRECEDING AND CURRENT ROW) FROM t");
    printStatement("SELECT avg(x) OVER (ROWS BETWEEN 10 PRECEDING AND UNBOUND FOLLOWING) FROM t");
  }

  @Test
  public void test_over_references_empty_window_def() {
    printStatement("SELECT avg(x) OVER (w) FROM t WINDOW w AS ()");
    printStatement("SELECT avg(x) OVER w FROM t WINDOW w AS ()");
    printStatement("SELECT avg(x) OVER w, sum(x) OVER w FROM t WINDOW w AS ()");
  }

  @Test
  public void test_over_with_order_by_or_frame_references_empty_window_def() {
    printStatement("SELECT avg(x) OVER (w ORDER BY x) FROM t WINDOW w AS ()");
    printStatement("SELECT avg(x) OVER (w ROWS UNBOUND PRECEDING) FROM t WINDOW w AS ()");
  }

  @Test
  public void test_over_references_window_with_order_by_or_partition_by() {
    printStatement("SELECT avg(x) OVER w FROM t WINDOW w AS (PARTITION BY x)");
    printStatement("SELECT avg(x) OVER (w) FROM t WINDOW w AS (ORDER BY x)");
    printStatement("SELECT avg(x) OVER w FROM t WINDOW w AS (PARTITION BY x ORDER BY x)");
  }

  @Test
  public void test_multiple_window_definitions_referenced_in_over() {
    printStatement("SELECT avg(x) OVER w1, avg(x) OVER w2 FROM t WINDOW w1 AS (), w2 AS ()");
  }

  @Test
  public void test_over_does_not_reference_window_definitions_from_window_clause() {
    printStatement("SELECT x FROM t WINDOW w AS ()");
    printStatement("SELECT avg(x) OVER () FROM t WINDOW w AS () ");
  }

  @Test
  public void test_duplicates_in_window_definitions() {
    assertThrows(
        IllegalArgumentException.class,
        () -> printStatement("SELECT x FROM t WINDOW w AS (), w as ()"),
        "Window w is already defined");
  }

  //    @Test
  //    public void testAlterClusterGCDanglingArtifacts() {
  //        printStatement("ALTER CLUSTER GC DANGLING ARTIFACTS");
  //    }

  @Test
  public void test_circular_references_in_window_definitions() {
    assertThrows(
        IllegalArgumentException.class,
        () -> printStatement("SELECT x FROM t WINDOW w AS (ww), ww as (w), www as ()"),
        "Window ww does not exist");
  }

  @Test
  public void testCommit() {
    printStatement("COMMIT");
  }

  @Test
  public void testNullNotAllowedAsArgToExtractField() {
    assertThrows(
        ParsingException.class,
        () -> printStatement("select extract(null from x)"),
        "no viable alternative at input 'select extract(null'");
  }

  @Test
  public void testShowCreateTableStmtBuilder() {
    printStatement("show create table test");
    printStatement("show create table foo.test");
    printStatement("show create table \"select\"");
  }

  @Test
  public void testDropTableStmtBuilder() {
    printStatement("drop table test");
    printStatement("drop table if exists test");
    printStatement("drop table bar.foo");
  }

  @Test
  public void testSwapTable() {
    printStatement("ALTER CLUSTER SWAP TABLE t1 TO t2");
    printStatement("ALTER CLUSTER SWAP TABLE t1 TO t2 WITH (prune_second = true)");
  }

  @Test
  public void testAlterClusterDecommissionNode() {
    printStatement("ALTER CLUSTER DECOMMISSION 'node1'");
  }

  @Test
  public void test_decommission_node_id_is_missing() {
    assertThrows(
        ParsingException.class,
        () -> printStatement("ALTER CLUSTER DECOMMISSION'"),
        "mismatched input");
  }

  @Test
  public void testStmtWithSemicolonBuilder() {
    printStatement("select 1;");
  }

  @Test
  public void testShowTablesStmtBuilder() {
    printStatement("show tables");
    printStatement("show tables like '.*'");
    printStatement("show tables from table_schema");
    printStatement("show tables from \"tableSchema\"");
    printStatement("show tables in table_schema");
    printStatement("show tables from foo like '.*'");
    printStatement("show tables in foo like '.*'");
    printStatement("show tables from table_schema like '.*'");
    printStatement("show tables in table_schema like '*'");
    printStatement("show tables in table_schema where name = 'foo'");
    printStatement("show tables in table_schema where name > 'foo'");
    printStatement("show tables in table_schema where name != 'foo'");
  }

  @Test
  public void testShowColumnsStmtBuilder() {
    printStatement("show columns from table_name");
    printStatement("show columns in table_name");
    printStatement("show columns from table_name from table_schema");
    printStatement("show columns in table_name from table_schema");
    printStatement("show columns in foo like '*'");
    printStatement("show columns from foo like '*'");
    printStatement("show columns from table_name from table_schema like '*'");
    printStatement("show columns in table_name from table_schema like '*'");
    printStatement("show columns from table_name where column_name = 'foo'");
    printStatement("show columns from table_name from table_schema where column_name = 'foo'");
  }

  @Test
  public void testDeleteFromStmtBuilder() {
    printStatement("delete from foo as alias");
    printStatement("delete from foo");
    printStatement("delete from schemah.foo where foo.a=foo.b and a is not null");
    printStatement("delete from schemah.foo as alias where foo.a=foo.b and a is not null");
  }

  @Test
  public void testShowSchemasStmtBuilder() {
    printStatement("show schemas");
    printStatement("show schemas like 'doc%'");
    printStatement("show schemas where schema_name='doc'");
    printStatement("show schemas where schema_name LIKE 'd%'");
  }

  @Test
  public void testShowParameterStmtBuilder() {
    printStatement("show search_path");
    printStatement("show all");
  }

  @Test
  public void testUpdateStmtBuilder() {
    printStatement("update foo set \"column['looks_like_nested']\"=1");
    printStatement("update foo set foo.a='b'");
    printStatement("update bar.foo set bar.foo.t=3");
    printStatement("update foo set col['x'] = 3");
    printStatement("update foo set col['x'] = 3 where foo['x'] = 2");
    printStatement("update schemah.foo set foo.a='b', foo.b=foo.a");
    printStatement("update schemah.foo set foo.a=abs(-6.3334), x=true where x=false");
  }

  @Test
  public void test_update_returning_clause() {
    printStatement("update foo set foo='a' returning id");
    printStatement("update foo set foo='a' where x=false returning id");
    printStatement("update foo set foo='a' returning id AS foo");
    printStatement("update foo set foo='a' returning id + 1 AS foo, id -1 as bar");
    printStatement("update foo set foo='a' returning \"column['nested']\" AS bar");
    printStatement("update foo set foo='a' returning foo.bar - 1 AS foo");
    printStatement("update foo set foo='a' returning *");
    printStatement("update foo set foo='a' returning foo.*");
  }

  @Test
  public void testExplainStmtBuilder() {
    printStatement("explain drop table foo");
    printStatement("explain analyze drop table foo");
  }

  @Test
  public void testSetStmtBuiler() {
    printStatement("set session some_setting = 1, ON");
    printStatement("set session some_setting = false");
    printStatement("set session some_setting = DEFAULT");
    printStatement("set session some_setting = 1, 2, 3");
    printStatement("set session some_setting = ON");
    printStatement("set session some_setting = 'value'");

    printStatement("set session some_setting TO DEFAULT");
    printStatement("set session some_setting TO 'value'");
    printStatement("set session some_setting TO 1, 2, 3");
    printStatement("set session some_setting TO ON");
    printStatement("set session some_setting TO true");
    printStatement("set session some_setting TO 1, ON");

    printStatement("set local some_setting = DEFAULT");
    printStatement("set local some_setting = 'value'");
    printStatement("set local some_setting = 1, 2, 3");
    printStatement("set local some_setting = 1, ON");
    printStatement("set local some_setting = ON");
    printStatement("set local some_setting = false");

    printStatement("set local some_setting TO DEFAULT");
    printStatement("set local some_setting TO 'value'");
    printStatement("set local some_setting TO 1, 2, 3");
    printStatement("set local some_setting TO ON");
    printStatement("set local some_setting TO true");
    printStatement("set local some_setting TO ALWAYS");

    printStatement("set some_setting TO 1, 2, 3");
    printStatement("set some_setting TO ON");

    printStatement("set session characteristics as transaction isolation level read uncommitted");
  }

  @Test
  public void test_set_session_authorization_statement_with_user() {
    printStatement("set session authorization 'test'");
    printStatement("set session authorization test");
    printStatement("set session session authorization test");
    printStatement("set local session authorization 'test'");
  }

  @Test
  public void test_set_session_authorization_statement_with_default_user() {
    printStatement("set session authorization default");
    printStatement("set session session authorization default");
    printStatement("set local session authorization default");
  }

  @Test
  public void test_reset_session_authorization_statement() {
    printStatement("reset session authorization");
  }

  @Test
  public void testKillStmtBuilder() {
    printStatement("kill all");
    printStatement("kill '6a3d6fb6-1401-4333-933d-b38c9322fca7'");
    printStatement("kill ?");
    printStatement("kill $1");
  }

  @Test
  public void testKillJob() {
    KillStatement stmt = (KillStatement) SqlParser.createStatement("KILL $1");
    assertThat(stmt.jobId(), is(notNullValue()));
  }

  @Test
  public void testKillAll() {
    Statement stmt = SqlParser.createStatement("KILL ALL");
    assertThat(stmt, is(new KillStatement(null)));
  }

  @Test
  public void testDeallocateStmtBuilder() {
    printStatement("deallocate all");
    printStatement("deallocate prepare all");
    printStatement("deallocate 'myStmt'");
    printStatement("deallocate myStmt");
    printStatement("deallocate test.prep.stmt");
  }

  @Test
  public void testDeallocateWithoutParamThrowsParsingException() {
    assertThrows(
        ParsingException.class,
        () -> printStatement("deallocate"),
        "line 1:11: mismatched input '<EOF>'");
  }

  @Test
  public void testDeallocate() {
    DeallocateStatement stmt =
        (DeallocateStatement) SqlParser.createStatement("DEALLOCATE test_prep_stmt");
    assertThat(stmt.preparedStmt().toString(), is("'test_prep_stmt'"));
    stmt = (DeallocateStatement) SqlParser.createStatement("DEALLOCATE 'test_prep_stmt'");
    assertThat(stmt.preparedStmt().toString(), is("'test_prep_stmt'"));
  }

  @Test
  public void testDeallocateAll() {
    Statement stmt = SqlParser.createStatement("DEALLOCATE ALL");
    assertTrue(stmt.equals(new DeallocateStatement()));
  }

  @Test
  public void testRefreshStmtBuilder() {
    printStatement("refresh table t");
    printStatement("refresh table t partition (pcol='val'), tableh partition (pcol='val')");
    printStatement("refresh table schemah.tableh");
    printStatement("refresh table tableh partition (pcol='val')");
    printStatement("refresh table tableh partition (pcol=?)");
    printStatement("refresh table tableh partition (pcol['nested'] = ?)");
  }

  @Test
  public void testOptimize() {
    printStatement("optimize table t");
    printStatement("optimize table t1, t2");
    printStatement("optimize table schema.t");
    printStatement("optimize table schema.t1, schema.t2");
    printStatement("optimize table t partition (pcol='val')");
    printStatement("optimize table t partition (pcol=?)");
    printStatement("optimize table t partition (pcol['nested'] = ?)");
    printStatement("optimize table t partition (pcol='val') with (param1=val1, param2=val2)");
    printStatement("optimize table t1 partition (pcol1='val1'), t2 partition (pcol2='val2')");
    printStatement(
        "optimize table t1 partition (pcol1='val1'), t2 partition (pcol2='val2') "
            + "with (param1=val1, param2=val2, param3='val3')");
  }

  @Test
  public void testSetSessionInvalidSetting() {
    assertThrows(
        ParsingException.class,
        () -> printStatement("set session 'some_setting' TO 1, ON"),
        "no viable alternative");
  }

  @Test
  public void testSetGlobal() {
    printStatement("set global sys.cluster['some_settings']['3'] = '1'");
    printStatement("set global sys.cluster['some_settings'] = '1', other_setting = 2");
    printStatement("set global transient sys.cluster['some_settings'] = '1'");
    printStatement("set global persistent sys.cluster['some_settings'] = '1'");
  }

  @Test
  public void testSetLicenseStmtBuilder() {
    printStatement("set license 'LICENSE_KEY'");
  }

  @Test
  public void testSetLicenseInputWithoutQuotesThrowsParsingException() {
    assertThrows(
        ParsingException.class,
        () -> printStatement("set license LICENSE_KEY"),
        "no viable alternative at input");
  }

  @Test
  public void testSetLicenseWithoutParamThrowsParsingException() {
    assertThrows(
        ParsingException.class,
        () -> printStatement("set license"),
        "no viable alternative at input 'set license'");
  }

  @Test
  public void testSetLicenseLikeAnExpressionThrowsParsingException() {
    assertThrows(
        ParsingException.class,
        () -> printStatement("set license key='LICENSE_KEY'"),
        "no viable alternative at input");
  }

  @Test
  public void testSetLicenseMultipleInputThrowsParsingException() {
    assertThrows(
        ParsingException.class,
        () -> printStatement("set license 'LICENSE_KEY' 'LICENSE_KEY2'"),
        "line 1:27: extraneous input ''LICENSE_KEY2'' expecting {<EOF>, ';'}");
  }

  @Test
  public void testSetLicense() {
    SetStatement<?> stmt = (SetStatement) SqlParser.createStatement("set license 'LICENSE_KEY'");
    assertThat(stmt.scope(), is(SetStatement.Scope.LICENSE));
    assertThat(stmt.settingType(), is(SetStatement.SettingType.PERSISTENT));
    assertThat(stmt.assignments().size(), is(1));

    Assignment<?> assignment = stmt.assignments().get(0);
    assertThat(assignment.expressions().size(), is(1));
    assertThat(assignment.expressions().get(0).toString(), is("'LICENSE_KEY'"));
  }

  @Test
  public void testResetGlobalStmtBuilder() {
    printStatement("reset global some_setting['nested'], other_setting");
  }

  @Test
  public void testAlterTableStmtBuilder() {
    printStatement("alter table t add foo integer");
    printStatement("alter table t add foo['1']['2'] integer");

    printStatement("alter table t set (number_of_replicas=4)");
    printStatement("alter table schema.t set (number_of_replicas=4)");
    printStatement("alter table t reset (number_of_replicas)");
    printStatement("alter table t reset (property1, property2, property3)");

    printStatement("alter table t add foo integer");
    printStatement("alter table t add column foo integer");
    printStatement("alter table t add foo integer primary key");
    printStatement("alter table t add foo string index using fulltext");
    printStatement("alter table t add column foo['x'] integer");
    printStatement("alter table t add column foo integer");

    printStatement("alter table t add column foo['x'] integer");
    printStatement("alter table t add column foo['x']['y'] object as (z integer)");

    printStatement("alter table t partition (partitioned_col=1) set (number_of_replicas=4)");
    printStatement("alter table only t set (number_of_replicas=4)");
  }

  @Test
  public void testCreateTableStmtBuilder() {
    printStatement("create table if not exists t (id integer primary key, name string)");
    printStatement("create table t (id double precision)");
    printStatement("create table t (id character varying)");
    printStatement("create table t (id integer primary key, value array(double precision))");
    printStatement("create table t (id integer, value double precision not null)");
    printStatement("create table t (id integer primary key, name string)");
    printStatement("create table t (id integer primary key, name string) clustered into 3 shards");
    printStatement("create table t (id integer primary key, name string) clustered into ? shards");
    printStatement("create table t (id integer primary key, name string) clustered by (id)");
    printStatement(
        "create table t (id integer primary key, name string) clustered by (id) into 4 shards");
    printStatement(
        "create table t (id integer primary key, name string) clustered by (id) into ? shards");
    printStatement(
        "create table t (id integer primary key, name string) with (number_of_replicas=4)");
    printStatement(
        "create table t (id integer primary key, name string) with (number_of_replicas=?)");
    printStatement(
        "create table t (id integer primary key, name string) "
            + "clustered by (id) with (number_of_replicas=4)");
    printStatement(
        "create table t (id integer primary key, name string) "
            + "clustered by (id) into 999 shards with (number_of_replicas=4)");
    printStatement(
        "create table t (id integer primary key, name string) with (number_of_replicas=-4)");
    printStatement("create table t (o object(dynamic) as (i integer, d double))");
    printStatement("create table t (id integer, name string, primary key (id))");
    printStatement(
        "create table t ("
            + "  \"_i\" integer, "
            + "  \"in\" int,"
            + "  \"Name\" string, "
            + "  bo boolean,"
            + "  \"by\" byte,"
            + "  sh short,"
            + "  lo long,"
            + "  fl float,"
            + "  do double,"
            + "  \"ip_\" ip,"
            + "  ti timestamp with time zone,"
            + "  ob object"
            + ")");
    printStatement("create table \"TABLE\" (o object(dynamic))");
    printStatement("create table \"TABLE\" (o object(strict))");
    printStatement("create table \"TABLE\" (o object(ignored))");
    printStatement(
        "create table \"TABLE\" (o object(strict) as "
            + "(inner_col object as "
            + "(sub_inner_col timestamp with time zone, another_inner_col string)))");

    printStatement("create table test (col1 int, col2 timestamp with time zone not null)");
    printStatement(
        "create table test (col1 int primary key not null, col2 timestamp with time zone)");

    printStatement(
        "create table t ("
            + "name string index off, "
            + "another string index using plain, "
            + "\"full\" string index using fulltext,"
            + "analyzed string index using fulltext with (analyzer='german', param=?, list=[1,2,3])"
            + ")");
    printStatement(
        "create table test (col1 string, col2 string,"
            + "index \"_col1_ft\" using fulltext(col1))");
    printStatement(
        "create table test (col1 string, col2 string,"
            + "index col1_col2_ft using fulltext(col1, col2) with (analyzer='custom'))");

    printStatement(
        "create table test (col1 int, col2 timestamp with time zone) partitioned by (col1)");
    printStatement(
        "create table test (col1 int, col2 timestamp with time zone) partitioned by (col1, col2)");
    printStatement(
        "create table test (col1 int, col2 timestamp with time zone) "
            + "partitioned by (col1) clustered by (col2)");
    printStatement(
        "create table test (col1 int, col2 timestamp with time zone) "
            + "clustered by (col2) partitioned by (col1)");
    printStatement(
        "create table test (col1 int, col2 object as (col3 timestamp with time zone)) "
            + "partitioned by (col2['col3'])");
    printStatement("create table test (col1 object as (col3 timestamp without time zone))");
    printStatement("create table test (col1 int, col2 timestamp without time zone not null)");

    printStatement("create table test (col1 string storage with (columnstore = false))");
  }

  @Test
  public void testCreateTableColumnTypeOrGeneratedExpressionAreDefined() {
    assertThrows(
        IllegalArgumentException.class,
        () -> printStatement("create table test (col1)"),
        "Column [col1]: data type "
            + "needs to be provided or column should be defined as a generated expression");
  }

  @Test
  public void testCreateTableDefaultExpression() {
    printStatement("create table test (col1 int default 1)");
    printStatement("create table test (col1 int default random())");
  }

  @Test
  public void testCreateTableBothDefaultAndGeneratedExpressionsNotAllowed() {
    assertThrows(
        IllegalArgumentException.class,
        () -> printStatement("create table test (col1 int default random() as 1+1)"),
        "Column [col1]: the default and generated expressions are mutually exclusive");
  }

  @Test
  public void testCreateTableOptionsMultipleTimesNotAllowed() {
    assertThrows(
        ParsingException.class,
        () ->
            printStatement(
                "create table test (col1 int, col2 timestamp with time zone) "
                    + "partitioned by (col1) partitioned by (col2)"),
        "line 1:83: mismatched input 'partitioned' expecting {<EOF>, ';'}");
  }

  @Test
  public void testCreateAnalyzerStmtBuilder() {
    printStatement("create analyzer myAnalyzer ( tokenizer german )");
    printStatement(
        "create analyzer my_analyzer ("
            + " token_filters ("
            + "   filter_1,"
            + "   filter_2,"
            + "   filter_3 WITH ("
            + "     \"key\"=?"
            + "   )"
            + " ),"
            + " tokenizer my_tokenizer WITH ("
            + "   property='value',"
            + "   property_list=['l', 'i', 's', 't']"
            + " ),"
            + " char_filters ("
            + "   filter_1,"
            + "   filter_2 WITH ("
            + "     key='property'"
            + "   ),"
            + "   filter_3"
            + " )"
            + ")");
    printStatement(
        "create analyzer \"My_Builtin\" extends builtin WITH (" + "  over='write'" + ")");
  }

  @Test
  public void testDropAnalyzer() {
    printStatement("drop analyzer my_analyzer");
  }

  @Test
  public void testCreateUserStmtBuilder() {
    printStatement("create user \"Günter\"");
    printStatement("create user root");
    printStatement("create user foo with (password = 'foo')");
  }

  @Test
  public void testDropUserStmtBuilder() {
    printStatement("drop user \"Günter\"");
    printStatement("drop user root");
    printStatement("drop user if exists root");
  }

  @Test
  public void testGrantPrivilegeStmtBuilder() {
    printStatement("grant DML To \"Günter\"");
    printStatement("grant DQL, DDL to root");
    printStatement("grant DQL, DDL to root, wolfie, anna");
    printStatement("grant ALL PRIVILEGES to wolfie");
    printStatement("grant ALL PRIVILEGES to wolfie, anna");
    printStatement("grant ALL to wolfie, anna");
    printStatement("grant ALL to anna");
  }

  @Test
  public void testGrantOnSchemaPrivilegeStmtBuilder() {
    printStatement("grant DML ON SCHEMA my_schema To \"Günter\"");
    printStatement("grant DQL, DDL ON SCHEMA my_schema to root");
    printStatement("grant DQL, DDL ON SCHEMA my_schema to root, wolfie, anna");
    printStatement("grant ALL PRIVILEGES ON SCHEMA my_schema to wolfie");
    printStatement("grant ALL PRIVILEGES ON SCHEMA my_schema to wolfie, anna");
    printStatement("grant ALL ON SCHEMA my_schema to wolfie, anna");
    printStatement("grant ALL ON SCHEMA my_schema to anna");
    printStatement("grant ALL ON SCHEMA my_schema, banana, tree to anna, nyan, cat");
  }

  @Test
  public void testGrantOnTablePrivilegeStmtBuilder() {
    printStatement("grant DML ON TABLE my_schema.t To \"Günter\"");
    printStatement("grant DQL, DDL ON TABLE my_schema.t to root");
    printStatement("grant DQL, DDL ON TABLE my_schema.t to root, wolfie, anna");
    printStatement("grant ALL PRIVILEGES ON TABLE my_schema.t to wolfie");
    printStatement("grant ALL PRIVILEGES ON TABLE my_schema.t to wolfie, anna");
    printStatement("grant ALL ON TABLE my_schema.t to wolfie, anna");
    printStatement("grant ALL ON TABLE my_schema.t to anna");
    printStatement("grant ALL ON TABLE my_schema.t, banana.b, tree to anna, nyan, cat");
  }

  @Test
  public void testDenyPrivilegeStmtBuilder() {
    printStatement("deny DML To \"Günter\"");
    printStatement("deny DQL, DDL to root");
    printStatement("deny DQL, DDL to root, wolfie, anna");
    printStatement("deny ALL PRIVILEGES to wolfie");
    printStatement("deny ALL PRIVILEGES to wolfie, anna");
    printStatement("deny ALL to wolfie, anna");
    printStatement("deny ALL to anna");
    printStatement("deny dml to anna");
    printStatement("deny ddl, dql to anna");
  }

  @Test
  public void testDenyOnSchemaPrivilegeStmtBuilder() {
    printStatement("deny DML ON SCHEMA my_schema To \"Günter\"");
    printStatement("deny DQL, DDL ON SCHEMA my_schema to root");
    printStatement("deny DQL, DDL ON SCHEMA my_schema to root, wolfie, anna");
    printStatement("deny ALL PRIVILEGES ON SCHEMA my_schema to wolfie");
    printStatement("deny ALL PRIVILEGES ON SCHEMA my_schema to wolfie, anna");
    printStatement("deny ALL ON SCHEMA my_schema to wolfie, anna");
    printStatement("deny ALL ON SCHEMA my_schema to anna");
    printStatement("deny ALL ON SCHEMA my_schema, banana, tree to anna, nyan, cat");
  }

  @Test
  public void testDenyOnTablePrivilegeStmtBuilder() {
    printStatement("deny DML ON TABLE my_schema.t To \"Günter\"");
    printStatement("deny DQL, DDL ON TABLE my_schema.t to root");
    printStatement("deny DQL, DDL ON TABLE my_schema.t to root, wolfie, anna");
    printStatement("deny ALL PRIVILEGES ON TABLE my_schema.t to wolfie");
    printStatement("deny ALL PRIVILEGES ON TABLE my_schema.t to wolfie, anna");
    printStatement("deny ALL ON TABLE my_schema.t to wolfie, anna");
    printStatement("deny ALL ON TABLE my_schema.t to anna");
    printStatement("deny ALL ON TABLE my_schema.t, banana.b, tree to anna, nyan, cat");
  }

  @Test
  public void testRevokePrivilegeStmtBuilder() {
    printStatement("revoke DML from \"Günter\"");
    printStatement("revoke DQL, DDL from root");
    printStatement("revoke DQL, DDL from root, wolfie, anna");
    printStatement("revoke ALL PRIVILEGES from wolfie");
    printStatement("revoke ALL from wolfie");
    printStatement("revoke ALL PRIVILEGES from wolfie, anna, herald");
    printStatement("revoke ALL from wolfie, anna, herald");
  }

  @Test
  public void testRevokeOnSchemaPrivilegeStmtBuilder() {
    printStatement("revoke DML ON SCHEMA my_schema from \"Günter\"");
    printStatement("revoke DQL, DDL ON SCHEMA my_schema from root");
    printStatement("revoke DQL, DDL ON SCHEMA my_schema from root, wolfie, anna");
    printStatement("revoke ALL PRIVILEGES ON SCHEMA my_schema from wolfie");
    printStatement("revoke ALL ON SCHEMA my_schema from wolfie");
    printStatement("revoke ALL PRIVILEGES ON SCHEMA my_schema from wolfie, anna, herald");
    printStatement("revoke ALL ON SCHEMA my_schema from wolfie, anna, herald");
    printStatement("revoke ALL ON SCHEMA my_schema, banana, tree from anna, nyan, cat");
  }

  @Test
  public void testRevokeOnTablePrivilegeStmtBuilder() {
    printStatement("revoke DML ON TABLE my_schema.t from \"Günter\"");
    printStatement("revoke DQL, DDL ON TABLE my_schema.t from root");
    printStatement("revoke DQL, DDL ON TABLE my_schema.t from root, wolfie, anna");
    printStatement("revoke ALL PRIVILEGES ON TABLE my_schema.t from wolfie");
    printStatement("revoke ALL ON TABLE my_schema.t from wolfie");
    printStatement("revoke ALL PRIVILEGES ON TABLE my_schema.t from wolfie, anna, herald");
    printStatement("revoke ALL ON TABLE my_schema.t from wolfie, anna, herald");
    printStatement("revoke ALL ON TABLE my_schema.t, banana.b, tree from anna, nyan, cat");
  }

  @Test
  public void testCreateFunctionStmtBuilder() {
    printStatement("create function foo.bar() returns boolean language ? as ?");
    printStatement("create function foo.bar() returns boolean language $1 as $2");

    // create or replace function
    printStatement(
        "create function foo.bar(int, long)"
            + " returns int"
            + " language javascript"
            + " as 'function(a, b) {return a + b}'");
    printStatement(
        "create function bar(array(int))"
            + " returns array(int) "
            + " language javascript"
            + " as 'function(a) {return [a]}'");
    printStatement(
        "create function bar()"
            + " returns string"
            + " language javascript"
            + " as 'function() {return \"\"}'");
    printStatement(
        "create or replace function bar()"
            + " returns string"
            + " language javascript as 'function() {return \"1\"}'");

    // argument with names
    printStatement(
        "create function foo.bar(\"f\" int, s object)"
            + " returns object"
            + " language javascript as 'function(f, s) {return {\"a\": 1}}'");
    printStatement(
        "create function foo.bar(location geo_point, geo_shape)"
            + " returns boolean"
            + " language javascript as 'function(location, b) {return true;}'");
  }

  @Test
  public void testCreateFunctionStmtBuilderWithIncorrectFunctionName() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            printStatement(
                "create function foo.bar.a()" + " returns object" + " language sql as 'select 1'"),
        "[foo.bar.a] does not conform the " + "[[schema_name .] function_name] format");
  }

  @Test
  public void testDropFunctionStmtBuilder() {
    printStatement("drop function bar(int)");
    printStatement("drop function foo.bar(obj object)");
    printStatement("drop function if exists foo.bar(obj object)");
  }

  @Test
  public void testSelectStmtBuilder() {
    printStatement(
        "select ab"
            + " from (select (ii + y) as iiy, concat(a, b) as ab"
            + " from (select t1.a, t2.b, t2.y, (t1.i + t2.i) as ii "
            + " from t1, t2 where t1.a='a' or t2.b='aa') as t)"
            + " as tt order by iiy");
    printStatement("select extract(day from x) from y");
    printStatement("select * from foo order by 1, 2 limit 1 offset ?");
    printStatement("select * from foo a (x, y, z)");
    printStatement("select *, 123, * from foo");
    printStatement("select show from foo");
    printStatement("select extract(day from x), extract('day' from x) from y");

    printStatement("select 1 + 13 || '15' from foo");
    printStatement("select \"test\" from foo");
    printStatement("select col['x'] + col['y'] from foo");
    printStatement("select col['x'] - col['y'] from foo");
    printStatement("select col['y'] / col[2 / 1] from foo");
    printStatement("select col[1] from foo");

    printStatement("select - + 10");
    printStatement("select - ( - - 10)");
    printStatement("select - ( + - 10) * - ( - 10 - + 10)");
    printStatement("select - - col['x']");

    //         expressions as subscript index are only supported by the parser
    printStatement("select col[1 + 2] - col['y'] from foo");

    printStatement("select x is distinct from y from foo where a is not distinct from b");

    printStatement("select * from information_schema.tables");

    printStatement("select * from a.b.c@d");

    printStatement("select \"TOTALPRICE\" \"my price\" from \"orders\"");

    printStatement("select * from foo limit 100 offset 20");
    printStatement("select * from foo offset 20");

    printStatement("select * from t where 'value' LIKE ANY (col)");
    printStatement("select * from t where 'value' NOT LIKE ANY (col)");
    printStatement("select * from t where 'source' ~ 'pattern'");
    printStatement("select * from t where 'source' !~ 'pattern'");
    printStatement("select * from t where source_column ~ pattern_column");
    printStatement("select * from t where ? !~ ?");
  }

  @Test
  public void testIntervalLiteral() {
    printStatement("select interval '1' HOUR");
  }

  @Test
  public void testEscapedStringLiteralBuilder() {
    printStatement("select E'aValue'");
    printStatement("select E'\\141Value'");
    printStatement("select e'aa\\'bb'");
  }

  @Test
  public void testBitString() {
    printStatement("select B'0010'");
  }

  @Test
  public void testThatEscapedStringLiteralContainingDoubleBackSlashAndSingleQuoteThrowsException() {
    assertThrows(
        IllegalArgumentException.class,
        () -> printStatement("select e'aa\\\\'bb' as col1"),
        "Invalid Escaped String Literal");
  }

  @Test
  public void testTrimFunctionStmtBuilder() {
    printStatement("SELECT trim(LEADING ' ' FROM  x) FROM t");
    printStatement("SELECT trim(' ' FROM  x) FROM t");
    printStatement("SELECT trim(FROM  x) FROM t");
    printStatement("SELECT trim(x) FROM t");
  }

  @Test
  public void testSystemInformationFunctionsStmtBuilder() {
    printStatement("select current_schema");
    printStatement("select current_schema()");
    printStatement("select pg_catalog.current_schema()");
    printStatement("select * from information_schema.tables where table_schema = current_schema");
    printStatement("select * from information_schema.tables where table_schema = current_schema()");
    printStatement(
        "select * from information_schema.tables where table_schema = pg_catalog.current_schema()");

    printStatement("select current_user");
    printStatement("select user");
    printStatement("select session_user");
  }

  @Test
  public void testStatementBuilderTpch() throws Exception {
    printTpchQuery(1, 3);
    printTpchQuery(2, 33, "part type like", "region name");
    printTpchQuery(3, "market segment", "2013-03-05");
    printTpchQuery(4, "2013-03-05");
    printTpchQuery(5, "region name", "2013-03-05");
    printTpchQuery(6, "2013-03-05", 33, 44);
    printTpchQuery(7, "nation name 1", "nation name 2");
    printTpchQuery(8, "nation name", "region name", "part type");
    printTpchQuery(9, "part name like");
    printTpchQuery(10, "2013-03-05");
    printTpchQuery(11, "nation name", 33);
    printTpchQuery(12, "ship mode 1", "ship mode 2", "2013-03-05");
    printTpchQuery(13, "comment like 1", "comment like 2");
    printTpchQuery(14, "2013-03-05");
    // query 15: views not supported
    printTpchQuery(16, "part brand", "part type like", 3, 4, 5, 6, 7, 8, 9, 10);
    printTpchQuery(17, "part brand", "part container");
    printTpchQuery(18, 33);
    printTpchQuery(19, "part brand 1", "part brand 2", "part brand 3", 11, 22, 33);
    printTpchQuery(20, "part name like", "2013-03-05", "nation name");
    printTpchQuery(21, "nation name");
    printTpchQuery(22, "phone 1", "phone 2", "phone 3", "phone 4", "phone 5", "phone 6", "phone 7");
  }

  @Test
  public void testShowTransactionLevel() {
    printStatement("show transaction isolation level");
  }

  @Test
  public void testArrayConstructorStmtBuilder() {
    printStatement("select []");
    printStatement("select [ARRAY[1]]");
    printStatement("select ARRAY[]");
    printStatement("select ARRAY[1, 2]");
    printStatement("select ARRAY[ARRAY[1,2], ARRAY[2]]");
    printStatement("select ARRAY[ARRAY[1,2], [2]]");
    printStatement("select ARRAY[ARRAY[1,2], ?]");

    printStatement("select ARRAY[1 + 2]");
    printStatement("select ARRAY[ARRAY[col, 2 + 3], [col]]");
    printStatement("select [ARRAY[1 + 2, ?], [1 + 2]]");
    printStatement("select ARRAY[col_a IS NULL, col_b IS NOT NULL]");
  }

  @Test
  public void test_cast_with_pg_array_type_definition() {
    printStatement("SELECT '{a,b,c}'::text[]");
  }

  @Test
  public void testArrayConstructorSubSelectBuilder() {
    printStatement("select array(select foo from f1) from f2");
    printStatement("select array(select * from f1) as array1 from f2");
    printStatement("select count(*) from f1 where f1.array1 = array(select foo from f2)");
  }

  @Test
  public void testArrayConstructorSubSelectBuilderNoParenthesisThrowsParsingException() {
    assertThrows(
        ParsingException.class,
        () -> printStatement("select array from f2"),
        "no viable alternative at input 'select array from'");
  }

  @Test
  public void testArrayConstructorSubSelectBuilderNoSubQueryThrowsParsingException() {
    assertThrows(
        ParsingException.class,
        () -> printStatement("select array() as array1 from f2"),
        "no viable alternative at input 'select array()'");
  }

  @Test
  public void testTableFunctions() {
    printStatement("select * from unnest([1, 2], ['Arthur', 'Marvin'])");
    printStatement("select * from unnest(?, ?)");
    printStatement("select * from open('/tmp/x')");
    printStatement("select * from x.y()");
  }

  @Test
  public void testStatementSubscript() {
    printStatement("select a['x'] from foo where a['x']['y']['z'] = 1");
    printStatement("select a['x'] from foo where a[1 + 2]['y'] = 1");
  }

  @Test
  public void testCopy() {
    printStatement("copy foo partition (a='x') from ?");
    printStatement("copy foo partition (a={key='value'}) from ?");
    printStatement("copy foo from '/folder/file.extension'");
    printStatement("copy foo from ?");
    printStatement("copy foo from ? with (some_property=1)");
    printStatement("copy foo from ? with (some_property=false)");
    printStatement("copy schemah.foo from '/folder/file.extension'");
    printStatement("copy schemah.foo from '/folder/file.extension' return summary");
    printStatement(
        "copy schemah.foo from '/folder/file.extension' with (some_property=1) return summary");

    printStatement("copy foo (nae) to '/folder/file.extension'");
    printStatement("copy foo to '/folder/file.extension'");
    printStatement("copy foo to DIRECTORY '/folder'");
    printStatement("copy foo to DIRECTORY ?");
    printStatement("copy foo to DIRECTORY '/folder' with (some_param=4)");
    printStatement("copy foo partition (a='x') to DIRECTORY '/folder' with (some_param=4)");
    printStatement("copy foo partition (a=?) to DIRECTORY '/folder' with (some_param=4)");

    printStatement("copy foo where a = 'x' to DIRECTORY '/folder'");
  }

  @Test
  public void testInsertStmtBuilder() {
    // insert from values
    printStatement("insert into foo (id, name) values ('string', 1.2)");
    printStatement("insert into foo values ('string', NULL)");
    printStatement("insert into foo (id, name) values ('string', 1.2), (abs(-4), 4+?)");
    printStatement("insert into schemah.foo (id, name) values ('string', 1.2)");

    printStatement("insert into t (a, b) values (1, 2) on conflict do nothing");
    printStatement("insert into t (a, b) values (1, 2) on conflict (a,b) do nothing");
    printStatement("insert into t (a, b) values (1, 2) on conflict (o['id'], b) do nothing");
    printStatement("insert into t (a, b) values (1, 2) on conflict (a, o['x']['y']) do nothing");
    printStatement("insert into t (a, b) values (1, 2) on conflict (a) do update set b = b + 1");
    printStatement(
        "insert into t (a, b, c) values (1, 2, 3) on conflict (a, b) "
            + "do update set a = a + 1, b = 3");
    printStatement(
        "insert into t (a, b, c) values (1, 2), (3, 4) on conflict (c) "
            + "do update set a = excluded.a + 1, b = 4");
    printStatement(
        "insert into t (a, b, c) values (1, 2), (3, 4) on conflict (c) "
            + "do update set a = excluded.a + 1, b = excluded.b - 2");

    Insert<Expression> insert =
        (Insert<Expression>)
            SqlParser.createStatement(
                "insert into test_generated_column (id, ts) values (?, ?) "
                    + "on conflict (id) do update set ts = ?");
    Assignment<Expression> onDuplicateAssignment =
        insert.duplicateKeyContext().getAssignments().get(0);
    assertThat(onDuplicateAssignment.expression(), instanceOf(ParameterExpression.class));
    assertThat(onDuplicateAssignment.expressions().get(0).toString(), is("$3"));

    // insert from query
    printStatement("insert into foo (id, name) select id, name from bar order by id");
    printStatement("insert into foo (id, name) select * from bar limit 3 offset 10");
    printStatement("insert into foo (wealth, name) select sum(money), name from bar group by name");
    printStatement("insert into foo select sum(money), name from bar group by name");

    printStatement("insert into foo (id, name) (select id, name from bar order by id)");
    printStatement("insert into foo (id, name) (select * from bar limit 3 offset 10)");
    printStatement(
        "insert into foo (wealth, name) (select sum(money), name from bar group by name)");
    printStatement("insert into foo (select sum(money), name from bar group by name)");
  }

  @Test
  public void test_insert_returning() {
    printStatement("insert into foo (id, name) values ('string', 1.2) returning id");
    printStatement("insert into foo (id, name) values ('string', 1.2) returning id as foo");
    printStatement("insert into foo (id, name) values ('string', 1.2) returning *");
    printStatement("insert into foo (id, name) values ('string', 1.2) returning foo.*");
  }

  @Test
  public void testParameterExpressionLimitOffset() {
    // ORMs like SQLAlchemy generate these kind of queries.
    printStatement("select * from foo limit ? offset ?");
  }

  @Test
  public void testMatchPredicateStmtBuilder() {
    printStatement("select * from foo where match (a['1']['2'], 'abc')");
    printStatement("select * from foo where match (a, 'abc')");
    printStatement("select * from foo where match ((a, b 2.0), 'abc')");
    printStatement("select * from foo where match ((a ?, b 2.0), ?)");
    printStatement(
        "select * from foo where match ((a ?, b 2.0), {type= 'Point', coordinates= [0.0,0.0] })");
    printStatement("select * from foo where match ((a 1, b 2.0), 'abc') using best_fields");
    printStatement(
        "select * from foo where match ((a 1, b 2.0), 'abc') "
            + "using best_fields with (prop=val, foo=1)");
    printStatement("select * from foo where match (a, (select shape from countries limit 1))");
  }

  @Test
  public void testCreateSnapshotStmtBuilder() {
    printStatement("CREATE SNAPSHOT my_repo.my_snapshot ALL");
    printStatement("CREATE SNAPSHOT my_repo.my_snapshot TABLE authors, books");
    printStatement(
        "CREATE SNAPSHOT my_repo.my_snapshot TABLE authors, books with (wait_for_completion=True)");
    printStatement("CREATE SNAPSHOT my_repo.my_snapshot ALL with (wait_for_completion=True)");
    Statement statement =
        SqlParser.createStatement(
            "CREATE SNAPSHOT my_repo.my_snapshot TABLE authors "
                + "PARTITION (year=2015, year=2014), books");
    assertThat(
        statement.toString(),
        is(
            "CreateSnapshot{"
                + "name=my_repo.my_snapshot, "
                + "properties={}, "
                + "tables=[Table{only=false, authors, partitionProperties=["
                + "Assignment{column=\"year\", expressions=[2015]}, "
                + "Assignment{column=\"year\", expressions=[2014]}]}, "
                + "Table{only=false, books, partitionProperties=[]}]}"));
  }

  @Test
  public void testDropSnapshotStmtBuilder() {
    Statement statement = SqlParser.createStatement("DROP SNAPSHOT my_repo.my_snapshot");
    assertThat(statement.toString(), is("DropSnapshot{name=my_repo.my_snapshot}"));
  }

  @Test
  public void testRestoreSnapshotStmtBuilder() {
    printStatement("RESTORE SNAPSHOT my_repo.my_snapshot ALL");
    printStatement("RESTORE SNAPSHOT my_repo.my_snapshot TABLE authors, books");
    printStatement(
        "RESTORE SNAPSHOT my_repo.my_snapshot TABLE authors, "
            + "books with (wait_for_completion=True)");
    printStatement("RESTORE SNAPSHOT my_repo.my_snapshot ALL with (wait_for_completion=True)");
    printStatement(
        "RESTORE SNAPSHOT my_repo.my_snapshot TABLE "
            + "authors PARTITION (year=2015, year=2014), books");
    Statement statement =
        SqlParser.createStatement(
            "RESTORE SNAPSHOT my_repo.my_snapshot "
                + "TABLE authors PARTITION (year=2015, year=2014), books "
                + "WITH (wait_for_completion=True)");
    assertThat(
        statement.toString(),
        is(
            "RestoreSnapshot{"
                + "name=my_repo.my_snapshot, "
                + "properties={wait_for_completion=true}, "
                + "tables=["
                + "Table{only=false, authors, partitionProperties=["
                + ""
                + "Assignment{column=\"year\", expressions=[2015]}, "
                + "Assignment{column=\"year\", expressions=[2014]}]}, "
                + "Table{only=false, books, partitionProperties=[]}]}"));
    statement = SqlParser.createStatement("RESTORE SNAPSHOT my_repo.my_snapshot ALL");
    assertThat(
        statement.toString(),
        is("RestoreSnapshot{" + "name=my_repo.my_snapshot, " + "properties={}, " + "tables=[]}"));
  }

  @Test
  public void testGeoShapeStmtBuilder() {
    printStatement(
        "create table test ("
            + "    col1 geo_shape,"
            + "    col2 geo_shape index using geohash"
            + ")");
    printStatement(
        "create table test("
            + "    col1 geo_shape index using quadtree with (precision='1m')"
            + ")");
    printStatement(
        "create table test("
            + "    col1 geo_shape,"
            + "    index geo_shape_i using quadtree(col1) with (precision='1m')"
            + ")");
    printStatement(
        "create table test("
            + "    col1 geo_shape INDEX OFF,"
            + "    index geo_shape_i using quadtree(col1) with (precision='1m')"
            + ")");
  }

  @Test
  public void testCastStmtBuilder() {
    // double colon cast
    printStatement("select 1+4::integer");
    printStatement("select '2'::integer");
    printStatement("select 1.0::timestamp with time zone");
    printStatement("select 1.0::timestamp without time zone");
    printStatement("select 1+3::string");
    printStatement("select [0,1,5]::array(boolean)");
    printStatement("select field::boolean");
    printStatement("select port['http']::boolean");
    printStatement("select '4'::integer + 4");
    printStatement("select 4::string || ' apples'");
    printStatement("select '-4'::integer");
    printStatement("select -4::string");
    printStatement("select '-4'::integer + 10");
    printStatement("select -4::string || ' apples'");
    // cast
    printStatement("select cast(1+4 as integer) from foo");
    printStatement("select cast('2' as integer) from foo");
    // try cast
    printStatement("select try_cast(y as integer) from foo");
  }

  @Test
  public void test_row_type_access() {
    printStatement("SELECT (obj).col FROM tbl");
    printStatement("SELECT ((obj).x).y FROM tbl");
  }

  @Test
  public void testSubscriptExpression() {
    Expression expression = SqlParser.createExpression("a['sub']");
    assertThat(expression, instanceOf(SubscriptExpression.class));
    SubscriptExpression subscript = (SubscriptExpression) expression;
    assertThat(subscript.index(), instanceOf(StringLiteral.class));
    assertThat(((StringLiteral) subscript.index()).getValue(), is("sub"));

    assertThat(subscript.base(), instanceOf(QualifiedNameReference.class));

    expression = SqlParser.createExpression("[1,2,3][1]");
    assertThat(expression, instanceOf(SubscriptExpression.class));
    subscript = (SubscriptExpression) expression;
    assertThat(subscript.index(), instanceOf(IntegerLiteral.class));
    assertThat(((IntegerLiteral) subscript.index()).getValue(), is(1));
    assertThat(subscript.base(), instanceOf(ArrayLiteral.class));
  }

  @Test
  public void testSafeSubscriptExpression() {
    MatchPredicate matchPredicate =
        (MatchPredicate) SqlParser.createExpression("match (a['1']['2'], 'abc')");
    assertThat(matchPredicate.idents().get(0).columnIdent().toString(), is("\"a\"['1']['2']"));

    matchPredicate = (MatchPredicate) SqlParser.createExpression("match (a['1']['2']['4'], 'abc')");
    assertThat(matchPredicate.idents().get(0).columnIdent().toString(), is("\"a\"['1']['2']['4']"));

    assertThrows(
        ParsingException.class,
        () -> SqlParser.createExpression("match ([1]['1']['2'], 'abc')"),
        "mismatched input '<EOF>' expecting 'SET'");
  }

  @Test
  public void testCaseSensitivity() {
    Expression expression = SqlParser.createExpression("\"firstName\" = 'myName'");
    QualifiedNameReference nameRef =
        (QualifiedNameReference) ((ComparisonExpression) expression).getLeft();
    StringLiteral myName = (StringLiteral) ((ComparisonExpression) expression).getRight();
    assertThat(nameRef.getName().getSuffix(), is("firstName"));
    assertThat(myName.getValue(), is("myName"));

    expression = SqlParser.createExpression("FIRSTNAME = 'myName'");
    nameRef = (QualifiedNameReference) ((ComparisonExpression) expression).getLeft();
    assertThat(nameRef.getName().getSuffix(), is("firstname"));

    expression = SqlParser.createExpression("ABS(1)");
    QualifiedName functionName = ((FunctionCall) expression).getName();
    assertThat(functionName.getSuffix(), is("abs"));
  }

  @Test
  public void testArrayComparison() {
    Expression anyExpression = SqlParser.createExpression("1 = ANY (arrayColumnRef)");
    assertThat(anyExpression, instanceOf(ArrayComparisonExpression.class));
    ArrayComparisonExpression arrayComparisonExpression = (ArrayComparisonExpression) anyExpression;
    assertThat(
        arrayComparisonExpression.quantifier(), is(ArrayComparisonExpression.Quantifier.ANY));
    assertThat(arrayComparisonExpression.getLeft(), instanceOf(IntegerLiteral.class));
    assertThat(arrayComparisonExpression.getRight(), instanceOf(QualifiedNameReference.class));

    Expression someExpression = SqlParser.createExpression("1 = SOME (arrayColumnRef)");
    assertThat(someExpression, instanceOf(ArrayComparisonExpression.class));
    ArrayComparisonExpression someArrayComparison = (ArrayComparisonExpression) someExpression;
    assertThat(someArrayComparison.quantifier(), is(ArrayComparisonExpression.Quantifier.ANY));
    assertThat(someArrayComparison.getLeft(), instanceOf(IntegerLiteral.class));
    assertThat(someArrayComparison.getRight(), instanceOf(QualifiedNameReference.class));

    Expression allExpression = SqlParser.createExpression("'StringValue' = ALL (arrayColumnRef)");
    assertThat(allExpression, instanceOf(ArrayComparisonExpression.class));
    ArrayComparisonExpression allArrayComparison = (ArrayComparisonExpression) allExpression;
    assertThat(allArrayComparison.quantifier(), is(ArrayComparisonExpression.Quantifier.ALL));
    assertThat(allArrayComparison.getLeft(), instanceOf(StringLiteral.class));
    assertThat(allArrayComparison.getRight(), instanceOf(QualifiedNameReference.class));
  }

  @Test
  public void testArrayComparisonSubSelect() {
    Expression anyExpression = SqlParser.createExpression("1 = ANY ((SELECT 5))");
    assertThat(anyExpression, instanceOf(ArrayComparisonExpression.class));
    ArrayComparisonExpression arrayComparisonExpression = (ArrayComparisonExpression) anyExpression;
    assertThat(
        arrayComparisonExpression.quantifier(), is(ArrayComparisonExpression.Quantifier.ANY));
    assertThat(arrayComparisonExpression.getLeft(), instanceOf(IntegerLiteral.class));
    assertThat(arrayComparisonExpression.getRight(), instanceOf(SubqueryExpression.class));

    // It's possible to omit the parenthesis
    anyExpression = SqlParser.createExpression("1 = ANY (SELECT 5)");
    assertThat(anyExpression, instanceOf(ArrayComparisonExpression.class));
    arrayComparisonExpression = (ArrayComparisonExpression) anyExpression;
    assertThat(
        arrayComparisonExpression.quantifier(), is(ArrayComparisonExpression.Quantifier.ANY));
    assertThat(arrayComparisonExpression.getLeft(), instanceOf(IntegerLiteral.class));
    assertThat(arrayComparisonExpression.getRight(), instanceOf(SubqueryExpression.class));
  }

  @Test
  public void testArrayLikeExpression() {
    Expression expression = SqlParser.createExpression("'books%' LIKE ANY(race['interests'])");
    assertThat(expression, instanceOf(ArrayLikePredicate.class));
    ArrayLikePredicate arrayLikePredicate = (ArrayLikePredicate) expression;
    assertThat(arrayLikePredicate.inverse(), is(false));
    assertThat(arrayLikePredicate.getEscape(), is(nullValue()));
    assertThat(arrayLikePredicate.getPattern().toString(), is("'books%'"));
    assertThat(arrayLikePredicate.getValue().toString(), is("\"race\"['interests']"));

    expression = SqlParser.createExpression("'b%' NOT LIKE ANY(race)");
    assertThat(expression, instanceOf(ArrayLikePredicate.class));
    arrayLikePredicate = (ArrayLikePredicate) expression;
    assertThat(arrayLikePredicate.inverse(), is(true));
    assertThat(arrayLikePredicate.getEscape(), is(nullValue()));
    assertThat(arrayLikePredicate.getPattern().toString(), is("'b%'"));
    assertThat(arrayLikePredicate.getValue().toString(), is("\"race\""));
  }

  @Test
  public void testStringLiteral() {
    String[] testString =
        new String[] {"foo' or 1='1", "foo''bar", "foo\\bar", "foo'bar", "''''", "''", ""};
    for (String s : testString) {
      Expression expr = SqlParser.createExpression(Literals.quoteStringLiteral(s));
      assertThat(((StringLiteral) expr).getValue(), is(s));
    }
  }

  @Test
  public void testEscapedStringLiteral() {
    String input = "this is a triple-a:\\141\\x61\\u0061";
    String expectedValue = "this is a triple-a:aaa";
    Expression expr = SqlParser.createExpression(Literals.quoteEscapedStringLiteral(input));
    EscapedCharStringLiteral escapedCharStringLiteral = (EscapedCharStringLiteral) expr;
    assertThat(escapedCharStringLiteral.getRawValue(), is(input));
    assertThat(escapedCharStringLiteral.getValue(), is(expectedValue));
  }

  @Test
  public void testObjectLiteral() {
    Expression emptyObjectLiteral = SqlParser.createExpression("{}");
    assertThat(emptyObjectLiteral, instanceOf(ObjectLiteral.class));
    assertThat(((ObjectLiteral) emptyObjectLiteral).values().size(), is(0));

    ObjectLiteral objectLiteral =
        (ObjectLiteral) SqlParser.createExpression("{a=1, aa=-1, b='str', c=[], d={}}");
    assertThat(objectLiteral.values().size(), is(5));
    assertThat(objectLiteral.values().get("a"), instanceOf(IntegerLiteral.class));
    assertThat(objectLiteral.values().get("aa"), instanceOf(NegativeExpression.class));
    assertThat(objectLiteral.values().get("b"), instanceOf(StringLiteral.class));
    assertThat(objectLiteral.values().get("c"), instanceOf(ArrayLiteral.class));
    assertThat(objectLiteral.values().get("d"), instanceOf(ObjectLiteral.class));

    ObjectLiteral quotedObjectLiteral = (ObjectLiteral) SqlParser.createExpression("{\"AbC\"=123}");
    assertThat(quotedObjectLiteral.values().size(), is(1));
    assertThat(quotedObjectLiteral.values().get("AbC"), instanceOf(IntegerLiteral.class));
    assertThat(quotedObjectLiteral.values().get("abc"), CoreMatchers.nullValue());
    assertThat(quotedObjectLiteral.values().get("ABC"), CoreMatchers.nullValue());

    SqlParser.createExpression("{a=func('abc')}");
    SqlParser.createExpression("{b=identifier}");
    SqlParser.createExpression("{c=1+4}");
    SqlParser.createExpression("{d=sub['script']}");
  }

  @Test
  public void testArrayLiteral() {
    ArrayLiteral emptyArrayLiteral = (ArrayLiteral) SqlParser.createExpression("[]");
    assertThat(emptyArrayLiteral.values().size(), is(0));

    ArrayLiteral singleArrayLiteral = (ArrayLiteral) SqlParser.createExpression("[1]");
    assertThat(singleArrayLiteral.values().size(), is(1));
    assertThat(singleArrayLiteral.values().get(0), instanceOf(IntegerLiteral.class));

    ArrayLiteral multipleArrayLiteral =
        (ArrayLiteral) SqlParser.createExpression("['str', -12.56, {}, ['another', 'array']]");
    assertThat(multipleArrayLiteral.values().size(), is(4));
    assertThat(multipleArrayLiteral.values().get(0), instanceOf(StringLiteral.class));
    assertThat(multipleArrayLiteral.values().get(1), instanceOf(NegativeExpression.class));
    assertThat(multipleArrayLiteral.values().get(2), instanceOf(ObjectLiteral.class));
    assertThat(multipleArrayLiteral.values().get(3), instanceOf(ArrayLiteral.class));
  }

  @Test
  public void testParameterNode() {
    printStatement("select foo, $1 from foo where a = $2 or a = $3");

    final AtomicInteger counter = new AtomicInteger(0);

    Expression inExpression = SqlParser.createExpression("x in (?, ?, ?)");
    inExpression.accept(
        new DefaultTraversalVisitor<>() {
          @Override
          public Object visitParameterExpression(ParameterExpression node, Object context) {
            assertEquals(counter.incrementAndGet(), node.position());
            return super.visitParameterExpression(node, context);
          }
        },
        null);

    assertEquals(3, counter.get());
    counter.set(0);

    Expression andExpression = SqlParser.createExpression("a = ? and b = ? and c = $3");
    andExpression.accept(
        new DefaultTraversalVisitor<>() {
          @Override
          public Object visitParameterExpression(ParameterExpression node, Object context) {
            assertEquals(counter.incrementAndGet(), node.position());
            return super.visitParameterExpression(node, context);
          }
        },
        null);
    assertEquals(3, counter.get());
  }

  @Test
  public void testShowCreateTable() {
    Statement stmt = SqlParser.createStatement("SHOW CREATE TABLE foo");
    assertTrue(stmt instanceof ShowCreateTable);
    assertEquals(((ShowCreateTable) stmt).table().getName().toString(), "foo");
    stmt = SqlParser.createStatement("SHOW CREATE TABLE my_schema.foo");
    assertEquals(((ShowCreateTable) stmt).table().getName().toString(), "my_schema.foo");
  }

  @Test
  public void testCreateTableWithGeneratedColumn() {
    printStatement("create table test (col1 int, col2 AS date_trunc('day', col1))");
    printStatement("create table test (col1 int, col2 AS (date_trunc('day', col1)))");
    printStatement("create table test (col1 int, col2 AS date_trunc('day', col1) INDEX OFF)");

    printStatement(
        "create table test (col1 int, col2 GENERATED ALWAYS AS date_trunc('day', col1))");
    printStatement(
        "create table test (col1 int, col2 GENERATED ALWAYS AS (date_trunc('day', col1)))");

    printStatement(
        "create table test (col1 int, col2 string GENERATED ALWAYS AS date_trunc('day', col1))");
    printStatement(
        "create table test (col1 int, col2 string GENERATED ALWAYS AS (date_trunc('day', col1)))");

    printStatement("create table test (col1 int, col2 AS cast(col1 as string))");
    printStatement("create table test (col1 int, col2 AS (cast(col1 as string)))");
    printStatement("create table test (col1 int, col2 AS col1 + 1)");
    printStatement("create table test (col1 int, col2 AS (col1 + 1))");

    printStatement("create table test (col1 int, col2 AS col1['name'] + 1)");
  }

  @Test
  public void testCreateTableWithCheckConstraints() {
    printStatement("create table t (a int check(a >= 0), b boolean)");
    printStatement("create table t (a int constraint over_zero check(a >= 0), b boolean)");
    printStatement("create table t (a int, b boolean, check(a >= 0))");
    printStatement("create table t (a int, b boolean, constraint over_zero check(a >= 0))");
    printStatement("create table t (a int, b boolean check(b))");
    printStatement("create table t (a int, b boolean constraint b check(b))");
  }

  @Test
  public void testAlterTableAddColumnWithCheckConstraint() {
    printStatement("alter table t add column a int check(a >= 0)");
    printStatement("alter table t add column a int constraint over_zero check(a >= 0)");
  }

  @Test
  public void testAlterTableDropCheckConstraint() {
    printStatement("alter table t drop constraint check");
  }

  @Test
  public void testAddGeneratedColumn() {
    printStatement("alter table t add col2 AS date_trunc('day', col1)");
    printStatement("alter table t add col2 AS date_trunc('day', col1) INDEX USING PLAIN");
    printStatement("alter table t add col2 AS (date_trunc('day', col1))");

    printStatement("alter table t add col2 GENERATED ALWAYS AS date_trunc('day', col1)");
    printStatement("alter table t add col2 GENERATED ALWAYS AS (date_trunc('day', col1))");

    printStatement("alter table t add col2 string GENERATED ALWAYS AS date_trunc('day', col1)");
    printStatement("alter table t add col2 string GENERATED ALWAYS AS (date_trunc('day', col1))");

    printStatement("alter table t add col2 AS cast(col1 as string)");
    printStatement("alter table t add col2 AS (cast(col1 as string))");
    printStatement("alter table t add col2 AS col1 + 1");
    printStatement("alter table t add col2 AS (col1 + 1)");

    printStatement("alter table t add col2 AS col1['name'] + 1");
  }

  @Test
  public void testAlterTableAddColumnTypeOrGeneratedExpressionAreDefined() {
    assertThrows(
        IllegalArgumentException.class,
        () -> printStatement("alter table t add column col2"),
        "Column [\"col2\"]: data type needs to be provided or "
            + "column should be defined as a generated expression");
  }

  @Test
  public void testAddColumnWithDefaultExpressionIsNotSupported() {
    assertThrows(
        ParsingException.class,
        () -> printStatement("mismatched input 'default'"),
        "mismatched input 'default'");
  }

  @Test
  public void testAlterTableOpenClose() {
    printStatement("alter table t close");
    printStatement("alter table t open");

    printStatement("alter table t partition (partitioned_col=1) close");
    printStatement("alter table t partition (partitioned_col=1) open");
  }

  @Test
  public void testAlterTableRename() {
    printStatement("alter table t rename to t2");
  }

  @Test
  public void testAlterTableReroute() {
    printStatement("alter table t reroute move shard 1 from 'node1' to 'node2'");
    printStatement("alter table t partition (parted_col = ?) reroute move shard ? from ? to ?");
    printStatement("alter table t reroute allocate replica shard 1 on 'node1'");
    printStatement("alter table t reroute cancel shard 1 on 'node1'");
    printStatement("alter table t reroute cancel shard 1 on 'node1' with (allow_primary = true)");
    printStatement(
        "ALTER TABLE t REROUTE PROMOTE REPLICA SHARD 1 "
            + "ON 'node1' WITH (accept_data_loss = true, foo = ?)");
    printStatement("ALTER TABLE t REROUTE PROMOTE REPLICA SHARD ? ON ? ");
  }

  @Test
  public void testAlterUser() {
    printStatement("alter user crate set (password = 'password')");
    printStatement("alter user crate set (password = null)");
  }

  @Test
  public void testAlterUserWithMissingProperties() {
    assertThrows(
        ParsingException.class,
        () -> printStatement("alter user crate"),
        "mismatched input '<EOF>' expecting 'SET'");
  }

  @Test
  public void testSubSelects() {
    printStatement("select * from (select * from foo) as f");
    printStatement("select * from (select * from (select * from foo) as f1) as f2");
    printStatement("select * from (select * from foo) f");
    printStatement("select * from (select * from (select * from foo) f1) f2");
  }

  @Test
  public void testJoins() {
    printStatement("select * from foo inner join bar on foo.id = bar.id");

    printStatement("select * from foo left outer join bar on foo.id = bar.id");
    printStatement("select * from foo left join bar on foo.id = bar.id");
    printStatement("select * from foo right outer join bar on foo.id = bar.id");
    printStatement("select * from foo right join bar on foo.id = bar.id");
    printStatement("select * from foo full outer join bar on foo.id = bar.id");
    printStatement("select * from foo full join bar on foo.id = bar.id");
  }

  @Test
  public void testConditionals() {
    printStatement(
        "SELECT a,"
            + "       CASE WHEN a=1 THEN 'one'"
            + "            WHEN a=2 THEN 'two'"
            + "            ELSE 'other'"
            + "       END"
            + "    FROM test");
    printStatement(
        "SELECT a,"
            + "       CASE a WHEN 1 THEN 'one'"
            + "              WHEN 2 THEN 'two'"
            + "              ELSE 'other'"
            + "       END"
            + "    FROM test");
    printStatement("SELECT a WHERE CASE WHEN x <> 0 THEN y/x > 1.5 ELSE false END");
  }

  @Test
  public void testUnions() {
    printStatement("select * from foo union select * from bar");
    printStatement("select * from foo union all select * from bar");
    printStatement("select * from foo union distinct select * from bar");
    printStatement(
        "select 1 " + "union select 2 " + "union distinct select 3 " + "union all select 4");
    printStatement(
        "select 1 union "
            + "select 2 union all "
            + "select 3 union "
            + "select 4 union all "
            + "select 5 union distinct "
            + "select 6 "
            + "order by 1");
  }

  @Test
  public void testCreateViewParsing() {
    printStatement("CREATE VIEW myView AS SELECT * FROM foobar");
    printStatement("CREATE OR REPLACE VIEW myView AS SELECT * FROM foobar");
  }

  @Test
  public void testDropViewParsing() {
    printStatement("DROP VIEW myView");
    printStatement("DROP VIEW IF EXISTS myView");
  }

  @Test
  public void test_values_as_top_relation_parsing() {
    printStatement("VALUES (1, 2), (2, 3), (3, 4)");
    printStatement("VALUES (1), (2), (3)");
    printStatement("VALUES (1, 2, 3 + 3, (SELECT 1))");
  }

  @Test
  public void test_wildcard_aggregate_with_filter_clause() {
    printStatement("SELECT COUNT(*) FILTER (WHERE x > 10) FROM t");
  }

  @Test
  public void test_distinct_aggregate_with_filter_clause() {
    printStatement("SELECT AVG(DISTINCT x) FILTER (WHERE 1 = 1) FROM t");
  }

  @Test
  public void test_multiple_aggregates_with_filter_clauses() {
    printStatement(
        "SELECT "
            + "   SUM(x) FILTER (WHERE x > 10), "
            + "   AVG(x) FILTER (WHERE 1 = 1) "
            + "FROM t");
  }

  @Test
  public void test_create_table_with_parametrized_varchar_data_type() {
    printStatement("create table test(col varchar(1))");
    printStatement("create table test(col character varying(2))");
  }

  @Test
  public void test_restore_snapshot() {
    printStatement("RESTORE SNAPSHOT repo1.snap1 ALL");
    printStatement("RESTORE SNAPSHOT repo1.snap1 ALL WITH (wait_for_completion = true)");
    printStatement("RESTORE SNAPSHOT repo1.snap1 TABLE t PARTITION (parted_col = ?)");
    printStatement("RESTORE SNAPSHOT repo1.snap1 METADATA");
    printStatement("RESTORE SNAPSHOT repo1.snap1 USERS WITH (some_option = true)");
    printStatement("RESTORE SNAPSHOT repo1.snap1 USERS, PRIVILEGES");
    printStatement("RESTORE SNAPSHOT repo1.snap1 TABLES, PRIVILEGES");
  }
}
