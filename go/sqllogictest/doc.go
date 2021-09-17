// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package logic

// This file is home to TestLogic, a general-purpose engine for
// running SQL logic tests.
//
// TestLogic implements the infrastructure that runs end-to-end tests
// against CockroachDB's SQL layer. It is typically used to run
// CockroachDB's own tests (stored in the `testdata` directory) during
// development and CI, and a subset of SQLite's "Sqllogictest" during
// nightly CI runs. However, any test input can be specified via
// command-line flags (see below).
//
// In a nutshell, TestLogic reads one or more test input files
// containing sequences of SQL statements and queries. Each input file
// is meant to test a feature group. The reason why tests can/should
// be split across multiple files is that each test input file gets
// its own fresh, empty database.
//
// Input files for unit testing are stored alongside the source code
// in the `testdata` subdirectory. The input files for the larger
// TestSqlLiteLogic tests are stored in a separate repository.
//
// The test input is expressed using a domain-specific language, called
// Test-Script, defined by SQLite's "Sqllogictest".  The official home
// of Sqllogictest and Test-Script is
//      https://www.sqlite.org/sqllogictest/
//
// (the TestSqlLiteLogic test uses a fork of the Sqllogictest test files;
// its input files are hosted at https://github.com/cockroachdb/sqllogictest)
//
// Test-Script is line-oriented. It supports both statements which
// generate no result rows, and queries that produce result rows. The
// result of queries can be checked either using an explicit reference
// output in the test file, or using the expected row count and a hash
// of the expected output. A test can also check for expected column
// names for query results, or expected errors.
//
// Logic tests can start with a directive as follows:
//
//   # LogicTest: local fakedist
//
// This directive lists configurations; the test is run once in each
// configuration (in separate subtests). The configurations are defined by
// logicTestConfigs. If the directive is missing, the test is run in the
// default configuration.
//
// The Test-Script language is extended here for use with CockroachDB. The
// supported directives are:
//
//  - statement ok
//    Runs the statement that follows and expects success. For
//    example:
//      statement ok
//      CREATE TABLE kv (k INT PRIMARY KEY, v INT)
//
//
//  - statement count N
//    Like "statement ok" but expect a final RowsAffected count of N.
//    example:
//      statement count 2
//      INSERT INTO kv VALUES (1,2), (2,3)
//
//  - statement error <regexp>
//    Runs the statement that follows and expects an
//    error that matches the given regexp.
//
//  - query <typestring> <options> <label>
//    Runs the query that follows and verifies the results (specified after the
//    query and a ---- separator). Example:
//      query I
//      SELECT 1, 2
//      ----
//      1 2
//
//    The type string specifies the number of columns and their types:
//      - T for text; also used for various types which get converted
//        to string (arrays, timestamps, etc.).
//      - I for integer
//      - R for floating point or decimal
//      - B for boolean
//      - O for oid
//
//    Options are comma separated strings from the following:
//      - nosort (default)
//      - rowsort: sorts both the returned and the expected rows assuming one
//            white-space separated word per column.
//      - valuesort: sorts all values on all rows as one big set of
//            strings (for both the returned and the expected rows).
//      - partialsort(x,y,..): performs a partial sort on both the
//            returned and the expected results, preserving the relative
//            order of rows that differ on the specified columns
//            (1-indexed); for results that are expected to be already
//            ordered according to these columns. See partialSort() for
//            more information.
//      - colnames: column names are verified (the expected column names
//            are the first line in the expected results).
//      - retry: if the expected results do not match the actual results, the
//            test will be retried with exponential backoff up to some maximum
//            duration. If the test succeeds at any time during that period, it
//            is considered successful. Otherwise, it is a failure. See
//            testutils.SucceedsSoon for more information. If run with the
//            -rewrite flag, inserts a 500ms sleep before executing the query
//            once.
//
//    The label is optional. If specified, the test runner stores a hash
//    of the results of the query under the given label. If the label is
//    reused, the test runner verifies that the results are the
//    same. This can be used to verify that two or more queries in the
//    same test script that are logically equivalent always generate the
//    same output. If a label is provided, expected results don't need to
//    be provided (in which case there should be no ---- separator).
//
//  - query error <regexp>
//    Runs the query that follows and expects an error
//    that matches the given regexp.
//
//  - repeat <number>
//    It causes the following `statement` or `query` to be repeated the given
//    number of times. For example:
//      repeat 50
//      statement ok
//      INSERT INTO T VALUES ((SELECT MAX(k+1) FROM T))
//
//  - sleep <duration>
//    Introduces a sleep period. Example: sleep 2s
//
//  - subtest <testname>
//    Defines the start of a subtest. The subtest is any number of statements
//    that occur after this command until the end of file or the next subtest
//    command.
//
// The overall architecture of TestLogic is as follows:
//
// - TestLogic() selects the input files and instantiates
//   a `logicTest` object for each input file.
//
// - logicTest.run() sets up a new database.
// - logicTest.processTestFile() runs all tests from that input file.
//
// - each individual test in an input file is instantiated either as a
//   logicStatement or logicQuery object. These are then processed by
//   either logicTest.execStatement() or logicTest.execQuery().
//
// TestLogic has three main parameter groups:
//
// - Which input files are processed.
// - How and when to stop when tests fail.
// - Which results are reported.
//
