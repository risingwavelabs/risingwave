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

import (
	"fmt"
	"io/ioutil"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	gosql "database/sql"

	log "github.com/sirupsen/logrus"
)

// logicTest executes the test cases specified in a file. The file format is
// taken from the sqllogictest tool
// (http://www.sqlite.org/sqllogictest/doc/trunk/about.wiki) with various
// extensions to allow specifying errors and additional options. See
// https://github.com/gregrahn/sqllogictest/ for a github mirror of the
// sqllogictest source.
type logicTest struct {
	t        *testing.T
	subtestT *testing.T

	db *gosql.DB

	// progress holds the number of tests executed so far.
	progress int
	failures int
	// unsupported holds the number of queries ignored due
	// to prepare errors, when -allow-prepare-fail is set.
	unsupported int
	// lastProgress is used for the progress indicator message.
	lastProgress time.Time
	// perErrorSummary retains the per-error list of failing queries
	// when -error-summary is set.
	perErrorSummary map[string][]string
	// labelMap retains the expected result hashes that have
	// been marked using a result label in the input. See the
	// explanation for labels in processInputFiles().
	labelMap map[string]string
}

// printCompletion reports on the completion of all tests in a given
// input file.
func (t *logicTest) printCompletion(path string) {
	unsupportedMsg := ""
	if t.unsupported > 0 {
		unsupportedMsg = fmt.Sprintf(", ignored %d unsupported queries", t.unsupported)
	}
	log.Infof("--- done: %s: %d tests, %d failures%s", path, t.progress, t.failures, unsupportedMsg)
}

func (t *logicTest) processTestFile(path string) error {
	fdata, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	subtests, err := fetchSubtests(path, fdata)
	if err != nil {
		return err
	}

	defer t.printCompletion(path)

	for _, subtest := range subtests {
		// If subtest has no name, then it is not a subtest, so just run the lines
		// in the overall test. Note that this can only happen in the first subtest.
		if len(subtest.name) == 0 {
			t.subtestT = t.t
			if err := t.processSubtest(subtest, path); err != nil {
				return err
			}
		} else {
			t.t.Run(subtest.name, func(subtestT *testing.T) {
				t.subtestT = subtestT
				defer func() {
					t.subtestT = t.t
				}()
				if err := t.processSubtest(subtest, path); err != nil {
					t.failures++
					t.subtestT.Error(err)
				}
			})
		}
	}

	return nil
}

func (t *logicTest) processSubtest(subtest subtestDetails, path string) error {
	s := newLineScanner(subtest.buffer)
	t.lastProgress = time.Now()

	repeat := 1
	for s.Scan() {
		line := s.Text()
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}

		cmdParser := &commandParser{
			curPath:   path,
			curLineNo: s.line + subtest.startLineNo,
			fields:    fields,
			text:      line,
		}

		cmd := fields[0]
		if strings.HasPrefix(cmd, "#") {
			// Skip comment lines.
			continue
		}
		if len(fields) == 2 && fields[1] == "error" {
			return cmdParser.wrapErrorf("no expected error provided")
		}
		switch cmd {
		case "repeat":
			count, err := cmdParser.parseRepeat()
			if err != nil {
				return err
			}
			repeat = count

		case "sleep":
			duration, err := cmdParser.parseSleep()
			if err != nil {
				return err
			}
			time.Sleep(duration)

		case "statement":
			stmt, err := cmdParser.parseStatement(s)
			if err != nil {
				return err
			}
			stmt.t = t.subtestT
			for i := 0; i < repeat; i++ {
				if err := t.execStatement(stmt); err != nil {
					// Fail fast.
					return err
				}
			}

			repeat = 1
			t.success(path)

		case "query":
			query, err := cmdParser.parseQueryBlock(s)
			if err != nil {
				return err
			}
			query.t = t.subtestT
			for i := 0; i < repeat; i++ {
				if err := t.execQuery(query); err != nil {
					// Query failure will not effect db state, continue.
					t.subtestT.Error(err)
					t.failures++
					continue
				}
			}

			repeat = 1
			t.success(path)

		default:
			return cmdParser.wrapErrorf("unknown command: %s", cmd)
		}
	}
	return s.Err()
}

func (t *logicTest) execStatement(stmt *logicStatement) error {
	res, err := t.db.Exec(stmt.sql)
	if err == nil && stmt.expectCount >= 0 {
		var count int64
		count, err = res.RowsAffected()

		// If err becomes non-nil here, we'll catch it below.
		if err == nil && count != stmt.expectCount {
			return fmt.Errorf("%s\nexpected %d rows affected, got %d", stmt.sql, stmt.expectCount, count)
		}
	}
	return verifyError(stmt.sql, stmt.pos, stmt.expectErr, err)
}

func (t *logicTest) execQuery(query *logicQuery) error {
	rows, err := t.db.Query(query.sql)
	if err := verifyError(query.sql, query.pos, query.expectErr, err); err != nil {
		return err
	}
	if err != nil {
		// An error occurred, but it was expected.
		return nil
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	if len(query.colTypes) != len(cols) {
		return fmt.Errorf("%s: expected %d columns, but found %d",
			query.pos, len(query.colTypes), len(cols))
	}
	vals := make([]interface{}, len(cols))
	for i := range vals {
		vals[i] = new(interface{})
	}

	var actualResultsRaw []string
	if query.colNames {
		actualResultsRaw = append(actualResultsRaw, cols...)
	}
	for rows.Next() {
		if err := rows.Scan(vals...); err != nil {
			return err
		}
		for i, v := range vals {
			val := *v.(*interface{})
			if val == nil {
				actualResultsRaw = append(actualResultsRaw, "NULL")
				continue
			}
			if err := query.verifyValueType(val, i); err != nil {
				return err
			}
			if byteArray, ok := val.([]byte); ok {
				// The postgres wire protocol does not distinguish between
				// strings and byte arrays, but our tests do. In order to do
				// The Right Thing™, we replace byte arrays which are valid
				// UTF-8 with strings. This allows byte arrays which are not
				// valid UTF-8 to print as a list of bytes (e.g. `[124 107]`)
				// while printing valid strings naturally.
				if str := string(byteArray); utf8.ValidString(str) {
					val = str
				}
			}
			// Empty strings are rendered as "·" (middle dot)
			if val == "" {
				val = "·"
			}
			actualResultsRaw = append(actualResultsRaw, fmt.Sprint(val))
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	return query.deepEquals(t, actualResultsRaw, len(cols))
}

func (t *logicTest) success(file string) {
	t.progress++
	now := time.Now()
	if now.Sub(t.lastProgress) >= 2*time.Second {
		t.lastProgress = now
		log.Infof("--- progress: %s: %d statements/queries", file, t.progress)
	}
}

type errorArg struct {
	// error message, if any.
	msg string
	// pgcode for the error, if any.
	code string
}

func (err *errorArg) String() string {
	return fmt.Sprintf("{msg:\"%s\" code: \"%s\"}", err.msg, err.code)
}

// logicStatement represents a single statement test in Test-Script.
type logicStatement struct {
	t *testing.T

	// file and line number of the test.
	pos string
	// SQL string to be sent to the database.
	sql string
	// expected error, if any.
	expectErr *errorArg
	// expected rows affected count. -1 to avoid testing this.
	expectCount int64
}

func (stmt *logicStatement) Errorf(format string, args ...interface{}) {
	stmt.t.Error(fmt.Sprintf("%s %s", stmt.pos, fmt.Sprintf(format, args...)))
}

func (stmt *logicStatement) Error(args ...interface{}) {
	stmt.t.Error(fmt.Sprintf("%s %s", stmt.pos, fmt.Sprint(args...)))
}

// logicQuery represents a single query test in Test-Script.
type logicQuery struct {
	logicStatement

	// colTypes indicates the expected result column types.
	colTypes string
	// colNames controls the inclusion of column names in the query result.
	colNames bool
	// retry indicates if the query should be retried in case of failure with
	// exponential backoff up to some maximum duration.
	retry bool
	// some tests require the output to match modulo sorting.
	sorter logicSorter
	// expectedErr and expectedErrCode are as in logicStatement.

	// if set, the results are cross-checked against previous queries with the
	// same label.
	label string

	checkResults bool
	// valsPerLine is the number of values included in each line of the expected
	// results. This can either be 1, or else it must match the number of expected
	// columns (i.e. len(colTypes)).
	valsPerLine int
	// expectedResults indicates the expected sequence of text words
	// when flattening a query's results.
	expectedResults []string
	// expectedResultsRaw is the same as expectedResults, but
	// retaining the original formatting (whitespace, indentation) as
	// the test input file. This is used for pretty-printing unexpected
	// results.
	expectedResultsRaw []string
	// expectedHash indicates the expected hash of all result rows
	// combined. "" indicates hash checking is disabled.
	expectedHash string

	// expectedValues indicates the number of rows expected when
	// expectedHash is set.
	expectedValues int

	// rawOpts are the query options, before parsing. Used to display in error
	// messages.
	rawOpts string
}
