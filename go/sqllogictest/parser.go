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
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	errorRE = regexp.MustCompile(`^(?:statement|query)\s+error(?:\s+pgcode\s+([[:alnum:]]+))?(?:\s+(.+))?\s*$`)
)

// lineScanner handles reading from input test files.
type lineScanner struct {
	*bufio.Scanner
	line int
}

func newLineScanner(r io.Reader) *lineScanner {
	return &lineScanner{
		Scanner: bufio.NewScanner(r),
		line:    0,
	}
}

// Scan reads one line from file, if no data to read, it returns false.
func (l *lineScanner) Scan() bool {
	ok := l.Scanner.Scan()
	if ok {
		l.line++
	}
	return ok
}

func (l *lineScanner) Text() string {
	return l.Scanner.Text()
}

type subtestDetails struct {
	name        string        // the subtest's name, empty if not a subtest
	buffer      *bytes.Buffer // a chunk of the test file representing the subtest
	startLineNo int           // the line number of the test file where the subtest started
}

// fetchSubtests reads through the test file and splices it into subtest chunks.
// If there is no subtest, the output will only contain a single entry.
func fetchSubtests(testName string, fdata []byte) ([]subtestDetails, error) {
	s := newLineScanner(bytes.NewReader(fdata))
	var subtests []subtestDetails
	var curName string
	var curLineIndexIntoFile int
	buffer := &bytes.Buffer{}
	for s.Scan() {
		line := s.Text()
		fields := strings.Fields(line)
		if len(fields) > 0 && fields[0] == "subtest" {
			if len(fields) != 2 {
				return nil, fmt.Errorf(
					"%s:%d expected only one field following the subtest command\n"+
						"Note that this check does not respect the other commands so if a query result has a "+
						"line that starts with \"subtest\" it will either fail or be split into a subtest.",
					testName, s.line,
				)
			}
			subtests = append(subtests, subtestDetails{
				name:        curName,
				buffer:      buffer,
				startLineNo: curLineIndexIntoFile,
			})
			buffer = &bytes.Buffer{}
			curName = fields[1]
			curLineIndexIntoFile = s.line + 1
		} else {
			buffer.WriteString(line)
			buffer.WriteRune('\n')
		}
	}
	subtests = append(subtests, subtestDetails{
		name:        curName,
		buffer:      buffer,
		startLineNo: curLineIndexIntoFile,
	})

	return subtests, nil
}

// readSQL reads the lines of a SQL statement or query until the first blank
// line or (optionally) a "----" separator, and sets stmt.sql.
//
// If a separator is found, returns separator=true. If a separator is found when
// it is not expected, returns an error.
func (ls *logicStatement) readSQL(s *lineScanner, allowSeparator bool) (separator bool, err error) {
	var buf bytes.Buffer
	for s.Scan() {
		line := s.Text()
		if line == "" {
			break
		}
		if line == "----" {
			separator = true
			if ls.expectErr != nil {
				return false, fmt.Errorf(
					"%s: invalid ---- separator after a statement or query expecting an error: %s",
					ls.pos, ls.expectErr,
				)
			}
			if !allowSeparator {
				return false, fmt.Errorf("%s: unexpected ---- separator", ls.pos)
			}
			break
		}
		fmt.Fprintln(&buf, line)
	}
	ls.sql = strings.TrimSpace(buf.String())
	return separator, nil
}

type commandParser struct {
	curPath   string
	curLineNo int

	fields []string
	text   string // The full command in text.
}

func (p *commandParser) parseRepeat() (int, error) {
	// A line "repeat X" makes the test repeat the following statement or query X times.
	var err error
	count := 0
	if len(p.fields) != 2 {
		err = errors.New("invalid line format")
	} else if count, err = strconv.Atoi(p.fields[1]); err == nil && count < 2 {
		err = errors.New("invalid count")
	}
	if err != nil {
		return 0, p.wrapErrorf("nvalid repeat line: %s", err)
	}
	return count, nil
}

func (p *commandParser) parseSleep() (time.Duration, error) {
	var err error
	var duration time.Duration
	// A line "sleep Xs" makes the test sleep for X seconds.
	if len(p.fields) != 2 {
		err = errors.New("invalid line format")
	} else if duration, err = time.ParseDuration(p.fields[1]); err != nil {
		err = errors.New("invalid duration")
	}
	if err != nil {
		return 0, p.wrapErrorf("invalid sleep line: %s", err)
	}
	return duration, nil
}

func (stmt *logicStatement) tryParseError(text string) bool {
	// Parse "statement|query error <regexp>"
	if m := errorRE.FindStringSubmatch(text); m != nil {
		stmt.expectErr = &errorArg{code: m[1], msg: m[2]}
		return true
	}
	return false
}

func (p *commandParser) parseStatement(s *lineScanner) (*logicStatement, error) {
	stmt := &logicStatement{
		pos:         p.commandPosition(),
		expectCount: -1,
	}
	if stmt.tryParseError(p.text) {
	} else if len(p.fields) >= 3 && p.fields[1] == "count" {
		n, err := strconv.ParseInt(p.fields[2], 10, 64)
		if err != nil {
			return nil, err
		}
		stmt.expectCount = n
	}
	if _, err := stmt.readSQL(s, false /* allowSeparator */); err != nil {
		return nil, err
	}
	return stmt, nil
}

func (p *commandParser) parseQueryBlock(s *lineScanner) (*logicQuery, error) {
	query, err := p.parseQuery()
	if err != nil {
		return nil, err
	}

	separator, err := query.readSQL(s, true /* allowSeparator */)
	if err != nil {
		return nil, err
	}

	query.checkResults = true
	if separator {
		// Lines follow the separator is the expected query results.
		if err := p.parseQueryResult(s, query); err != nil {
			return nil, err
		}
	} else if query.label != "" {
		// Label and no separator; we won't be directly checking results; we
		// cross-check results between all queries with the same label.
		query.checkResults = false
	}
	return query, nil
}

func (p *commandParser) parseQuery() (*logicQuery, error) {
	// A query record begins with a line of the following form:
	//     query <type-string> <sort-mode> <label>
	// The SQL for the query is found on second an subsequent lines of
	// the record up to first line of the form "----" or until the end of the record.
	// Lines following the "----" are expected results of the query, one value per line.
	// If the "----" and/or the results are omitted, then the query is expected to return
	// an empty set.

	query := &logicQuery{}
	query.pos = fmt.Sprintf("\n%s:%d", p.curPath, p.curLineNo)

	if query.tryParseError(p.text) {
	} else if len(p.fields) < 2 {
		return nil, p.wrapErrorf("invalid query: %s", p.text)
	} else {
		// Parse "query <type-string> <options> <label>"
		query.colTypes = p.fields[1]
		// Expect number of values to match expected type count.
		query.valsPerLine = len(query.colTypes)

		if len(p.fields) >= 3 {
			query.rawOpts = p.fields[2]

			tokens := strings.Split(query.rawOpts, ",")

			// One of the options can be partialSort(1,2,3); we want this to be
			// a single token.
			for i := 0; i < len(tokens)-1; i++ {
				if strings.HasPrefix(tokens[i], "partialsort(") && !strings.HasSuffix(tokens[i], ")") {
					// Merge this token with the next.
					tokens[i] = tokens[i] + "," + tokens[i+1]
					// Delete tokens[i+1].
					copy(tokens[i+1:], tokens[i+2:])
					tokens = tokens[:len(tokens)-1]
					// Look at the new token again.
					i--
				}
			}

			for _, opt := range tokens {
				if strings.HasPrefix(opt, "partialsort(") && strings.HasSuffix(opt, ")") {
					s := opt
					s = strings.TrimPrefix(s, "partialsort(")
					s = strings.TrimSuffix(s, ")")

					var orderedCols []int
					for _, c := range strings.Split(s, ",") {
						colIdx, err := strconv.Atoi(c)
						if err != nil || colIdx < 1 {
							return nil, p.wrapErrorf("invalid sort mode: %s", opt)
						}
						orderedCols = append(orderedCols, colIdx-1)
					}
					if len(orderedCols) == 0 {
						return nil, p.wrapErrorf("invalid sort mode: %s", opt)
					}
					query.sorter = func(numCols int, values []string) {
						partialSort(numCols, orderedCols, values)
					}
					continue
				}

				switch opt {
				case "nosort":
					query.sorter = nil

				case "rowsort":
					query.sorter = rowSort

				case "valuesort":
					query.sorter = valueSort

				case "colnames":
					query.colNames = true

				case "retry":
					query.retry = true

				default:
					return nil, p.wrapErrorf("unknown sort mode: %s", opt)
				}
			}
		}
		if len(p.fields) >= 4 {
			query.label = p.fields[3]
		}
	}
	return query, nil
}

func (p *commandParser) parseQueryResult(s *lineScanner, query *logicQuery) error {
	// Query results are either a space separated list of values up to a
	// blank line or a line of the form "xx values hashing to yyy". The
	// latter format is used by sqllogictest when a large number of results
	// match the query.
	if !s.Scan() {
		return nil
	}

	// If the line matches to "xx values hashing to yyy".
	resultsRE := regexp.MustCompile(`^(\d+)\s+values?\s+hashing\s+to\s+([0-9A-Fa-f]+)$`)
	if m := resultsRE.FindStringSubmatch(s.Text()); m != nil {
		var err error
		query.expectedValues, err = strconv.Atoi(m[1])
		if err != nil {
			return err
		}
		query.expectedHash = m[2]
		query.checkResults = false
		return nil
	}

	for {
		// Normalize each expected row by discarding leading/trailing
		// whitespace and by replacing each run of contiguous whitespace
		// with a single space.
		query.expectedResultsRaw = append(query.expectedResultsRaw, s.Text())
		results := strings.Fields(s.Text())
		if len(results) == 0 {
			break
		}

		if query.sorter == nil {
			// When rows don't need to be sorted, then always compare by
			// tokens, regardless of where row/column boundaries are.
			query.expectedResults = append(query.expectedResults, results...)
		} else if query.valsPerLine == 1 {
			// It's important to know where row/column boundaries are in
			// order to correctly perform sorting. Assume that boundaries
			// are delimited by whitespace. This means that values cannot
			// contain whitespace when there are multiple columns, since
			// that would be interpreted as extra values:
			//
			//   foo bar baz
			//
			// If there are two expected columns, then it's not possible
			// to know whether the expected results are ("foo bar", "baz")
			// or ("foo", "bar baz"), so just error in that case.

			// Only one expected value per line, so never ambiguous,
			// even if there is whitespace in the value.
			query.expectedResults = append(query.expectedResults, strings.Join(results, " "))
		} else if len(results) != len(query.colTypes) {
			return errors.New("expected results are invalid: unexpected column count")
		} else {
			query.expectedResults = append(query.expectedResults, results...)
		}

		if !s.Scan() {
			break
		}
	}
	return nil
}

func (p *commandParser) wrapErrorf(str string, args ...interface{}) error {
	return errors.New(p.commandPosition() + ": " + fmt.Sprintf(str, args...))
}

func (p *commandParser) commandPosition() string {
	return fmt.Sprintf("%s:%d", p.curPath, p.curLineNo)
}
