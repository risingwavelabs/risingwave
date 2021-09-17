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
	"bytes"
	"crypto/md5"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"text/tabwriter"

	"github.com/lib/pq"
)

func hashResults(results []string) (string, error) {
	// Hash the values using MD5. This hashing precisely matches the hashing in
	// sqllogictest.c.
	h := md5.New()
	for _, r := range results {
		if _, err := h.Write(append([]byte(r), byte('\n'))); err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func (query *logicQuery) deepEquals(t *logicTest, actualResultsRaw []string, numCols int) error {
	// Normalize each row in the result by mapping each run of contiguous
	// whitespace to a single space.
	var actualResults []string
	if actualResultsRaw != nil {
		actualResults = make([]string, 0, len(actualResultsRaw))
		for _, result := range actualResultsRaw {
			if query.sorter == nil || query.valsPerLine != 1 {
				actualResults = append(actualResults, strings.Fields(result)...)
			} else {
				actualResults = append(actualResults, strings.Join(strings.Fields(result), " "))
			}
		}
	}

	if query.sorter != nil {
		query.sorter(numCols, actualResults)
		query.sorter(numCols, query.expectedResults)
	}

	hash, err := hashResults(actualResults)
	if err != nil {
		return err
	}

	if query.expectedHash != "" {
		n := len(actualResults)
		if query.expectedValues != n {
			return fmt.Errorf("%s: expected %d results, but found %d", query.pos, query.expectedValues, n)
		}
		if query.expectedHash != hash {
			return fmt.Errorf("%s: expected %s, but found %s", query.pos, query.expectedHash, hash)
		}
	}

	if query.checkResults && !reflect.DeepEqual(query.expectedResults, actualResults) {
		var buf bytes.Buffer
		fmt.Fprintf(&buf, "%s: %s\nexpected:\n", query.pos, query.sql)
		for _, line := range query.expectedResultsRaw {
			fmt.Fprintf(&buf, "    %s\n", line)
		}
		sortMsg := ""
		if query.sorter != nil {
			// We performed an order-insensitive comparison of "actual" vs "expected"
			// rows by sorting both, but we'll display the error with the expected
			// rows in the order in which they were put in the file, and the actual
			// rows in the order in which the query returned them.
			sortMsg = " -> ignore the following ordering of rows"
		}
		fmt.Fprintf(&buf, "but found (query options: %q%s) :\n", query.rawOpts, sortMsg)
		for _, line := range t.formatValues(actualResultsRaw, query.valsPerLine) {
			fmt.Fprintf(&buf, "    %s\n", line)
		}
		return errors.New(buf.String())
	}

	if query.label != "" {
		if prevHash, ok := t.labelMap[query.label]; ok && prevHash != hash {
			query.Errorf(
				"error in input: previous values for label %s (hash %s) do not match (hash %s)",
				query.label, prevHash, hash,
			)
		}
		t.labelMap[query.label] = hash
	}

	return nil
}

func (query *logicQuery) verifyValueType(val interface{}, colIdx int) error {
	valT := reflect.TypeOf(val).Kind()
	colT := query.colTypes[colIdx]
	switch colT {
	case 'T': // For text.
		if valT != reflect.String && valT != reflect.Slice && valT != reflect.Struct {
			return fmt.Errorf("%s: expected text value for column %d, but found %T: %#v",
				query.pos, colIdx, val, val,
			)
		}
	case 'I': // For integer.
		if valT != reflect.Int64 {
			return fmt.Errorf("%s: expected int value for column %d, but found %T: %#v",
				query.pos, colIdx, val, val,
			)
		}
	case 'R': // For floating-point.
		if valT != reflect.Float64 && valT != reflect.Slice {
			return fmt.Errorf("%s: expected float/decimal value for column %d, but found %T: %#v",
				query.pos, colIdx, val, val,
			)
		}
	case 'B': // For boolean.
		if valT != reflect.Bool {
			return fmt.Errorf("%s: expected boolean value for column %d, but found %T: %#v",
				query.pos, colIdx, val, val,
			)
		}
	case 'O': // For oid.
		if valT != reflect.Slice {
			return fmt.Errorf("%s: expected oid value for column %d, but found %T: %#v",
				query.pos, colIdx, val, val,
			)
		}
	default:
		return fmt.Errorf("%s: unknown type in type string: %c in %s",
			query.pos, colT, query.colTypes,
		)
	}
	return nil
}

func (t *logicTest) formatValues(vals []string, valsPerLine int) []string {
	var buf bytes.Buffer
	tw := tabwriter.NewWriter(&buf, 2, 1, 2, ' ', 0)

	for line := 0; line < len(vals)/valsPerLine; line++ {
		for i := 0; i < valsPerLine; i++ {
			fmt.Fprintf(tw, "%s\t", vals[line*valsPerLine+i])
		}
		fmt.Fprint(tw, "\n")
	}
	_ = tw.Flush()

	// Split into lines and trim any trailing whitespace.
	// Note that the last line will be empty (which is what we want).
	results := make([]string, 0, len(vals)/valsPerLine)
	for _, s := range strings.Split(buf.String(), "\n") {
		results = append(results, strings.TrimRight(s, " "))
	}
	return results
}

// verifyError checks that either no error was found where none was
// expected, or that an error was found when one was expected.
// Returns a nil error to indicate the behavior was as expected.
func verifyError(sql, pos string, expectErr *errorArg, err error) error {
	if expectErr == nil {
		return err
	}
	if err == nil {
		return fmt.Errorf("%s: %s\nexpected %q, but no error occurred", pos, sql, expectErr)
	}
	// expectErr != nil && err != nil

	if expectErr.code != "" {
		pqErr, ok := err.(*pq.Error)
		if !ok {
			return fmt.Errorf("%s %s\n: expected error code %q, but the error we found is not "+
				"a libpq error: %s", pos, sql, expectErr.code, err)
		}
		if expectErr.code != string(pqErr.Code) {
			return fmt.Errorf(
				"%s: %s\n: serious error with code %q occurred; if expected, must use 'error pgcode %s ...' in test:\n%s",
				pos, sql, pqErr.Code, pqErr.Code, err.Error())
		}
	}

	if expectErr.msg != "" && !strings.Contains(err.Error(), expectErr.msg) { // The actual error doesn't match with the expected.
		return fmt.Errorf("%s: %s\nexpected:\n%s\n\ngot:\n%s", pos, sql, expectErr, err.Error())
	}
	return nil
}
