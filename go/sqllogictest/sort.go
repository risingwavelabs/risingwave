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

import "sort"

// logicSorter sorts result rows (or not) depending on Test-Script's
// sorting option for a "query" test. See the implementation of the
// "query" directive below for details.
type logicSorter func(numCols int, values []string)

type rowSorter struct {
	numCols int
	numRows int
	values  []string
}

func (r rowSorter) row(i int) []string {
	return r.values[i*r.numCols : (i+1)*r.numCols]
}

func (r rowSorter) Len() int {
	return r.numRows
}

func (r rowSorter) Less(i, j int) bool {
	a := r.row(i)
	b := r.row(j)
	for k := range a {
		if a[k] < b[k] {
			return true
		}
		if a[k] > b[k] {
			return false
		}
	}
	return false
}

func (r rowSorter) Swap(i, j int) {
	a := r.row(i)
	b := r.row(j)
	for i := range a {
		a[i], b[i] = b[i], a[i]
	}
}

func rowSort(numCols int, values []string) {
	sort.Sort(rowSorter{
		numCols: numCols,
		numRows: len(values) / numCols,
		values:  values,
	})
}

func valueSort(numCols int, values []string) {
	sort.Strings(values)
}

// partialSort rearranges consecutive rows that have the same values on a
// certain set of columns (orderedCols).
//
// More specifically: rows are partitioned into groups of consecutive rows that
// have the same values for columns orderedCols. Inside each group, the rows are
// sorted. The relative order of any two rows that differ on orderedCols is
// preserved.
//
// This is useful when comparing results for a statement that guarantees a
// partial, but not a total order. Consider:
//
//   SELECT a, b FROM ab ORDER BY a
//
// Some possible outputs for the same data:
//   1 2        1 5        1 2
//   1 5        1 4        1 4
//   1 4   or   1 2   or   1 5
//   2 3        2 2        2 3
//   2 2        2 3        2 2
//
// After a partialSort with orderedCols = {0} all become:
//   1 2
//   1 4
//   1 5
//   2 2
//   2 3
//
// An incorrect output like:
//   1 5                          1 2
//   1 2                          1 5
//   2 3          becomes:        2 2
//   2 2                          2 3
//   1 4                          1 4
// and it is detected as different.
func partialSort(numCols int, orderedCols []int, values []string) {
	// We use rowSorter here only as a container.
	c := rowSorter{
		numCols: numCols,
		numRows: len(values) / numCols,
		values:  values,
	}

	// Sort the group of rows [rowStart, rowEnd).
	sortGroup := func(rowStart, rowEnd int) {
		sort.Sort(rowSorter{
			numCols: numCols,
			numRows: rowEnd - rowStart,
			values:  values[rowStart*numCols : rowEnd*numCols],
		})
	}

	groupStart := 0
	for rIdx := 1; rIdx < c.numRows; rIdx++ {
		// See if this row belongs in the group with the previous row.
		row := c.row(rIdx)
		start := c.row(groupStart)
		differs := false
		for _, i := range orderedCols {
			if start[i] != row[i] {
				differs = true
				break
			}
		}
		if differs {
			// Sort the group and start a new group with just this row in it.
			sortGroup(groupStart, rIdx)
			groupStart = rIdx
		}
	}
	sortGroup(groupStart, c.numRows)
}
