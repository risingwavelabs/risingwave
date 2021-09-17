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
	"path/filepath"
	"testing"
	"time"
)

// RunLogicTest is the main entry point for the logic test. The globs parameter
// specifies the default sets of files to run.
func RunLogicTest(t *testing.T, glob string) {
	// Set time.Local to time.UTC to circumvent pq's timetz parsing flaw.
	time.Local = time.UTC

	paths, err := filepath.Glob(glob)
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("No test file is specified.")
	}

	// mu protects the following vars, which all get updated from within the
	// possibly parallel subtests.
	var progress = struct {
		total, totalFail, totalUnsupported int
		lastProgress                       time.Time
	}{
		lastProgress: time.Now(),
	}

	// Inner test: one per file path.
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			lt := logicTest{
				t:               t,
				perErrorSummary: make(map[string][]string),
			}
			lt.setup()

			fullPath, _ := filepath.Abs(path)
			lt.runFile(fullPath)

			progress.total += lt.progress
			progress.totalFail += lt.failures
			progress.totalUnsupported += lt.unsupported
			now := time.Now()
			if now.Sub(progress.lastProgress) >= 2*time.Second {
				progress.lastProgress = now
			}
		})
	}
}

func (t *logicTest) setup() {
	db, err := NewDatabase()
	if err != nil {
		t.t.Fatal(err)
	}
	t.db = db
}

func (t *logicTest) runFile(path string) {
	if err := t.processTestFile(path); err != nil {
		t.t.Fatal(err)
	}
}
