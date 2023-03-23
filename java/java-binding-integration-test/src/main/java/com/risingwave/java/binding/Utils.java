// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.risingwave.java.binding;

public class Utils {
    public static void validateRow(BaseRow row) {
        // The validation of row data are according to the data generation rule
        // defined in ${REPO_ROOT}/src/java_binding/gen-demo-insert-data.py
        short rowIndex = row.getShort(0);
        if (row.getInt(1) != rowIndex) {
            throw new RuntimeException(
                    String.format("invalid int value: %s %s", row.getInt(1), rowIndex));
        }
        if (row.getLong(2) != rowIndex) {
            throw new RuntimeException(
                    String.format("invalid long value: %s %s", row.getLong(2), rowIndex));
        }
        if (row.getDecimal(3).equals(rowIndex)) {
            throw new RuntimeException(
                    String.format("invalid decimal value: %s %s", row.getDecimal(3), rowIndex));
        }
        if (row.getDouble(4) != (double) rowIndex) {
            throw new RuntimeException(
                    String.format("invalid double value: %s %s", row.getDouble(4), rowIndex));
        }
        if (row.getBoolean(5) != (rowIndex % 3 == 0)) {
            throw new RuntimeException(
                    String.format(
                            "invalid bool value: %s %s", row.getBoolean(5), (rowIndex % 3 == 0)));
        }
        if (!row.getString(6).equals(((Short) rowIndex).toString().repeat((rowIndex % 10) + 1))) {
            throw new RuntimeException(
                    String.format(
                            "invalid string value: %s %s",
                            row.getString(6),
                            ((Short) rowIndex).toString().repeat((rowIndex % 10) + 1)));
        }
        System.err.format("var: %s\n", row.getDateTime(7));

        if (row.isNull(8) != (rowIndex % 5 == 0)) {
            throw new RuntimeException(
                    String.format(
                            "invalid isNull value: %s %s", row.isNull(7), (rowIndex % 5 == 0)));
        }
    }
}
