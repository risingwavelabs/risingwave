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

package com.risingwave.connector.source.common;

import com.risingwave.connector.api.TableSchema;
import java.sql.SQLException;
import java.util.Map;

public class CitusValidator extends PostgresValidator {
    public CitusValidator(Map<String, String> userProps, TableSchema tableSchema)
            throws SQLException {
        super(userProps, tableSchema);
    }

    @Override
    protected void alterPublicationIfNeeded() throws SQLException {
        // do nothing for citus worker node,
        // since we created a FOR ALL TABLES publication when creating the connector,
        // which will replicates changes for all tables in the database, including tables created in
        // the future.
    }
}
