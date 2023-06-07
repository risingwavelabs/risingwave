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

package com.risingwave.connector;

import com.google.common.collect.Lists;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.proto.ConnectorServiceProto;
import com.risingwave.proto.Data;
import java.util.List;

public class TestUtils {

    public static TableSchema getMockTableSchema() {
        return new TableSchema(
                Lists.newArrayList("id", "name"),
                Lists.newArrayList(
                        Data.DataType.newBuilder()
                                .setTypeName(Data.DataType.TypeName.INT32)
                                .build(),
                        Data.DataType.newBuilder()
                                .setTypeName(Data.DataType.TypeName.VARCHAR)
                                .build()),
                Lists.newArrayList("id"));
    }

    public static ConnectorServiceProto.TableSchema getMockTableProto() {
        return ConnectorServiceProto.TableSchema.newBuilder()
                .addColumns(
                        ConnectorServiceProto.TableSchema.Column.newBuilder()
                                .setName("id")
                                .setDataType(
                                        Data.DataType.newBuilder()
                                                .setTypeName(Data.DataType.TypeName.INT32)
                                                .build())
                                .build())
                .addColumns(
                        ConnectorServiceProto.TableSchema.Column.newBuilder()
                                .setName("name")
                                .setDataType(
                                        Data.DataType.newBuilder()
                                                .setTypeName(Data.DataType.TypeName.VARCHAR)
                                                .build())
                                .build())
                .addAllPkIndices(List.of(1))
                .build();
    }
}
