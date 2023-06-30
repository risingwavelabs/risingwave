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

import static io.grpc.Status.*;

import com.risingwave.connector.api.sink.SinkFactory;

public class SinkUtils {
    public static SinkFactory getSinkFactory(String sinkType) {
        switch (sinkType) {
            case "file":
                return new FileSinkFactory();
            case "jdbc":
                return new JDBCSinkFactory();
            case "iceberg":
                return new IcebergSinkFactory();
            case "deltalake":
                return new DeltaLakeSinkFactory();
            case "elasticsearch":
                return new EsSinkFactory();
            default:
                throw UNIMPLEMENTED
                        .withDescription("unknown sink type: " + sinkType)
                        .asRuntimeException();
        }
    }
}
