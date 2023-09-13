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

package com.risingwave.connector.api.sink;

import com.risingwave.proto.ConnectorServiceProto;
import java.util.Iterator;
import java.util.Optional;

public interface SinkWriterV1 {
    void write(Iterator<SinkRow> rows);

    void sync();

    void drop();

    class Adapter implements SinkWriter {

        private final SinkWriterV1 inner;
        private boolean hasBegun;

        public Adapter(SinkWriterV1 inner) {
            this.inner = inner;
            this.hasBegun = false;
        }

        public SinkWriterV1 getInner() {
            return inner;
        }

        @Override
        public void beginEpoch(long epoch) {}

        @Override
        public void write(Iterator<SinkRow> rows) {
            if (!hasBegun) {
                hasBegun = true;
            }
            this.inner.write(rows);
        }

        @Override
        public Optional<ConnectorServiceProto.SinkMetadata> barrier(boolean isCheckpoint) {
            if (isCheckpoint) {
                if (hasBegun) {
                    this.inner.sync();
                    this.hasBegun = false;
                }
            }
            return Optional.empty();
        }

        @Override
        public void drop() {
            this.inner.drop();
        }
    }
}
