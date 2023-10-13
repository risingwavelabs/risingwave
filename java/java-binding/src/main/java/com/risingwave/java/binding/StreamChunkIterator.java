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

public class StreamChunkIterator implements AutoCloseable {
    private final long pointer;
    private boolean isClosed;

    public StreamChunkIterator(byte[] streamChunkPayload) {
        this.pointer = Binding.iteratorNewFromStreamChunkPayload(streamChunkPayload);
        this.isClosed = false;
    }

    /**
     * This method generate the StreamChunkIterator
     *
     * @param str A string that represent table format, content and operation. Example:"I I\n + 199
     *     40"
     */
    public StreamChunkIterator(String str) {
        this.pointer = Binding.iteratorNewFromStreamChunkPretty(str);
        this.isClosed = false;
    }

    public StreamChunkRow next() {
        boolean hasNext = Binding.iteratorNext(this.pointer);
        if (!hasNext) {
            return null;
        }
        return new StreamChunkRow(pointer);
    }

    @Override
    public void close() {
        if (!isClosed) {
            isClosed = true;
            Binding.iteratorClose(pointer);
        }
    }
}
