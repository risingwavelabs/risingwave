// Copyright 2025 RisingWave Labs
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

public class CdcSourceChannel implements AutoCloseable {
    private final long pointer;

    CdcSourceChannel(long pointer) {
        this.pointer = pointer;
    }

    public static CdcSourceChannel fromOwnedPointer(long pointer) {
        return new CdcSourceChannel(pointer);
    }

    public boolean send(byte[] msg) {
        return Binding.sendCdcSourceMsgToChannel(pointer, msg);
    }

    public boolean sendError(String errorMsg) {
        return Binding.sendCdcSourceErrorToChannel(pointer, errorMsg);
    }

    @Override
    public void close() {
        Binding.cdcSourceSenderClose(pointer);
    }
}
