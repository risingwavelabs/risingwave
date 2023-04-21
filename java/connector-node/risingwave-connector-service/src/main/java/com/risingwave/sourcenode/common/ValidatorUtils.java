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

package com.risingwave.sourcenode.common;

import io.grpc.Status;

public abstract class ValidatorUtils {
    public static RuntimeException invalidArgument(String description) {
        return Status.INVALID_ARGUMENT.withDescription(description).asRuntimeException();
    }

    public static RuntimeException internalError(String description) {
        return Status.INTERNAL.withDescription(description).asRuntimeException();
    }
}
