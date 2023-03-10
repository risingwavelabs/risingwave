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

package com.risingwave.java.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class VnodeHelper {
    public static <R> List<R> splitGroup(
            int groupCount, int vnodeCount, Function<List<Integer>, R> groupBuilder) {
        List<R> groups = new ArrayList<>(vnodeCount);
        int vnodePerGroup = vnodeCount / groupCount;
        int remainCount = vnodeCount % groupCount;
        int nextVnodeId = 0;
        for (int i = 0; i < groupCount; i++) {
            List<Integer> vnodeIds = new ArrayList<>();
            for (int j = 0; j < vnodePerGroup; j++) {
                vnodeIds.add(nextVnodeId);
                nextVnodeId++;
            }
            if (remainCount > 0) {
                remainCount--;
                vnodeIds.add(nextVnodeId);
                nextVnodeId++;
            }
            groups.add(groupBuilder.apply(vnodeIds));
        }
        return groups;
    }
}
