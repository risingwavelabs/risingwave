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

package com.risingwave.connector.utils;

import com.risingwave.java.utils.VnodeHelper;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

public class VnodeHelperTest {
    @Test
    public void testVnodeSplit() {
        testSplitGroupInner(16, 30000);
        testSplitGroupInner(23, 100);
    }

    void testSplitGroupInner(int splitCount, int vnodeCount) {
        List<List<Integer>> vnodeGroups =
                VnodeHelper.splitGroup(splitCount, vnodeCount, vnodeIds -> vnodeIds);
        Set<Integer> vnodeSet = new HashSet<Integer>();
        int nodePerGroup = vnodeCount / splitCount;
        for (List<Integer> vnodeGroup : vnodeGroups) {
            assertContinuous(vnodeGroup);
            for (Integer vnode : vnodeGroup) {
                vnodeSet.add(vnode);
            }
            Assert.assertTrue(
                    vnodeGroup.size() == nodePerGroup || vnodeGroup.size() == nodePerGroup + 1);
        }
        Assert.assertEquals(vnodeSet.size(), vnodeCount);
    }

    void assertContinuous(List<Integer> vnodeIds) {
        for (int i = 0; i < vnodeIds.size() - 1; i++) {
            Assert.assertEquals(vnodeIds.get(i) + 1, (long) vnodeIds.get(i + 1));
        }
    }
}
