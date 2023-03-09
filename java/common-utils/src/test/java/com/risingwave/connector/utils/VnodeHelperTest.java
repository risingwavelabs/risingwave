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
