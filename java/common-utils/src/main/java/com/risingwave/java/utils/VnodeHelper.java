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
