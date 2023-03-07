package com.risingwave.connector.api;

import io.grpc.Status;
import java.util.Comparator;
import java.util.List;

public class PkComparator implements Comparator<List<Comparable<Object>>> {

    @Override
    public int compare(List<Comparable<Object>> objects1, List<Comparable<Object>> objects2) {
        int cnt1 = objects1.size();
        int cnt2 = objects2.size();
        if (cnt1 != cnt2) {
            throw Status.FAILED_PRECONDITION
                    .withDescription(
                            String.format("primary key lengths %d and %d do not match", cnt1, cnt2))
                    .asRuntimeException();
        }
        for (int i = 0; i < cnt1; i++) {
            int res = objects1.get(i).compareTo(objects2.get(i));
            if (res != 0) {
                return res;
            }
        }
        return 0;
    }
}
