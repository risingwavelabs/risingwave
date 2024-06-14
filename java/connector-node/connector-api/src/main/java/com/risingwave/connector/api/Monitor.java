package com.risingwave.connector.api;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

public class Monitor {

    public static String dumpStackTrace() {
        StringBuilder builder = new StringBuilder();
        ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();
        for (ThreadInfo ti : threadMxBean.dumpAllThreads(true, true)) {
            builder.append(ti.toString());
        }
        return builder.toString();
    }
}
