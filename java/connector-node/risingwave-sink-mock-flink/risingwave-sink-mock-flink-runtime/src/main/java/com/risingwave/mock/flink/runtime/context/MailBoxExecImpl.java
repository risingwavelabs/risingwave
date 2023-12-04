/*
 * Copyright 2023 RisingWave Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.risingwave.mock.flink.runtime.context;

import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.ThrowingRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple Implementation of Flink MailboxExecutor. Only the mailbox method of executing a mail via
 * `yield` call is implemented. In flink, there is also a background thread that polls for
 * execution, which is not implemented because it is not currently used.
 *
 * <p>Specifically, the class has a queue of different mails saved in the queue, and passed
 * downstream, when the downstream call yield, back to the fifo way to pick the mail execution
 */
public class MailBoxExecImpl implements MailboxExecutor {
    // The size of queue is Integer.MAX. In other words, we won't limit the number of emails.
    BlockingQueue<Mail> queue = new LinkedBlockingQueue<>();

    private static final Logger LOG = LoggerFactory.getLogger(MailBoxExecImpl.class);

    class Mail {
        ThrowingRunnable<? extends Exception> command;
        String descriptionFormat;
        Object[] descriptionArgs;

        public Mail(
                ThrowingRunnable<? extends Exception> command,
                String descriptionFormat,
                Object... descriptionArgs) {
            this.command = command;
            this.descriptionFormat = descriptionFormat;
            this.descriptionArgs = Arrays.stream(descriptionArgs).toArray();
        }
    }

    /**
     * Create a mail and add it to the queue
     *
     * @param command the runnable task to add to the mailbox for execution.
     * @param descriptionFormat the optional description for the command that is used for debugging
     *     and error-reporting.
     * @param descriptionArgs the parameters used to format the final description string.
     */
    @Override
    public void execute(
            ThrowingRunnable<? extends Exception> command,
            String descriptionFormat,
            Object... descriptionArgs) {
        queue.add(new Mail(command, descriptionFormat, descriptionArgs));
    }

    /** Pick a mail to execute, join the queue as empty, then block and wait */
    @Override
    public void yield() throws InterruptedException, FlinkRuntimeException {
        Mail headMail;
        while (true) {
            if ((headMail = queue.poll(1, TimeUnit.SECONDS)) != null) {
                break;
            } else {
                LOG.warn("Query mail queue wait timeout, will continue to wait");
            }
        }
        try {
            headMail.command.run();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** Pick a mail to execute, join the queue as empty, then return false. */
    @Override
    public boolean tryYield() throws FlinkRuntimeException {
        Mail headMail = queue.poll();
        if (headMail != null) {
            try {
                headMail.command.run();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return true;
        } else {
            return false;
        }
    }
}
