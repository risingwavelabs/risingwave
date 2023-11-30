package com.risingwave.connector.context;

import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.ThrowingRunnable;

/**
 * Simple Implementation of Flink MailboxExecutor. Only the mailbox method of executing a mail via
 * `yeild` call is implemented. In flink, there is also a background thread that polls for
 * execution, which is not implemented because it is not currently used.
 *
 * <p>Specifically, the class has a queue of different mails saved in the queue, and passed
 * downstream, when the downstream call yeild, back to the fifo way to pick the mail execution
 */
public class MailBoxExecImpl implements MailboxExecutor {
    BlockingQueue<Mail> queue = new LinkedBlockingQueue<>();

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
                try {
                    headMail.command.run();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return;
            }
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
