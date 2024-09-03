package org.ldbcouncil.snb.driver.runtime;

import org.ldbcouncil.snb.driver.AtomicLongHolder;
import org.ldbcouncil.snb.driver.control.LoggingService;
import org.ldbcouncil.snb.driver.control.LoggingServiceFactory;
import org.ldbcouncil.snb.driver.control.RecentThroughputAndDuration;
import org.ldbcouncil.snb.driver.runtime.coordination.CompletionTimeService;
import org.ldbcouncil.snb.driver.runtime.metrics.MetricsService.MetricsServiceWriter;
import org.ldbcouncil.snb.driver.runtime.metrics.WorkloadStatusSnapshot;
import org.ldbcouncil.snb.driver.runtime.scheduling.Spinner;

import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;

class WorkloadStatusThread extends Thread {
    private final long statusUpdateIntervalAsMilli;
    private final MetricsServiceWriter metricsServiceWriter;
    private final ConcurrentErrorReporter errorReporter;
    private final CompletionTimeService completionTimeService;
    private final LoggingService loggingService;
    private final AtomicLongHolder atomicLongHolder;
    private AtomicBoolean continueRunning = new AtomicBoolean(true);

    WorkloadStatusThread(
            long statusUpdateIntervalAsMilli,
            MetricsServiceWriter metricsServiceWriter,
            ConcurrentErrorReporter errorReporter,
            CompletionTimeService completionTimeService,
            LoggingServiceFactory loggingServiceFactory, AtomicLongHolder atomicLongHolder) {
        super(WorkloadStatusThread.class.getSimpleName() + "-" + System.currentTimeMillis());
        this.statusUpdateIntervalAsMilli = statusUpdateIntervalAsMilli;
        this.metricsServiceWriter = metricsServiceWriter;
        this.errorReporter = errorReporter;
        this.completionTimeService = completionTimeService;
        this.loggingService = loggingServiceFactory.loggingServiceFor(getClass().getSimpleName());
        this.atomicLongHolder = atomicLongHolder;
    }

    @Override
    public void run() {
        final SettableRecentThroughputAndDuration settableRecentThroughputAndDuration = new SettableRecentThroughputAndDuration();
        final int statusRecency = 4;
        final long[][] operationCountsAtDurations = new long[statusRecency][2];
        for (int i = 0; i < operationCountsAtDurations.length; i++) {
            operationCountsAtDurations[i][0] = -1;
            operationCountsAtDurations[i][1] = -1;
        }
        int statusRecencyIndex = 0;
        Long last = 0L;
        Long curr = this.atomicLongHolder.get();

        while (continueRunning.get()) {
            try {
                WorkloadStatusSnapshot status = metricsServiceWriter.status();
                operationCountsAtDurations[statusRecencyIndex][0] = status.operationCount();
                operationCountsAtDurations[statusRecencyIndex][1] = status.runDurationAsMilli();
                statusRecencyIndex = (statusRecencyIndex + 1) % statusRecency;
                updateRecentThroughput(operationCountsAtDurations,
                        settableRecentThroughputAndDuration);

                loggingService.status(
                        status,
                        settableRecentThroughputAndDuration,
                        completionTimeService.completionTimeAsMilli());
                // long startTime = System.nanoTime();
                // last = curr;

                Spinner.powerNap(statusUpdateIntervalAsMilli);
                // curr = this.atomicLongHolder.get();
                // long endTime = System.nanoTime();
                // long timeElapsed = endTime - startTime;
                // double operationsPerSecond = (curr - last) * 1.0 / (timeElapsed /
                // 1_000_000_000.0);
                // System.out.println("last 1s throughput: " + operationsPerSecond);
            } catch (Throwable e) {
                errorReporter.reportError(
                        this,
                        format(
                                "Status reporting thread encountered unexpected error - exiting\n%s",
                                ConcurrentErrorReporter.stackTraceToString(e)));
                break;
            }
        }
    }

    synchronized public final void shutdown() {
        if (false == continueRunning.get()) {
            return;
        }
        continueRunning.set(false);
    }

    private void updateRecentThroughput(final long[][] recentOperationCountsAtDurations,
            final SettableRecentThroughputAndDuration settableRecentThroughputAndDuration) {
        long minOperationCount = Long.MAX_VALUE;
        long maxOperationCount = Long.MIN_VALUE;
        long minDurationAsMilli = Long.MAX_VALUE;
        long maxDurationAsMilli = Long.MIN_VALUE;
        for (int i = 0; i < recentOperationCountsAtDurations.length; i++) {
            long operationCount = recentOperationCountsAtDurations[i][0];
            long durationAsMilli = recentOperationCountsAtDurations[i][1];
            if (-1 == operationCount) {
                continue;
            }
            minOperationCount = Math.min(minOperationCount, operationCount);
            maxOperationCount = Math.max(maxOperationCount, operationCount);
            minDurationAsMilli = Math.min(minDurationAsMilli, durationAsMilli);
            maxDurationAsMilli = Math.max(maxDurationAsMilli, durationAsMilli);
        }
        long recentRunDurationAsMilli = maxDurationAsMilli - minDurationAsMilli;
        long recentOperationCount = maxOperationCount - minOperationCount;
        double recentThroughput = (0 == recentRunDurationAsMilli)
                ? 0
                : (double) recentOperationCount / recentRunDurationAsMilli * 1000;
        settableRecentThroughputAndDuration.setThroughput(recentThroughput);
        settableRecentThroughputAndDuration.setDuration(recentRunDurationAsMilli);
    }

    private class SettableRecentThroughputAndDuration implements RecentThroughputAndDuration {
        private double throughput = 0.0;
        private long duration = 0;

        private void setThroughput(double throughput) {
            this.throughput = throughput;
        }

        private void setDuration(long duration) {
            this.duration = duration;
        }

        public double throughput() {
            return throughput;
        }

        public long duration() {
            return duration;
        }
    }
}
