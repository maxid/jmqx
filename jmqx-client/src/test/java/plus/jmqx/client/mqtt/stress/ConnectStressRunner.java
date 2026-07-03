package plus.jmqx.client.mqtt.stress;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 连接压测：并发建连 → 持久保持 → 批量断开，用于压测同时在线连接数。
 */
@Slf4j
public final class ConnectStressRunner {

    private ConnectStressRunner() {
    }

    public record ConnectStats(long established, long peakActive, long completed, long failed,
                               boolean completedInTime) {
    }

    @FunctionalInterface
    public interface ConnectionHandle {
        void disconnectQuietly();
    }

    @FunctionalInterface
    public interface ConnectionOpener {
        ConnectionHandle open(String clientId) throws Exception;
    }

    public static ConnectStats run(String label, ClientStressConfig c, ConnectionOpener opener) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(Math.min(c.threads, c.connections));
        AtomicLong active = new AtomicLong();
        AtomicLong established = new AtomicLong();
        AtomicLong completed = new AtomicLong();
        AtomicLong failed = new AtomicLong();
        AtomicLong peakActive = new AtomicLong();
        List<ConnectionHandle> handles = Collections.synchronizedList(new ArrayList<>(c.connections));

        long start = System.nanoTime();
        boolean finishedInTime;
        try (StressProgressReporter progressReporter = StressProgressReporter.start(
                label + "-connect", c.progressIntervalSeconds, start,
                () -> ClientStressSupport.formatConnectProgress(
                        c.connections, active.get(), established.get(), completed.get(), start))) {

            CountDownLatch connectLatch = new CountDownLatch(c.connections);
            for (int i = 0; i < c.connections; i++) {
                final int idx = i;
                pool.submit(() -> {
                    try {
                        ConnectionHandle handle = opener.open("stress-conn-" + idx + "-" + System.nanoTime());
                        handles.add(handle);
                        established.incrementAndGet();
                        long nowActive = active.incrementAndGet();
                        peakActive.updateAndGet(prev -> Math.max(prev, nowActive));
                    } catch (Exception e) {
                        failed.incrementAndGet();
                    } finally {
                        connectLatch.countDown();
                    }
                });
            }
            boolean connectedInTime = connectLatch.await(c.timeoutSeconds, TimeUnit.SECONDS);
            assertTrue(connectedInTime, "connect phase timed out");

            if (c.connectionHoldSeconds > 0) {
                log.info("{} connect hold: established={}, holding for {}s",
                        label, established.get(), c.connectionHoldSeconds);
                Thread.sleep(c.connectionHoldSeconds * 1000L);
            }

            CountDownLatch disconnectLatch = new CountDownLatch(handles.size());
            for (ConnectionHandle handle : handles) {
                pool.submit(() -> {
                    try {
                        handle.disconnectQuietly();
                        completed.incrementAndGet();
                    } finally {
                        active.decrementAndGet();
                        disconnectLatch.countDown();
                    }
                });
            }
            finishedInTime = disconnectLatch.await(c.timeoutSeconds, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }

        long end = System.nanoTime();
        ConnectStats stats = new ConnectStats(
                established.get(), peakActive.get(), completed.get(), failed.get(), finishedInTime);
        ClientStressSupport.logConnectStress(label, c, stats, start, end, finishedInTime);
        return stats;
    }

}
