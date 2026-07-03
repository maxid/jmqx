package plus.jmqx.client.mqtt.stress;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * 压测期间按固定间隔打印进度（默认每 5 秒）。
 */
@Slf4j
public final class StressProgressReporter implements AutoCloseable {

    private final ScheduledExecutorService executor;
    private final ScheduledFuture<?>       future;

    private StressProgressReporter(ScheduledExecutorService executor, ScheduledFuture<?> future) {
        this.executor = executor;
        this.future = future;
    }

    public static StressProgressReporter start(String label, int intervalSeconds, long startNanos,
                                               Supplier<String> metrics) {
        if (intervalSeconds <= 0) {
            return new StressProgressReporter(null, null);
        }
        ScheduledExecutorService ex = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "stress-progress-" + label);
            t.setDaemon(true);
            return t;
        });
        ScheduledFuture<?> task = ex.scheduleAtFixedRate(() -> {
            double elapsedSec = Math.max((System.nanoTime() - startNanos) / 1_000_000_000.0, 0.001);
            log.info("{} progress [elapsed={}s]: {}",
                    label,
                    String.format("%.1f", elapsedSec),
                    metrics.get());
        }, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
        return new StressProgressReporter(ex, task);
    }

    @Override
    public void close() {
        if (future != null) {
            future.cancel(false);
        }
        if (executor != null) {
            executor.shutdownNow();
        }
    }

}
