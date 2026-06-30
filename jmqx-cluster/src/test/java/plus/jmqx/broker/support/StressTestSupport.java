package plus.jmqx.broker.support;

import lombok.extern.slf4j.Slf4j;
import plus.jmqx.broker.mqtt.MqttConfiguration;
import plus.jmqx.broker.mqtt.message.dispatch.ConnectMessage;
import plus.jmqx.broker.mqtt.message.dispatch.ConnectionLostMessage;
import plus.jmqx.broker.mqtt.message.dispatch.DisconnectMessage;
import plus.jmqx.broker.mqtt.message.dispatch.PlatformDispatcher;
import plus.jmqx.broker.mqtt.message.dispatch.PublishMessage;
import plus.jmqx.broker.util.PortUtil;
import reactor.core.publisher.Mono;

import java.net.ServerSocket;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * MQTT 消息压测公共工具
 *
 * @author maxid
 * @since 2026/6/27
 */
@Slf4j
public final class StressTestSupport {

    private StressTestSupport() {
    }

    public static int intProp(String key, int def) {
        String value = System.getProperty(key);
        if (value == null || value.isEmpty()) {
            return def;
        }
        return Integer.parseInt(value);
    }

    /**
     * 解析可用端口：{@code configured > 0} 时从该端口起找空闲位，{@code 0} 时由 OS 分配。
     */
    public static int resolvePort(int configured) throws Exception {
        if (configured > 0) {
            return PortUtil.getAvailablePort(configured);
        }
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    public static StressConfig loadStressConfig() {
        StressConfig config = new StressConfig();
        config.port = intProp("jmqx.stress.port", 1883);
        config.threads = intProp("jmqx.stress.threads", 4);
        config.durationSeconds = intProp("jmqx.stress.durationSeconds", 600);
        config.payloadBytes = intProp("jmqx.stress.payloadBytes", 64);
        config.flushEvery = intProp("jmqx.stress.flushEvery", 256);
        config.inFlightLimit = intProp("jmqx.stress.inFlightLimit", 20000);
        config.timeoutSeconds = intProp("jmqx.stress.timeoutSeconds", 720);
        config.reportIntervalSeconds = intProp("jmqx.stress.reportIntervalSeconds", 10);
        config.topic = "stress/topic";
        return config;
    }

    public static MqttConfiguration brokerStressConfig(int mqttPort) {
        MqttConfiguration config = new MqttConfiguration();
        config.setBusinessQueueSize(Integer.MAX_VALUE);
        config.setSslEnable(false);
        config.setPort(mqttPort);
        config.setSecurePort(-1);
        config.setWebsocketPort(-1);
        config.setWebsocketSecurePort(-1);
        config.getClusterConfig().setNamespace("jmqx-stress-" + UUID.randomUUID());
        return config;
    }

    public static PlatformDispatcher countingDispatcher(AtomicLong dispatchReceived) {
        return new PlatformDispatcher() {
            @Override
            public Mono<Void> onConnect(ConnectMessage message) {
                return Mono.empty();
            }

            @Override
            public Mono<Void> onDisconnect(DisconnectMessage message) {
                return Mono.empty();
            }

            @Override
            public Mono<Void> onConnectionLost(ConnectionLostMessage message) {
                return Mono.empty();
            }

            @Override
            public Mono<Void> onPublish(PublishMessage message) {
                return Mono.fromRunnable(dispatchReceived::incrementAndGet);
            }
        };
    }

    public static StressResult runBrokerStress(StressConfig config, AtomicLong dispatchReceived)
            throws InterruptedException {
        AtomicLong published = new AtomicLong();
        AtomicLong acked = new AtomicLong();
        CountDownLatch latch = new CountDownLatch(config.threads);
        ExecutorService executor = Executors.newFixedThreadPool(config.threads);
        ScheduledExecutorService reporter = Executors.newSingleThreadScheduledExecutor();
        long start = System.nanoTime();
        AtomicLong lastAcked = new AtomicLong();
        AtomicLong lastReportAt = new AtomicLong(start);
        reporter.scheduleAtFixedRate(
                () -> logProgress("broker", config, published, acked, dispatchReceived,
                        lastAcked, lastReportAt, start),
                config.reportIntervalSeconds,
                config.reportIntervalSeconds,
                TimeUnit.SECONDS);
        if (!preflightPublish(config, config.port, acked, dispatchReceived)) {
            reporter.shutdownNow();
            executor.shutdownNow();
            return new StressResult(acked.get(), start, System.nanoTime(), false);
        }

        for (int i = 0; i < config.threads; i++) {
            final String clientId = "stress-" + i;
            executor.submit(() -> {
                try {
                    MqttStressClient client = new MqttStressClient(clientId, config.port, acked);
                    client.connect();
                    long sentCount = client.publishLoop(config.topic, config.payloadBytes,
                            config.durationSeconds, config.flushEvery, config.inFlightLimit, published);
                    client.awaitAcks(sentCount, config.timeoutSeconds);
                    client.close();
                } catch (Exception e) {
                    log.error("stress client error", e);
                } finally {
                    latch.countDown();
                }
            });
        }

        boolean ok = latch.await(config.timeoutSeconds, TimeUnit.SECONDS);
        long end = System.nanoTime();
        executor.shutdownNow();
        reporter.shutdownNow();
        return new StressResult(acked.get(), start, end, ok);
    }

    public static boolean preflightPublish(StressConfig config, int port,
                                           AtomicLong acked, AtomicLong dispatchReceived) {
        MqttStressClient client = new MqttStressClient("stress-preflight", port, acked);
        try {
            client.connect();
            long beforeDispatch = dispatchReceived.get();
            boolean ok = client.publishOnceWaitAck(config.topic, config.payloadBytes, 5);
            if (!ok) {
                log.error("preflight publish did not receive PUBACK");
                return false;
            }
            if (!waitForDispatch(dispatchReceived, beforeDispatch, 5)) {
                log.error("preflight publish did not reach dispatcher");
                return false;
            }
            return true;
        } catch (Exception e) {
            log.error("preflight publish failed", e);
            return false;
        } finally {
            client.close();
        }
    }

    public static boolean waitForDispatch(AtomicLong dispatchReceived, long beforeDispatch, int timeoutSeconds) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        while (System.nanoTime() < deadline) {
            if (dispatchReceived.get() > beforeDispatch) {
                return true;
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    public static void logProgress(String label,
                                   StressConfig config,
                                   AtomicLong published,
                                   AtomicLong acked,
                                   AtomicLong dispatchReceived,
                                   AtomicLong lastAcked,
                                   AtomicLong lastReportAt,
                                   long startNanos) {
        long now = System.nanoTime();
        long totalAcked = acked.get();
        long totalPublished = published.get();
        long totalDispatch = dispatchReceived.get();
        long lastTotal = lastAcked.getAndSet(totalAcked);
        long lastAt = lastReportAt.getAndSet(now);
        double intervalSeconds = Math.max((now - lastAt) / 1_000_000_000.0, 0.001);
        double intervalThroughput = (totalAcked - lastTotal) / intervalSeconds;
        double elapsedSeconds = Math.max((now - startNanos) / 1_000_000_000.0, 0.001);
        log.info("{} stress progress: threads={}, published={}, acked={}, dispatchReceived={}, intervalThroughput={} msg/s, elapsed={}s",
                label,
                config.threads,
                totalPublished,
                totalAcked,
                totalDispatch,
                String.format("%.0f", intervalThroughput),
                String.format("%.1f", elapsedSeconds));
    }

    public static void logStressResult(String label, StressConfig config, StressResult result, long dispatchReceived) {
        double seconds = (result.endNanos - result.startNanos) / 1_000_000_000.0;
        double throughput = result.sent / Math.max(seconds, 0.001);
        log.info("{} stress result: threads={}, durationSeconds={}, payloadBytes={}, acked={}, dispatchReceived={}, time={}s, throughput={} msg/s, completed={}",
                label,
                config.threads,
                config.durationSeconds,
                config.payloadBytes,
                result.sent,
                dispatchReceived,
                String.format("%.3f", seconds),
                String.format("%.0f", throughput),
                result.completed);
    }
}
