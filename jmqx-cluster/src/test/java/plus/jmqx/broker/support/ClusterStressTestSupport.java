package plus.jmqx.broker.support;

import lombok.extern.slf4j.Slf4j;
import plus.jmqx.broker.Bootstrap;
import plus.jmqx.broker.mqtt.MqttConfiguration;
import plus.jmqx.broker.mqtt.message.dispatch.PlatformDispatcher;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 集群消息压测工具
 */
@Slf4j
public final class ClusterStressTestSupport {

    private static final Semaphore CONNECT_SEMAPHORE = new Semaphore(
            Integer.parseInt(System.getProperty("jmqx.stress.connectLimit", "50")));

    private ClusterStressTestSupport() {
    }

    public static Bootstrap startClusterNode(String namespace, String node, String urls,
                                             int clusterPort, int mqttPort,
                                             PlatformDispatcher dispatcher) {
        MqttConfiguration config = new MqttConfiguration();
        config.setPort(mqttPort);
        config.setSslEnable(false);
        config.setSecurePort(-1);
        config.setWebsocketPort(-1);
        config.setWebsocketSecurePort(-1);
        config.getClusterConfig().setEnabled(true);
        config.getClusterConfig().setUrl(urls);
        config.getClusterConfig().setPort(clusterPort);
        config.getClusterConfig().setNode(node);
        config.getClusterConfig().setNamespace(namespace);
        try {
            Bootstrap bootstrap = new Bootstrap(config, dispatcher);
            bootstrap.start().block();
            return bootstrap;
        } catch (Exception e) {
            throw new RuntimeException("cluster node [" + node + "] start failed", e);
        }
    }

    public static void runClusterStress(StressConfig stress, String namespace, String clusterUrls)
            throws InterruptedException {
        AtomicLong dispatchReceived = new AtomicLong();
        PlatformDispatcher dispatcher = StressTestSupport.countingDispatcher(dispatchReceived);

        Bootstrap bootstrap1 = startClusterNode(namespace, "node-1", clusterUrls, 7771,
                stress.port, dispatcher);
        log.info("cluster node-1 started on MQTT port {}", stress.port);

        Bootstrap bootstrap2 = startClusterNode(namespace, "node-2", clusterUrls, 7772,
                stress.port + 1000, dispatcher);
        log.info("cluster node-2 started on MQTT port {}", stress.port + 1000);

        Thread.sleep(3000);
        log.info("cluster stress: cluster formed, preflight start");

        try {
            int totalDevices = Math.max(2, stress.threads);
            int devicesPerNode = totalDevices / 2;
            CountDownLatch latch = new CountDownLatch(totalDevices);
            ExecutorService executor = Executors.newFixedThreadPool(totalDevices);

            AtomicLong published = new AtomicLong();
            AtomicLong acked = new AtomicLong();
            ScheduledExecutorService reporter = Executors.newSingleThreadScheduledExecutor();
            long start = System.nanoTime();
            AtomicLong lastAcked = new AtomicLong();
            AtomicLong lastReportAt = new AtomicLong(start);
            List<MqttStressClient> allClients = new ArrayList<>();

            reporter.scheduleAtFixedRate(
                    () -> StressTestSupport.logProgress("cluster", stress, published, acked,
                            dispatchReceived, lastAcked, lastReportAt, start),
                    stress.reportIntervalSeconds,
                    stress.reportIntervalSeconds,
                    TimeUnit.SECONDS);

            if (!StressTestSupport.preflightPublish(stress, stress.port, acked, dispatchReceived)) {
                reporter.shutdownNow();
                executor.shutdownNow();
                StressResult failed = new StressResult(acked.get(), start, System.nanoTime(), false);
                StressTestSupport.logStressResult("cluster", stress, failed, dispatchReceived.get());
                return;
            }

            for (int i = 0; i < devicesPerNode; i++) {
                int idx = i;
                executor.submit(() -> runStressClient("stress-n1-" + idx, stress.port,
                        stress, acked, published, allClients, latch));
            }
            for (int i = 0; i < devicesPerNode; i++) {
                int idx = i;
                executor.submit(() -> runStressClient("stress-n2-" + idx, stress.port + 1000,
                        stress, acked, published, allClients, latch));
            }

            boolean ok = latch.await(stress.timeoutSeconds, TimeUnit.SECONDS);
            long end = System.nanoTime();
            executor.shutdownNow();
            reporter.shutdownNow();

            for (MqttStressClient client : allClients) {
                try {
                    client.close();
                } catch (Exception ignored) {
                }
            }

            StressResult result = new StressResult(acked.get(), start, end, ok);
            StressTestSupport.logStressResult("cluster", stress, result, dispatchReceived.get());
        } finally {
            bootstrap2.shutdown();
            bootstrap1.shutdown();
        }
    }

    private static void runStressClient(String clientId, int port, StressConfig config,
                                        AtomicLong acked, AtomicLong published,
                                        List<MqttStressClient> allClients, CountDownLatch latch) {
        MqttStressClient client = new MqttStressClient(clientId, port, acked);
        try {
            CONNECT_SEMAPHORE.acquire();
            try {
                client.connect();
            } finally {
                CONNECT_SEMAPHORE.release();
            }
            synchronized (allClients) {
                allClients.add(client);
            }
            long sentCount = client.publishLoop(config.topic, config.payloadBytes,
                    config.durationSeconds, config.flushEvery, config.inFlightLimit, published);
            client.awaitAcks(sentCount, config.timeoutSeconds);
        } catch (Exception e) {
            log.error("stress client [{}] error", clientId, e);
        } finally {
            latch.countDown();
        }
    }
}
