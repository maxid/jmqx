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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 集群消息压测工具
 *
 * @author maxid
 * @since 2026/6/27
 */
@Slf4j
public final class ClusterStressTestSupport {

    private ClusterStressTestSupport() {
    }

    public static Bootstrap startClusterNode(String namespace, String node, String urls,
                                             int clusterPort, int mqttPort,
                                             PlatformDispatcher dispatcher) {
        MqttConfiguration config = new MqttConfiguration();
        config.setBusinessQueueSize(Integer.MAX_VALUE);
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

    public static void runClusterStress(StressConfig stress, String namespace)
            throws Exception {
        int mqttPort1 = StressTestSupport.resolvePort(stress.port);
        int mqttPort2 = StressTestSupport.resolvePort(mqttPort1 + 1000);
        int clusterPort1 = StressTestSupport.resolvePort(
                StressTestSupport.intProp("jmqx.stress.clusterPort1", 7771));
        int clusterPort2 = StressTestSupport.resolvePort(
                StressTestSupport.intProp("jmqx.stress.clusterPort2", 7772));
        String clusterUrls = "127.0.0.1:" + clusterPort1 + ",127.0.0.1:" + clusterPort2;
        stress.port = mqttPort1;

        AtomicLong dispatchReceived = new AtomicLong();
        PlatformDispatcher dispatcher = StressTestSupport.countingDispatcher(dispatchReceived);

        Bootstrap bootstrap1 = startClusterNode(namespace, "node-1", clusterUrls, clusterPort1,
                mqttPort1, dispatcher);
        log.info("cluster node-1 started on MQTT port {} (cluster {})", mqttPort1, clusterPort1);

        Bootstrap bootstrap2 = startClusterNode(namespace, "node-2", clusterUrls, clusterPort2,
                mqttPort2, dispatcher);
        log.info("cluster node-2 started on MQTT port {} (cluster {})", mqttPort2, clusterPort2);

        Thread.sleep(3000);
        log.info("cluster stress: cluster formed, preflight start");

        try {
            int totalDevices = Math.max(2, stress.threads);
            int devicesPerNode = totalDevices / 2;
            int connectConcurrency = StressTestSupport.intProp("jmqx.stress.connectConcurrency", 50);
            int connectTimeoutSeconds = StressTestSupport.intProp("jmqx.stress.connectTimeoutSeconds", 30);
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

            if (!StressTestSupport.preflightPublish(stress, mqttPort1, acked, dispatchReceived)) {
                reporter.shutdownNow();
                executor.shutdownNow();
                StressResult failed = new StressResult(acked.get(), start, System.nanoTime(), false);
                StressTestSupport.logStressResult("cluster", stress, failed, dispatchReceived.get());
                return;
            }

            List<StressClientTask> tasks = new ArrayList<>(totalDevices);
            for (int i = 0; i < devicesPerNode; i++) {
                tasks.add(new StressClientTask("stress-n1-" + i, mqttPort1));
            }
            for (int i = 0; i < devicesPerNode; i++) {
                tasks.add(new StressClientTask("stress-n2-" + i, mqttPort2));
            }
            connectClientsInBatches(tasks, allClients, acked, connectConcurrency, connectTimeoutSeconds);

            for (StressClientTask task : tasks) {
                if (task.client == null) {
                    latch.countDown();
                    continue;
                }
                executor.submit(() -> runPublishPhase(task, stress, acked, published, latch));
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

    private static void connectClientsInBatches(List<StressClientTask> tasks,
                                                List<MqttStressClient> allClients,
                                                AtomicLong acked,
                                                int connectConcurrency,
                                                int connectTimeoutSeconds)
            throws InterruptedException {
        ExecutorService connectExecutor = Executors.newFixedThreadPool(connectConcurrency);
        int totalBatches = (tasks.size() + connectConcurrency - 1) / connectConcurrency;
        for (int batch = 0; batch < totalBatches; batch++) {
            int batchStart = batch * connectConcurrency;
            int batchEnd = Math.min(batchStart + connectConcurrency, tasks.size());
            CountDownLatch batchLatch = new CountDownLatch(batchEnd - batchStart);
            for (int i = batchStart; i < batchEnd; i++) {
                StressClientTask task = tasks.get(i);
                connectExecutor.submit(() -> {
                    try {
                        MqttStressClient client = new MqttStressClient(task.clientId, task.port, acked);
                        client.connect(connectTimeoutSeconds);
                        task.client = client;
                        synchronized (allClients) {
                            allClients.add(client);
                        }
                    } catch (Exception e) {
                        log.error("stress client [{}] connect failed", task.clientId, e);
                    } finally {
                        batchLatch.countDown();
                    }
                });
            }
            if (!batchLatch.await(connectTimeoutSeconds + 30L, TimeUnit.SECONDS)) {
                log.warn("connect batch {}/{} did not finish in time", batch + 1, totalBatches);
            }
        }
        connectExecutor.shutdownNow();
        log.info("cluster stress: {}/{} clients connected", allClients.size(), tasks.size());
    }

    private static void runPublishPhase(StressClientTask task, StressConfig config,
                                        AtomicLong acked, AtomicLong published,
                                        CountDownLatch latch) {
        try {
            long sentCount = task.client.publishLoop(config.topic, config.payloadBytes,
                    config.durationSeconds, config.flushEvery, config.inFlightLimit, published);
            task.client.awaitAcks(sentCount, config.timeoutSeconds);
        } catch (Exception e) {
            log.error("stress client [{}] publish error", task.clientId, e);
        } finally {
            latch.countDown();
        }
    }

    private static final class StressClientTask {
        private final String           clientId;
        private final int              port;
        private       MqttStressClient client;

        private StressClientTask(String clientId, int port) {
            this.clientId = clientId;
            this.port = port;
        }
    }

}
