package plus.jmqx.broker;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import plus.jmqx.broker.cluster.ClusterRegistry;
import plus.jmqx.broker.mqtt.MqttConfiguration;
import plus.jmqx.broker.mqtt.context.NamespaceContextHolder;
import plus.jmqx.broker.mqtt.context.ReceiveContext;
import plus.jmqx.broker.mqtt.message.dispatch.PlatformDispatcher;
import plus.jmqx.broker.mqtt.registry.SessionRegistry;
import plus.jmqx.broker.support.MqttKeepaliveClient;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 集群海量设备连接测试
 *
 * <p>验证集群模式双节点在大量 MQTT 连接压力下的正确性。</p>
 *
 * <p>验证方式：客户端侧观测 CONNACK 状态 + 重连验证（同 clientId 重连成功即证明旧会话已清理）。</p>
 *
 * @author maxid
 * @since 2025/4/9 16:30
 */
@Slf4j
@EnabledIfSystemProperty(named = "jmqx.integration.tests", matches = "true")
public class MassiveConnectionTest {

    // ========== 配置参数 ==========

    /**
     * 读取系统属性整数值
     */
    private static int intProp(String key, int def) {
        String value = System.getProperty(key);
        if (value == null || value.isEmpty()) {
            return def;
        }
        return Integer.parseInt(value);
    }

    /**
     * 通过 ReceiveContext 获取会话注册中心
     */
    private static SessionRegistry sessionRegistry(String namespace, String node) {
        return NamespaceContextHolder.get(namespace, node).getContext().getSessionRegistry();
    }

    /**
     * 轮询等待 SessionRegistry 计数达到预期值
     */
    private static void awaitSessionCount(SessionRegistry registry, int expected, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (registry.counts() == expected) {
                return;
            }
            Thread.sleep(100);
        }
        assertEquals(expected, registry.counts(), "SessionRegistry 计数未在超时内达到预期");
    }

    /**
     * 计算连接阶段 latch 等待超时（秒）
     */
    private static long latchTimeoutSeconds(int taskCount, int connectConcurrency, int connectTimeoutSeconds) {
        int configured = intProp("jmqx.test.latchTimeoutSeconds", -1);
        if (configured > 0) {
            return configured;
        }
        int waves = (taskCount + connectConcurrency - 1) / connectConcurrency;
        return Math.max(connectTimeoutSeconds * 5L + 30L, (long) waves * connectTimeoutSeconds + 120L);
    }

    // ========== 集群海量连接测试 ==========

    /**
     * 集群双节点海量连接测试
     *
     * <p>使用场景：验证集群环境下双节点各自承载连接的能力。</p>
     *
     * <p>JVM 参数示例：
     * -Djmqx.integration.tests=true
     * -Djmqx.test.targetConnectionsPerNode=5000
     * -Djmqx.test.connectConcurrency=50
     * -Djmqx.test.holdSeconds=30</p>
     */
    @Test
    void testMassiveConnectionsCluster() throws Exception {
        int targetPerNode = intProp("jmqx.test.targetConnectionsPerNode", 5000);
        int connectConcurrency = intProp("jmqx.test.connectConcurrency", 50);
        int batchIntervalMs = intProp("jmqx.test.batchIntervalMs", 10);
        int holdSeconds = intProp("jmqx.test.holdSeconds", 30);
        int connectTimeoutSeconds = intProp("jmqx.test.connectTimeoutSeconds", 30);
        int keepAliveSeconds = intProp("jmqx.test.keepAliveSeconds", 60);

        log.info("=== 集群海量连接测试开始 ===");
        log.info("targetPerNode={}, totalTarget={}, connectConcurrency={}, holdSeconds={}",
                targetPerNode, targetPerNode * 2, connectConcurrency, holdSeconds);

        String namespace = "cluster-massive-" + UUID.randomUUID().toString().substring(0, 8);
        String node1 = "node-1";
        String node2 = "node-2";
        String clusterUrls = "localhost:7771,localhost:7772";

        Bootstrap bootstrap1 = null;
        Bootstrap bootstrap2 = null;
        try {
            // 1. 启动双节点集群
            bootstrap1 = startClusterNode(namespace, node1, clusterUrls,
                    7771, 1883, null);
            bootstrap2 = startClusterNode(namespace, node2, clusterUrls,
                    7772, 2883, null);
            log.info("cluster nodes started: node-1 mqtt=1883/cluster=7771, node-2 mqtt=2883/cluster=7772");

            // 2. 等待集群形成
            Thread.sleep(3000);
            ReceiveContext<?> context1 = NamespaceContextHolder.get(namespace, node1).getContext();
            ReceiveContext<?> context2 = NamespaceContextHolder.get(namespace, node2).getContext();
            SessionRegistry registry1 = context1.getSessionRegistry();
            SessionRegistry registry2 = context2.getSessionRegistry();
            ClusterRegistry clusterRegistry = context1.getClusterRegistry();
            assertTrue(clusterRegistry.getClusterNode().size() >= 2,
                    "集群应至少包含 2 个节点");
            log.info("cluster formed with {} members, starting connection phase",
                    clusterRegistry.getClusterNode().size());

            // 3. 分批向两节点建立连接（CountDownLatch 等待所有任务完成）
            Semaphore semaphore = new Semaphore(connectConcurrency);
            ExecutorService executor = Executors.newFixedThreadPool(connectConcurrency);
            List<MqttKeepaliveClient> allClients = new CopyOnWriteArrayList<>();
            AtomicLong connectedNode1 = new AtomicLong();
            AtomicLong connectedNode2 = new AtomicLong();
            AtomicLong errorCount = new AtomicLong();
            int totalTasks = targetPerNode * 2;
            CountDownLatch latch = new CountDownLatch(totalTasks);

            long startTime = System.currentTimeMillis();
            int totalPerNode = targetPerNode;
            int totalBatches = (totalPerNode + connectConcurrency - 1) / connectConcurrency;

            for (int batch = 0; batch < totalBatches; batch++) {
                int batchStart = batch * connectConcurrency;
                int batchEnd = Math.min(batchStart + connectConcurrency, totalPerNode);

                // Node-1 连接
                for (int i = batchStart; i < batchEnd; i++) {
                    int idx = i;
                    executor.submit(() -> {
                        if (Thread.interrupted()) { /* 清除残留 */ }
                        try {
                            semaphore.acquire();
                        } catch (InterruptedException e) {
                            errorCount.incrementAndGet();
                            latch.countDown();
                            return;
                        }
                        try {
                            MqttKeepaliveClient client = new MqttKeepaliveClient(
                                    "cluster-n1-" + idx, 1883, keepAliveSeconds);
                            client.connect(connectTimeoutSeconds);
                            connectedNode1.incrementAndGet();
                            allClients.add(client);
                        } catch (Exception e) {
                            errorCount.incrementAndGet();
                            log.error("node-1 connection [cluster-n1-{}] failed", idx, e);
                        } finally {
                            semaphore.release();
                            latch.countDown();
                        }
                    });
                }

                // Node-2 连接
                for (int i = batchStart; i < batchEnd; i++) {
                    int idx = i;
                    executor.submit(() -> {
                        if (Thread.interrupted()) { /* 清除残留 */ }
                        try {
                            semaphore.acquire();
                        } catch (InterruptedException e) {
                            errorCount.incrementAndGet();
                            latch.countDown();
                            return;
                        }
                        try {
                            MqttKeepaliveClient client = new MqttKeepaliveClient(
                                    "cluster-n2-" + idx, 2883, keepAliveSeconds);
                            client.connect(connectTimeoutSeconds);
                            connectedNode2.incrementAndGet();
                            allClients.add(client);
                        } catch (Exception e) {
                            errorCount.incrementAndGet();
                            log.error("node-2 connection [cluster-n2-{}] failed", idx, e);
                        } finally {
                            semaphore.release();
                            latch.countDown();
                        }
                    });
                }

                if (batchIntervalMs > 0 && batch < totalBatches - 1) {
                    Thread.sleep(batchIntervalMs);
                }
                if ((batch + 1) % 5 == 0 || batch == totalBatches - 1) {
                    log.info("connect progress: {}/{} batches, n1={}, n2={}",
                            batch + 1, totalBatches,
                            connectedNode1.get(), connectedNode2.get());
                }
            }

            // 4. 等待所有连接任务完成
            long latchTimeout = latchTimeoutSeconds(totalTasks, connectConcurrency, connectTimeoutSeconds);
            log.info("waiting up to {}s for {} connection tasks", latchTimeout, totalTasks);
            boolean finished = latch.await(latchTimeout, TimeUnit.SECONDS);
            executor.shutdownNow();
            if (!finished) {
                log.warn("latch did not complete in time, {} tasks remaining", latch.getCount());
            }

            long elapsed = System.currentTimeMillis() - startTime;
            log.info("connection phase complete: n1={}, n2={}, errors={}, elapsed={}ms",
                    connectedNode1.get(), connectedNode2.get(), errorCount.get(), elapsed);

            // 5. 验证连接建立与 Broker 端会话计数
            assertEquals(0, errorCount.get(), "不应有连接错误");
            assertEquals(targetPerNode, connectedNode1.get(),
                    "node-1 所有连接均应成功");
            assertEquals(targetPerNode, connectedNode2.get(),
                    "node-2 所有连接均应成功");
            assertEquals(connectedNode1.get(), registry1.counts().intValue(),
                    "node-1 SessionRegistry 计数应与成功连接数一致");
            assertEquals(connectedNode2.get(), registry2.counts().intValue(),
                    "node-2 SessionRegistry 计数应与成功连接数一致");
            // 两节点各自独立托管会话，集群成员列表应包含全部节点
            assertTrue(context2.getClusterRegistry().getClusterNode().size() >= 2,
                    "node-2 视角也应看到完整集群成员");

            // 6. 保持连接
            if (holdSeconds > 0) {
                log.info("holding {} connections for {} seconds...",
                        allClients.size(), holdSeconds);
                Thread.sleep(TimeUnit.SECONDS.toMillis(holdSeconds));
            }

            // 7. 关闭所有客户端连接
            log.info("closing all {} connections...", allClients.size());
            long closeStart = System.currentTimeMillis();
            for (MqttKeepaliveClient client : allClients) {
                try {
                    client.close();
                } catch (Exception e) {
                    // ignore close errors
                }
            }
            long closeElapsed = System.currentTimeMillis() - closeStart;
            log.info("all connections closed in {}ms", closeElapsed);

            // 8. 验证无连接泄漏：SessionRegistry 归零后，用已连接过的 clientId 重连
            awaitSessionCount(registry1, 0, 10_000);
            awaitSessionCount(registry2, 0, 10_000);
            assertTrue(!allClients.isEmpty(), "应有成功建立的连接用于泄漏检测");
            MqttKeepaliveClient probeN1 = allClients.stream()
                    .filter(c -> c.getPort() == 1883)
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("缺少 node-1 连接样本"));
            MqttKeepaliveClient probeN2 = allClients.stream()
                    .filter(c -> c.getPort() == 2883)
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("缺少 node-2 连接样本"));
            MqttKeepaliveClient reconnectN1 = new MqttKeepaliveClient(
                    probeN1.getClientId(), 1883, keepAliveSeconds);
            try {
                reconnectN1.connect(connectTimeoutSeconds);
                log.info("leak check OK: {} reconnected on node-1", probeN1.getClientId());
            } catch (Exception e) {
                throw new AssertionError("node-1 连接泄漏：" + probeN1.getClientId() + " 重连失败", e);
            } finally {
                reconnectN1.close();
            }

            MqttKeepaliveClient reconnectN2 = new MqttKeepaliveClient(
                    probeN2.getClientId(), 2883, keepAliveSeconds);
            try {
                reconnectN2.connect(connectTimeoutSeconds);
                log.info("leak check OK: {} reconnected on node-2", probeN2.getClientId());
            } catch (Exception e) {
                throw new AssertionError("node-2 连接泄漏：" + probeN2.getClientId() + " 重连失败", e);
            } finally {
                reconnectN2.close();
            }

            log.info("=== 集群海量连接测试通过 ===");
        } finally {
            if (bootstrap2 != null) {
                bootstrap2.shutdown();
            }
            if (bootstrap1 != null) {
                bootstrap1.shutdown();
            }
            log.info("cluster nodes shutdown complete");
        }
    }

    // ========== 集群节点启动 ==========

    /**
     * 启动单个集群节点（无 SSL，适用于压测）
     */
    private Bootstrap startClusterNode(String namespace, String node, String urls,
                                       int port, int mqttPort, PlatformDispatcher dispatcher) {
        MqttConfiguration config = new MqttConfiguration();
        config.setPort(mqttPort);
        config.setSslEnable(false);
        config.setSecurePort(-1);
        config.setWebsocketPort(-1);
        config.setWebsocketSecurePort(-1);
        config.getClusterConfig().setEnabled(true);
        config.getClusterConfig().setUrl(urls);
        config.getClusterConfig().setPort(port);
        config.getClusterConfig().setNode(node);
        config.getClusterConfig().setNamespace(namespace);
        try {
            // 避免 null 导致的构造器歧义
            Bootstrap bootstrap = dispatcher != null
                    ? new Bootstrap(config, dispatcher)
                    : new Bootstrap(config);
            bootstrap.start().block();
            return bootstrap;
        } catch (Exception e) {
            throw new RuntimeException("cluster node [" + node + "] start failed", e);
        }
    }
}
