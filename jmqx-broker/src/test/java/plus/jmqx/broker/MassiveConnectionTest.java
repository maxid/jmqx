package plus.jmqx.broker;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import plus.jmqx.broker.mqtt.MqttConfiguration;
import plus.jmqx.broker.mqtt.context.NamespaceContextHolder;
import plus.jmqx.broker.mqtt.context.ReceiveContext;
import plus.jmqx.broker.mqtt.registry.SessionRegistry;
import plus.jmqx.broker.support.MqttKeepaliveClient;
import plus.jmqx.broker.support.MqttKeepaliveClient.ConnectionRejectedException;

import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 海量设备连接测试
 *
 * <p>验证单节点在大量 MQTT 连接压力下的正确性，
 * 包括连接建立、连接准入、连接关闭无泄漏。</p>
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
     * 解析 MQTT 端口（0 表示自动分配空闲端口，避免与残留进程冲突）
     */
    private static int resolvePort(int configured) throws Exception {
        if (configured > 0) {
            return configured;
        }
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    // ========== 单节点海量连接测试 ==========

    /**
     * 单节点海量连接测试
     *
     * <p>使用场景：验证单节点 Broker 在大量连接压力下的稳定性，
     * 以及连接准入机制的正确性。</p>
     *
     * <p>JVM 参数示例：
     * -Djmqx.integration.tests=true
     * -Djmqx.test.targetConnections=10000
     * -Djmqx.test.connectConcurrency=100
     * -Djmqx.test.holdSeconds=60</p>
     */
    @Test
    void testMassiveConnectionsSingleNode() throws Exception {
        int targetConnections = intProp("jmqx.test.targetConnections", 10000);
        int connectConcurrency = intProp("jmqx.test.connectConcurrency", 100);
        int batchIntervalMs = intProp("jmqx.test.batchIntervalMs", 10);
        int holdSeconds = intProp("jmqx.test.holdSeconds", 60);
        int port = resolvePort(intProp("jmqx.test.port", 0));
        int maxConnections = intProp("jmqx.test.maxConnections", 0);
        int connectTimeoutSeconds = intProp("jmqx.test.connectTimeoutSeconds", 30);
        int keepAliveSeconds = intProp("jmqx.test.keepAliveSeconds", 60);

        log.info("=== 单节点海量连接测试开始 ===");
        log.info("targetConnections={}, connectConcurrency={}, maxConnections={}, holdSeconds={}",
                targetConnections, connectConcurrency, maxConnections, holdSeconds);

        // 1. 启动 Broker
        MqttConfiguration config = new MqttConfiguration();
        config.setPort(port);
        config.setSslEnable(false);
        config.setSecurePort(-1);
        config.setWebsocketPort(-1);
        config.setWebsocketSecurePort(-1);
        if (maxConnections > 0) {
            config.setMaxConnections(maxConnections);
        }

        Bootstrap bootstrap = new Bootstrap(config);
        bootstrap.start().block();
        Thread.sleep(500);
        ReceiveContext<?> receiveContext = NamespaceContextHolder.get(
                config.getClusterConfig().getNamespace(),
                config.getClusterConfig().getNode()).getContext();
        assertNotNull(receiveContext, "ReceiveContext 未就绪");
        SessionRegistry sessionRegistry = receiveContext.getSessionRegistry();
        log.info("broker started on port {}", port);
        try {
            // 2. 逐批建立连接（每批完成后再提交下一批，避免压垮 Broker/连接池）
            ExecutorService executor = Executors.newFixedThreadPool(connectConcurrency);
            List<MqttKeepaliveClient> allClients = new CopyOnWriteArrayList<>();
            AtomicLong connectedCount = new AtomicLong();
            AtomicLong rejectedCount = new AtomicLong();
            AtomicLong errorCount = new AtomicLong();

            long startTime = System.currentTimeMillis();
            int totalBatches = (targetConnections + connectConcurrency - 1) / connectConcurrency;
            for (int batch = 0; batch < totalBatches; batch++) {
                int batchStart = batch * connectConcurrency;
                int batchEnd = Math.min(batchStart + connectConcurrency, targetConnections);
                int batchSize = batchEnd - batchStart;
                CountDownLatch batchLatch = new CountDownLatch(batchSize);
                for (int i = batchStart; i < batchEnd; i++) {
                    int idx = i;
                    executor.submit(() -> {
                        if (Thread.interrupted()) {
                            // 清除线程池复用可能残留的 interrupt 标志
                        }
                        try {
                            MqttKeepaliveClient client = new MqttKeepaliveClient(
                                    "massive-" + idx, port, keepAliveSeconds);
                            client.connect(connectTimeoutSeconds);
                            connectedCount.incrementAndGet();
                            allClients.add(client);
                        } catch (ConnectionRejectedException e) {
                            rejectedCount.incrementAndGet();
                            log.debug("connection [massive-{}] rejected: {}", idx, e.getMessage());
                        } catch (Exception e) {
                            errorCount.incrementAndGet();
                            log.error("connection [massive-{}] failed", idx, e);
                        } finally {
                            batchLatch.countDown();
                        }
                    });
                }
                if (!batchLatch.await(connectTimeoutSeconds + 30L, TimeUnit.SECONDS)) {
                    log.warn("batch {}/{} did not finish in time", batch + 1, totalBatches);
                }
                if (batchIntervalMs > 0 && batch < totalBatches - 1) {
                    Thread.sleep(batchIntervalMs);
                }
                if ((batch + 1) % 10 == 0 || batch == totalBatches - 1) {
                    log.info("connect progress: {}/{} batches, {}/{} connected, broker={}",
                            batch + 1, totalBatches, connectedCount.get(), targetConnections,
                            sessionRegistry.counts());
                }
            }

            executor.shutdownNow();

            long elapsed = System.currentTimeMillis() - startTime;
            log.info("connection phase complete: connected={}, rejected={}, errors={}, elapsed={}ms",
                    connectedCount.get(), rejectedCount.get(), errorCount.get(), elapsed);

            // 4. 验证 Broker 端会话计数（短暂轮询等待异步注册完成）
            awaitSessionCount(sessionRegistry, (int) connectedCount.get(), 5_000);

            // 5. 验证连接建立：无错误，连接成功数符合预期
            assertEquals(0, errorCount.get(), "不应有连接错误");
            if (maxConnections > 0 && targetConnections > maxConnections) {
                // 有连接准入限制时，成功数不应超过限制
                assertTrue(connectedCount.get() <= maxConnections,
                        "成功连接数不应超过 maxConnections");
                assertTrue(rejectedCount.get() > 0,
                        "超限连接应被拒绝");
                log.info("connection admission OK: {} accepted, {} rejected (max={})",
                        connectedCount.get(), rejectedCount.get(), maxConnections);
            } else {
                // 无限制时，全部应成功
                assertEquals(targetConnections, connectedCount.get(),
                        "所有连接均应成功建立（无 maxConnections 限制）");
            }

            // 6. 内存监控
            Runtime runtime = Runtime.getRuntime();
            long freeBeforeHold = runtime.freeMemory();
            long totalMemory = runtime.totalMemory();
            log.info("memory before hold: free={}MB, total={}MB",
                    freeBeforeHold / 1024 / 1024, totalMemory / 1024 / 1024);

            // 7. 保持连接
            if (holdSeconds > 0) {
                log.info("holding {} connections for {} seconds...",
                        connectedCount.get(), holdSeconds);
                Thread.sleep(TimeUnit.SECONDS.toMillis(holdSeconds));
            }

            long freeAfterHold = runtime.freeMemory();
            log.info("memory after hold: free={}MB", freeAfterHold / 1024 / 1024);
            assertTrue(freeAfterHold > 0, "保持连接期间不应 OOM");

            // 8. 关闭所有客户端连接
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

            // 9. 验证无连接泄漏：SessionRegistry 归零后，用已连接过的 clientId 重连
            awaitSessionCount(sessionRegistry, 0, 10_000);
            assertTrue(!allClients.isEmpty(), "应有成功建立的连接用于泄漏检测");
            String probeClientId = allClients.get(0).getClientId();
            MqttKeepaliveClient probe = new MqttKeepaliveClient(probeClientId, port, keepAliveSeconds);
            try {
                probe.connect(connectTimeoutSeconds);
                log.info("leak check OK: {} reconnected successfully (old session cleaned)", probeClientId);
            } catch (ConnectionRejectedException e) {
                throw new AssertionError(
                        "连接泄漏：旧会话未清理，" + probeClientId + " 重连被拒绝", e);
            } finally {
                probe.close();
            }

            String probeClientId2 = allClients.get(allClients.size() - 1).getClientId();
            MqttKeepaliveClient probe2 = new MqttKeepaliveClient(probeClientId2, port, keepAliveSeconds);
            try {
                probe2.connect(connectTimeoutSeconds);
                log.info("leak check OK: {} reconnected successfully", probeClientId2);
            } catch (ConnectionRejectedException e) {
                throw new AssertionError(
                        "连接泄漏：旧会话未清理，" + probeClientId2 + " 重连被拒绝", e);
            } finally {
                probe2.close();
            }

            log.info("=== 单节点海量连接测试通过 ===");
        } finally {
            bootstrap.shutdown();
            log.info("broker shutdown complete");
        }
    }
}
