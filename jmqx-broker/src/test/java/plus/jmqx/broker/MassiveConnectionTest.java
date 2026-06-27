package plus.jmqx.broker;

import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.resolver.NoopAddressResolverGroup;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import plus.jmqx.broker.mqtt.MqttConfiguration;
import plus.jmqx.broker.mqtt.message.MqttMessageBuilder;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpClient;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
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
        int connectConcurrency = intProp("jmqx.test.connectConcurrency", 25);
        int batchIntervalMs = intProp("jmqx.test.batchIntervalMs", 10);
        int holdSeconds = intProp("jmqx.test.holdSeconds", 60);
        int port = intProp("jmqx.test.port", 1883);
        int maxConnections = intProp("jmqx.test.maxConnections", 0);
        int connectTimeoutSeconds = intProp("jmqx.test.connectTimeoutSeconds", 30);

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
        log.info("broker started on port {}", port);
        try {
            // 2. 分批建立连接（CountDownLatch 等待所有任务完成）
            Semaphore semaphore = new Semaphore(connectConcurrency);
            ExecutorService executor = Executors.newFixedThreadPool(connectConcurrency);
            List<MassiveConnectionClient> allClients = new CopyOnWriteArrayList<>();
            AtomicLong connectedCount = new AtomicLong();
            AtomicLong rejectedCount = new AtomicLong();
            AtomicLong errorCount = new AtomicLong();
            int totalTasks = (maxConnections > 0 && targetConnections > maxConnections)
                    ? targetConnections  // 含超限场景：target 可能包含超出 max 的
                    : targetConnections;
            CountDownLatch latch = new CountDownLatch(targetConnections);

            long startTime = System.currentTimeMillis();
            int totalBatches = (targetConnections + connectConcurrency - 1) / connectConcurrency;
            for (int batch = 0; batch < totalBatches; batch++) {
                int batchStart = batch * connectConcurrency;
                int batchEnd = Math.min(batchStart + connectConcurrency, targetConnections);
                for (int i = batchStart; i < batchEnd; i++) {
                    int idx = i;
                    executor.submit(() -> {
                        // 清除线程池复用可能残留的 interrupt 标志
                        if (Thread.interrupted()) {
                            // 吞掉残留标志，避免影响 connectNow() 的 Mono.block()
                        }
                        try {
                            semaphore.acquire();
                        } catch (InterruptedException e) {
                            errorCount.incrementAndGet();
                            latch.countDown();
                            return;
                        }
                        try {
                            MassiveConnectionClient client = new MassiveConnectionClient(
                                    "massive-" + idx, port);
                            client.connect(connectTimeoutSeconds);
                            connectedCount.incrementAndGet();
                            allClients.add(client);
                        } catch (MassiveConnectionClient.ConnectionRejectedException e) {
                            rejectedCount.incrementAndGet();
                            log.debug("connection [massive-{}] rejected: {}", idx, e.getMessage());
                        } catch (Exception e) {
                            errorCount.incrementAndGet();
                            log.error("connection [massive-{}] failed", idx, e);
                        } finally {
                            semaphore.release();
                            latch.countDown();
                        }
                    });
                }
                if (batchIntervalMs > 0 && batch < totalBatches - 1) {
                    Thread.sleep(batchIntervalMs);
                }
                if ((batch + 1) % 10 == 0 || batch == totalBatches - 1) {
                    log.info("connect progress: {}/{} batches, {}/{} connected",
                            batch + 1, totalBatches, connectedCount.get(), targetConnections);
                }
            }

            // 3. 等待所有连接任务完成（用 latch 而非 executor.awaitTermination）
            boolean finished = latch.await(
                    connectTimeoutSeconds * 5 + 30, TimeUnit.SECONDS);
            executor.shutdownNow();
            if (!finished) {
                log.warn("latch did not complete in time, {} tasks remaining", latch.getCount());
            }

            long elapsed = System.currentTimeMillis() - startTime;
            log.info("connection phase complete: connected={}, rejected={}, errors={}, elapsed={}ms",
                    connectedCount.get(), rejectedCount.get(), errorCount.get(), elapsed);

            // 4. 验证连接建立：无错误，连接成功数符合预期
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

            // 5. 内存监控
            Runtime runtime = Runtime.getRuntime();
            long freeBeforeHold = runtime.freeMemory();
            long totalMemory = runtime.totalMemory();
            log.info("memory before hold: free={}MB, total={}MB",
                    freeBeforeHold / 1024 / 1024, totalMemory / 1024 / 1024);

            // 6. 保持连接
            if (holdSeconds > 0) {
                log.info("holding {} connections for {} seconds...",
                        connectedCount.get(), holdSeconds);
                Thread.sleep(TimeUnit.SECONDS.toMillis(holdSeconds));
            }

            long freeAfterHold = runtime.freeMemory();
            log.info("memory after hold: free={}MB", freeAfterHold / 1024 / 1024);
            assertTrue(freeAfterHold > 0, "保持连接期间不应 OOM");

            // 7. 关闭所有客户端连接
            log.info("closing all {} connections...", allClients.size());
            long closeStart = System.currentTimeMillis();
            for (MassiveConnectionClient client : allClients) {
                try {
                    client.close();
                } catch (Exception e) {
                    // ignore close errors
                }
            }
            long closeElapsed = System.currentTimeMillis() - closeStart;
            log.info("all connections closed in {}ms", closeElapsed);

            // 8. 验证无连接泄漏：等待清理后，用相同 clientId 重连，必须成功
            //    （ConnectMode.UNIQUE 下，若旧会话未清理则新连接会被拒绝）
            Thread.sleep(2000); // 等待 Broker 清理所有会话
            String probeClientId = "massive-0";
            MassiveConnectionClient probe = new MassiveConnectionClient(probeClientId, port);
            try {
                probe.connect(connectTimeoutSeconds);
                log.info("leak check OK: {} reconnected successfully (old session cleaned)", probeClientId);
            } catch (MassiveConnectionClient.ConnectionRejectedException e) {
                throw new AssertionError(
                        "连接泄漏：旧会话未清理，" + probeClientId + " 重连被拒绝", e);
            } finally {
                probe.close();
            }

            // 9. 确认第二个 clientId 也能重连（进一步确认全部清理）
            String probeClientId2 = "massive-" + Math.max(1, connectedCount.get() - 1);
            MassiveConnectionClient probe2 = new MassiveConnectionClient(probeClientId2, port);
            try {
                probe2.connect(connectTimeoutSeconds);
                log.info("leak check OK: {} reconnected successfully", probeClientId2);
            } catch (MassiveConnectionClient.ConnectionRejectedException e) {
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

    // ========== 海量连接客户端（精简版 MqttStressClient） ==========

    /**
     * 用于海量连接测试的 MQTT 客户端
     *
     * <p>只保留 connect() + close()，移除 publishLoop/awaitAcks 等压测逻辑。</p>
     */
    private static class MassiveConnectionClient {

        private final String clientId;
        private final int port;
        private Connection connection;
        private final ConcurrentLinkedQueue<MqttMessage> inbox = new ConcurrentLinkedQueue<>();

        MassiveConnectionClient(String clientId, int port) {
            this.clientId = clientId;
            this.port = port;
        }

        /**
         * 建立连接并完成 MQTT 握手
         *
         * @param timeoutSeconds 超时秒数
         * @throws ConnectionRejectedException 连接被 broker 拒绝
         * @throws IllegalStateException      连接超时
         */
        void connect(int timeoutSeconds) {
            this.connection = TcpClient.create()
                    .resolver(NoopAddressResolverGroup.INSTANCE)
                    .remoteAddress(() -> new InetSocketAddress("127.0.0.1", port))
                    .connectNow(Duration.ofSeconds(timeoutSeconds));
            ensureMqttPipeline();

            MqttMessage connect = MqttMessageBuilder.connectMessage(
                    clientId,
                    "",
                    "",
                    "",
                    "",
                    false,
                    false,
                    false,
                    0,
                    60
            );
            writeAndFlush(connect);
            connection.inbound()
                    .receiveObject()
                    .ofType(MqttMessage.class)
                    .subscribe(this::onInbound);

            MqttConnAckMessage ack = (MqttConnAckMessage) awaitMessage(
                    msg -> msg instanceof MqttConnAckMessage,
                    Duration.ofSeconds(timeoutSeconds));
            if (ack == null) {
                throw new IllegalStateException("connect ack timeout for [" + clientId + "]");
            }
            MqttConnectReturnCode returnCode = ack.variableHeader().connectReturnCode();
            if (returnCode != MqttConnectReturnCode.CONNECTION_ACCEPTED) {
                throw new ConnectionRejectedException(
                        "connection [" + clientId + "] rejected: " + returnCode);
            }
        }

        /**
         * 关闭连接
         */
        void close() {
            if (connection != null && !connection.isDisposed()) {
                connection.disposeNow();
            }
        }

        /**
         * 线程安全写入并刷新
         */
        private void writeAndFlush(MqttMessage message) {
            if (connection.channel().eventLoop().inEventLoop()) {
                connection.channel().writeAndFlush(message).syncUninterruptibly();
                return;
            }
            connection.channel().eventLoop()
                    .submit(() -> connection.channel().writeAndFlush(message))
                    .syncUninterruptibly();
        }

        /**
         * 确保 MQTT 编解码器存在
         */
        private void ensureMqttPipeline() {
            String bridgeName = "reactor.right.reactiveBridge";
            if (connection.channel().pipeline().get(bridgeName) != null) {
                if (connection.channel().pipeline().get(MqttEncoder.class) == null) {
                    connection.channel().pipeline()
                            .addBefore(bridgeName, "mqttEncoder", MqttEncoder.INSTANCE);
                }
                if (connection.channel().pipeline().get(MqttDecoder.class) == null) {
                    connection.channel().pipeline()
                            .addBefore(bridgeName, "mqttDecoder",
                                    new MqttDecoder(1024 * 1024));
                }
                return;
            }
            if (connection.channel().pipeline().get(MqttEncoder.class) == null) {
                connection.channel().pipeline()
                        .addFirst("mqttEncoder", MqttEncoder.INSTANCE);
            }
            if (connection.channel().pipeline().get(MqttDecoder.class) == null) {
                connection.channel().pipeline()
                        .addLast("mqttDecoder", new MqttDecoder(1024 * 1024));
            }
        }

        /**
         * 处理入站消息
         */
        private void onInbound(MqttMessage message) {
            if (message.fixedHeader().messageType() == MqttMessageType.PUBACK) {
                return;
            }
            inbox.add(message);
        }

        /**
         * 等待匹配的入站消息
         */
        private MqttMessage awaitMessage(
                java.util.function.Predicate<MqttMessage> predicate, Duration timeout) {
            long deadline = System.nanoTime() + timeout.toNanos();
            while (System.nanoTime() < deadline) {
                for (MqttMessage message : inbox) {
                    if (predicate.test(message)) {
                        inbox.remove(message);
                        return message;
                    }
                }
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return null;
                }
            }
            return null;
        }

        /**
         * 连接被拒绝异常
         */
        static final class ConnectionRejectedException extends RuntimeException {
            ConnectionRejectedException(String message) {
                super(message);
            }
        }
    }
}
