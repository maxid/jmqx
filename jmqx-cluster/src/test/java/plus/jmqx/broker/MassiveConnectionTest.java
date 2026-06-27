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
import plus.jmqx.broker.mqtt.message.dispatch.PlatformDispatcher;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpClient;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
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

        log.info("=== 集群海量连接测试开始 ===");
        log.info("targetPerNode={}, totalTarget={}, connectConcurrency={}, holdSeconds={}",
                targetPerNode, targetPerNode * 2, connectConcurrency, holdSeconds);

        String namespace = "cluster-massive-" + UUID.randomUUID().toString().substring(0, 8);
        String clusterUrls = "localhost:7771,localhost:7772";

        Bootstrap bootstrap1 = null;
        Bootstrap bootstrap2 = null;
        try {
            // 1. 启动双节点集群
            bootstrap1 = startClusterNode(namespace, "node-1", clusterUrls,
                    7771, 1883, null);
            bootstrap2 = startClusterNode(namespace, "node-2", clusterUrls,
                    7772, 2883, null);
            log.info("cluster nodes started: node-1 mqtt=1883/cluster=7771, node-2 mqtt=2883/cluster=7772");

            // 2. 等待集群形成
            Thread.sleep(3000);
            log.info("cluster formed, starting connection phase");

            // 3. 分批向两节点建立连接（CountDownLatch 等待所有任务完成）
            Semaphore semaphore = new Semaphore(connectConcurrency);
            ExecutorService executor = Executors.newFixedThreadPool(connectConcurrency);
            List<ClusterConnectionClient> allClients = new CopyOnWriteArrayList<>();
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
                            ClusterConnectionClient client = new ClusterConnectionClient(
                                    "cluster-n1-" + idx, 1883);
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
                            ClusterConnectionClient client = new ClusterConnectionClient(
                                    "cluster-n2-" + idx, 2883);
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
            boolean finished = latch.await(
                    connectTimeoutSeconds * 5 + 60, TimeUnit.SECONDS);
            executor.shutdownNow();
            if (!finished) {
                log.warn("latch did not complete in time, {} tasks remaining", latch.getCount());
            }

            long elapsed = System.currentTimeMillis() - startTime;
            log.info("connection phase complete: n1={}, n2={}, errors={}, elapsed={}ms",
                    connectedNode1.get(), connectedNode2.get(), errorCount.get(), elapsed);

            // 5. 验证连接建立
            assertEquals(0, errorCount.get(), "不应有连接错误");
            assertEquals(targetPerNode, connectedNode1.get(),
                    "node-1 所有连接均应成功");
            assertEquals(targetPerNode, connectedNode2.get(),
                    "node-2 所有连接均应成功");

            // 6. 保持连接
            if (holdSeconds > 0) {
                log.info("holding {} connections for {} seconds...",
                        allClients.size(), holdSeconds);
                Thread.sleep(TimeUnit.SECONDS.toMillis(holdSeconds));
            }

            // 7. 关闭所有客户端连接
            log.info("closing all {} connections...", allClients.size());
            long closeStart = System.currentTimeMillis();
            for (ClusterConnectionClient client : allClients) {
                try {
                    client.close();
                } catch (Exception e) {
                    // ignore close errors
                }
            }
            long closeElapsed = System.currentTimeMillis() - closeStart;
            log.info("all connections closed in {}ms", closeElapsed);

            // 8. 验证无连接泄漏：用相同 clientId 在两节点上重连，必须成功
            Thread.sleep(2000);
            ClusterConnectionClient probeN1 = new ClusterConnectionClient("cluster-n1-0", 1883);
            try {
                probeN1.connect(connectTimeoutSeconds);
                log.info("leak check OK: cluster-n1-0 reconnected on node-1");
            } catch (Exception e) {
                throw new AssertionError("node-1 连接泄漏：cluster-n1-0 重连失败", e);
            } finally {
                probeN1.close();
            }

            ClusterConnectionClient probeN2 = new ClusterConnectionClient("cluster-n2-0", 2883);
            try {
                probeN2.connect(connectTimeoutSeconds);
                log.info("leak check OK: cluster-n2-0 reconnected on node-2");
            } catch (Exception e) {
                throw new AssertionError("node-2 连接泄漏：cluster-n2-0 重连失败", e);
            } finally {
                probeN2.close();
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

    // ========== 连接客户端 ==========

    /**
     * 集群连接测试客户端
     */
    private static class ClusterConnectionClient {

        private final String clientId;
        private final int port;
        private Connection connection;
        private final ConcurrentLinkedQueue<MqttMessage> inbox = new ConcurrentLinkedQueue<>();

        ClusterConnectionClient(String clientId, int port) {
            this.clientId = clientId;
            this.port = port;
        }

        void connect(int timeoutSeconds) {
            this.connection = TcpClient.create()
                    .resolver(NoopAddressResolverGroup.INSTANCE)
                    .remoteAddress(() -> new InetSocketAddress("127.0.0.1", port))
                    .connectNow(Duration.ofSeconds(timeoutSeconds));
            ensureMqttPipeline();

            MqttMessage connect = MqttMessageBuilder.connectMessage(
                    clientId, "", "", "", "", false, false, false, 0, 60);
            writeAndFlush(connect);
            connection.inbound().receiveObject().ofType(MqttMessage.class)
                    .subscribe(this::onInbound);

            MqttConnAckMessage ack = (MqttConnAckMessage) awaitMessage(
                    msg -> msg instanceof MqttConnAckMessage,
                    Duration.ofSeconds(timeoutSeconds));
            if (ack == null) {
                throw new IllegalStateException("connect ack timeout for [" + clientId + "]");
            }
            if (ack.variableHeader().connectReturnCode()
                    != MqttConnectReturnCode.CONNECTION_ACCEPTED) {
                throw new IllegalStateException(
                        "connection [" + clientId + "] rejected: "
                                + ack.variableHeader().connectReturnCode());
            }
        }

        void close() {
            if (connection != null && !connection.isDisposed()) {
                connection.disposeNow();
            }
        }

        private void writeAndFlush(MqttMessage message) {
            if (connection.channel().eventLoop().inEventLoop()) {
                connection.channel().writeAndFlush(message).syncUninterruptibly();
                return;
            }
            connection.channel().eventLoop()
                    .submit(() -> connection.channel().writeAndFlush(message))
                    .syncUninterruptibly();
        }

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

        private void onInbound(MqttMessage message) {
            if (message.fixedHeader().messageType() == MqttMessageType.PUBACK) {
                return;
            }
            inbox.add(message);
        }

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
    }
}
