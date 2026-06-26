package plus.jmqx.broker;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.*;
import io.netty.resolver.NoopAddressResolverGroup;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.LoggerFactory;
import plus.jmqx.broker.mqtt.MqttConfiguration;
import plus.jmqx.broker.mqtt.context.NamespaceContextHolder;
import plus.jmqx.broker.mqtt.context.ReceiveContext;
import plus.jmqx.broker.mqtt.message.MessageDispatcher;
import plus.jmqx.broker.mqtt.message.MqttMessageBuilder;
import plus.jmqx.broker.mqtt.message.dispatch.*;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpClient;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 集群测试用例
 *
 * @author maxid
 * @since 2025/4/22 10:46
 */
@Slf4j
@EnabledIfSystemProperty(named = "jmqx.integration.tests", matches = "true")
public class BootstrapTest {

    /**
     * 启动集群节点 1 测试
     *
     * @throws Exception 测试异常
     */
    @Test
    void cluster01() throws Exception {
        cluster("jmqx-cluster", "node-1", "127.0.0.1:7771,127.0.0.1:7772", 7771, 5,
                true, 1883, 8883, 1884, 8884);
    }

    /**
     * 启动集群节点 2 测试
     *
     * @throws Exception 测试异常
     */
    @Test
    void cluster02() throws Exception {
        cluster("jmqx-cluster", "node-2", "127.0.0.1:7771,127.0.0.1:7772", 7772, 5,
                true, 2883, 9883, 2884, 9884);
    }

    /**
     * 单节点模式测试
     *
     * @throws Exception 测试异常
     */
    @Test
    void clusterSingle() throws Exception {
        cluster("jmqx-cluster", "node-3", "127.0.0.1:7771,127.0.0.1:7772", 7773, 5,
                false, 3883, 9083, 3884, 9084);
    }

    /**
     * 定向投递：向指定 clientId 设备发送消息（集群模式）
     * <p>
     * 启动两个节点，两个设备都连接 node-1 并订阅同一主题，
     * 从 node-2 通过 MqttMessageDispatcher.publish(clientId, message) 向其中一个设备定向投递，
     * 验证目标设备收到消息、非目标设备未收到。
     *
     * @throws Exception 测试异常
     */
    @Test
    void testPublishToClientCluster() throws Exception {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.getLogger("root").setLevel(Level.INFO);
        loggerContext.getLogger("plus.jmqx.broker").setLevel(Level.INFO);

        String namespace = "jmqx-cluster-" + UUID.randomUUID().toString().split("-")[0];
        String topic = "cluster/target";
        String node1 = "node-1";
        String node2 = "node-2";
        int mqtt1Port = 1883;
        int mqtt2Port = 2883;
        String clusterUrls = "127.0.0.1:7771,127.0.0.1:7772";
        String client1Id = "device-a";
        String client2Id = "device-b";
        String testPayload = "hello-cluster";
        ExecutorService exec = Executors.newFixedThreadPool(2);

        // Node-1: MQTT port 1883, cluster port 7771
        exec.submit(() -> cluster(namespace, node1, clusterUrls, 7771, 10,
                true, mqtt1Port, 8883, 1884, 8884));

        // Node-2: MQTT port 2883, cluster port 7772;
        exec.submit(() -> cluster(namespace, node2, clusterUrls, 7772, 10,
                true, mqtt2Port, 9883, 2884, 9884));

        // 等待集群形成
        Thread.sleep(3000);

        try {
            ReceiveContext<?> context1 = NamespaceContextHolder.get(namespace, node1).getContext();
            ReceiveContext<?> context2 = NamespaceContextHolder.get(namespace, node2).getContext();
            MqttDevice deviceA = connect(client1Id, context1.getConfiguration().getPort());
            deviceA.subscribe(topic, MqttQoS.AT_LEAST_ONCE);

            MqttDevice deviceB = connect(client2Id, context1.getConfiguration().getPort());
            deviceB.subscribe(topic, MqttQoS.AT_LEAST_ONCE);

            // 从 node-2 向 deviceA 定向投递
            MessageDispatcher dispatcher = context2.getMessageDispatcher();
            MqttPublishMessage pubMsg = MqttMessageBuilder.publishMessage(false, MqttQoS.AT_MOST_ONCE,
                    0, topic, Unpooled.wrappedBuffer(testPayload.getBytes(StandardCharsets.UTF_8)));
            dispatcher.publish(deviceA.clientId, pubMsg);
            log.info("published to [{}] topic [{}] from node-2", deviceA.clientId, topic);

            assertReceived(deviceA, topic, testPayload);
            assertNotReceived(deviceB, topic, 2);
        } finally {
            exec.shutdown();
        }
    }

    @Test
    void clusterDispatcher01() throws Exception {
        cluster("jmqx-cluster", "node-1", "127.0.0.1:7771,127.0.0.1:7772", 7771, 5,
                true, 1883, 8883, 1884, 8884,
                false, this::dispatcher);
    }

    @Test
    void clusterDispatcher02() throws Exception {
        cluster("jmqx-cluster", "node-2", "127.0.0.1:7771,127.0.0.1:7772", 7772, 5,
                true, 2883, 9883, 2884, 9884,
                true, this::dispatcher);
    }

    /**
     * 启动集群节点
     *
     * @param namespace 命名空间
     * @param node      节点名称
     * @param urls      集群节点地址
     * @param port      集群节点端口
     * @param timeout   测试超时时间（秒）
     * @param mqttPort  MQTT 端口
     * @param wsPort    WebSocket 端口
     * @param mqttsPort MQTTS 端口
     * @param wssPort   WebSocket Secure 端口
     */
    private void cluster(String namespace, String node, String urls, int port, int timeout, boolean enabled,
                         int mqttPort, int wsPort, int mqttsPort, int wssPort) {
        cluster(namespace, node, urls, port, timeout, enabled, mqttPort, mqttsPort, wsPort, wssPort, false, null);
    }

    /**
     * 启动集群节点
     *
     * @param namespace   命名空间
     * @param node        节点名称
     * @param urls        集群节点地址
     * @param port        集群节点端口
     * @param timeout     测试超时时间（秒）
     * @param mqttPort    MQTT 端口
     * @param wsPort      WebSocket 端口
     * @param mqttsPort   MQTTS 端口
     * @param wssPort     WebSocket Secure 端口
     * @param enableReply 是否启用回复
     * @param dispatcher  消息分发器
     */
    private void cluster(String namespace, String node, String urls, int port, int timeout, boolean enabled,
                         int mqttPort, int wsPort, int mqttsPort, int wssPort, boolean enableReply,
                         DispatcherApply dispatcher) {
        Bootstrap bootstrap = null;
        try {
            LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
            loggerContext.getLogger("root").setLevel(Level.INFO);
            loggerContext.getLogger("plus.jmqx.broker").setLevel(Level.INFO);
            loggerContext.getLogger("plus.jmqx.broker.cluster").setLevel(Level.DEBUG);
            loggerContext.getLogger("reactor.netty").setLevel(Level.INFO);
            MqttConfiguration config = new MqttConfiguration();
            config.setPort(mqttPort);
            config.setWebsocketPort(wsPort);
            config.setSslEnable(true);
            config.setSecurePort(mqttsPort);
            config.setWebsocketSecurePort(wssPort);
            config.setSslCa(Objects.requireNonNull(BootstrapTest.class.getResource("/ca.crt")).getPath());
            config.setSslCrt(Objects.requireNonNull(BootstrapTest.class.getResource("/server.crt")).getPath());
            config.setSslKey(Objects.requireNonNull(BootstrapTest.class.getResource("/server.key")).getPath());
            config.getClusterConfig().setEnabled(enabled);
            config.getClusterConfig().setUrl(urls);
            config.getClusterConfig().setPort(port);
            config.getClusterConfig().setNode(node);
            config.getClusterConfig().setNamespace(namespace);
            bootstrap = dispatcher == null ? new Bootstrap(config)
                    : new Bootstrap(config, dispatcher.apply(config.getClusterConfig(), enableReply));
            bootstrap.start().block();
            Thread.sleep(intProp("jmqx.test.await.seconds", timeout) * TimeUnit.SECONDS.toMillis(1));
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (bootstrap != null) {
                bootstrap.shutdown();
            }
        }
    }

    interface DispatcherApply {

        /**
         * 应用消息分发器
         *
         * @param config      集群配置
         * @param enableReply 是否启用回复
         * @return 消息分发器
         */
        PlatformDispatcher apply(MqttConfiguration.ClusterConfig config, boolean enableReply);

    }

    /**
     * 构造测试用消息分发器（不适合用于压测）
     *
     * @return 分发器
     */
    private PlatformDispatcher dispatcher(MqttConfiguration.ClusterConfig config, boolean enableReply) {
        AtomicReference<String> clientId = new AtomicReference<>();
        if (enableReply) {
            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread thread = new Thread(r);
                thread.setName("jmqx-target-reply");
                return thread;
            });
            executor.scheduleAtFixedRate(() -> {
                // 消息下发示例, QoS 必须为 0
                MqttPublishMessage reply = MqttMessageBuilder.publishMessage(
                        false, MqttQoS.AT_MOST_ONCE, 0, "target_client_reply",
                        Unpooled.wrappedBuffer("message reply".getBytes(StandardCharsets.UTF_8)));
                MessageDispatcher dispatcher = NamespaceContextHolder.get(config.getNamespace(), config.getNode())
                        .getContext().getMessageDispatcher();
                if (clientId.get() != null && !clientId.get().isEmpty()) {
                    // 向指定 clientId 设备发送消息
                    dispatcher.publish(clientId.get(), reply);
                }
            }, 5, 5, TimeUnit.SECONDS);
        }
        return new PlatformDispatcher() {

            /**
             * 处理连接消息
             *
             * @param message 连接消息
             * @return 处理结果
             */
            @Override
            public Mono<Void> onConnect(ConnectMessage message) {
                return Mono.fromRunnable(() -> {
                    log.info("{}", message);
                });
            }

            /**
             * 处理断开连接消息
             *
             * @param message 断开消息
             * @return 处理结果
             */
            @Override
            public Mono<Void> onDisconnect(DisconnectMessage message) {
                return Mono.fromRunnable(() -> {
                    log.info("{}", message);
                });
            }

            /**
             * 处理连接丢失消息
             *
             * @param message 连接丢失消息
             * @return 处理结果
             */
            @Override
            public Mono<Void> onConnectionLost(ConnectionLostMessage message) {
                return Mono.fromRunnable(() -> {
                    log.info("{}", message);
                });
            }

            /**
             * 处理发布消息
             *
             * @param message 发布消息
             * @return 处理结果
             */
            @Override
            public Mono<Void> onPublish(PublishMessage message) {
                return Mono.fromRunnable(() -> {
                    String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
                    //*
                    log.info("PublishMessage(clientId={}, username={}, topic={}, payload={})",
                            message.getClientId(),
                            message.getUsername(),
                            message.getTopic(),
                            payload);
                    //*/
                    if (payload.startsWith("{") && payload.endsWith("}") && payload.contains("clientId")) {
                        JSONObject json = JSONUtil.parseObj(payload);
                        if (json.containsKey("clientId")) {
                            clientId.set(json.getStr("clientId"));
                        }
                    }
                });
            }

        };
    }

    /**
     * 为 Netty 连接添加 MQTT 编解码器
     */
    private static void addMqttCodec(Connection conn) {
        String bridge = "reactor.right.reactiveBridge";
        if (conn.channel().pipeline().get(bridge) != null) {
            if (conn.channel().pipeline().get(MqttEncoder.class) == null) {
                conn.channel().pipeline().addBefore(bridge, "mqttEncoder", MqttEncoder.INSTANCE);
            }
            if (conn.channel().pipeline().get(MqttDecoder.class) == null) {
                conn.channel().pipeline().addBefore(bridge, "mqttDecoder", new MqttDecoder(1024 * 1024));
            }
            return;
        }
        if (conn.channel().pipeline().get(MqttEncoder.class) == null) {
            conn.channel().pipeline().addFirst("mqttEncoder", MqttEncoder.INSTANCE);
        }
        if (conn.channel().pipeline().get(MqttDecoder.class) == null) {
            conn.channel().pipeline().addLast("mqttDecoder", new MqttDecoder(1024 * 1024));
        }
    }

    /**
     * 在消息队列中等待匹配的消息
     *
     * @param <T> 消息类型
     */
    @SuppressWarnings("unchecked")
    private static <T extends MqttMessage> T awaitPublish(ConcurrentLinkedQueue<MqttMessage> inbox,
                                                          Predicate<MqttMessage> predicate, long seconds) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        while (System.nanoTime() < deadline) {
            for (MqttMessage msg : inbox) {
                if (predicate.test(msg)) {
                    inbox.remove(msg);
                    return (T) msg;
                }
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        }
        return null;
    }

    /**
     * 读取整型系统属性
     *
     * @param key 属性名
     * @param def 默认值
     * @return 属性值
     */
    private static int intProp(String key, int def) {
        String value = System.getProperty(key);
        if (value == null || value.isEmpty()) {
            return def;
        }
        return Integer.parseInt(value);
    }

    // ========== 通用测试辅助方法 ==========

    /**
     * 创建基础配置（无 SSL，单端口）
     */
    private static MqttConfiguration config(String namespace, int mqttPort) {
        MqttConfiguration config = new MqttConfiguration();
        config.setBusinessQueueSize(Integer.MAX_VALUE);
        config.setSslEnable(false);
        config.setPort(mqttPort);
        config.setSecurePort(-1);
        config.setWebsocketPort(-1);
        config.setWebsocketSecurePort(-1);
        config.getClusterConfig().setNamespace(namespace);
        config.getClusterConfig().setNode("");
        return config;
    }

    /**
     * 创建集群节点配置（无 SSL，单端口）
     */
    private static MqttConfiguration config(String namespace, String node, int mqttPort) {
        MqttConfiguration config = config(namespace, mqttPort);
        config.getClusterConfig().setNode(node);
        config.getClusterConfig().setEnabled(true);
        return config;
    }

    /**
     * MQTT 设备抽象，持有连接、收件箱和 clientId
     */
    private static class MqttDevice {
        final Connection                         connection;
        final ConcurrentLinkedQueue<MqttMessage> inbox;
        final String                             clientId;

        MqttDevice(Connection connection, ConcurrentLinkedQueue<MqttMessage> inbox, String clientId) {
            this.connection = connection;
            this.inbox = inbox;
            this.clientId = clientId;
        }

        /**
         * 订阅主题
         */
        void subscribe(String topic, MqttQoS qos) {
            connection.channel().writeAndFlush(MqttMessageBuilder.subMessage(1,
                    Collections.singletonList(new MqttTopicSubscription(topic, qos))));
            assertNotNull(awaitPublish(inbox, msg -> msg instanceof MqttSubAckMessage, 5),
                    "subscribe ack timeout for " + topic);
        }
    }

    /**
     * 连接设备并完成 MQTT 握手
     */
    private static MqttDevice connect(String clientId, int port) {
        Connection connection = TcpClient.create()
                .resolver(NoopAddressResolverGroup.INSTANCE)
                .remoteAddress(() -> new InetSocketAddress("127.0.0.1", port))
                .connectNow(Duration.ofSeconds(5));
        addMqttCodec(connection);
        ConcurrentLinkedQueue<MqttMessage> inbox = new ConcurrentLinkedQueue<>();
        connection.inbound().receiveObject().ofType(MqttMessage.class).subscribe(msg -> {
            if (msg instanceof MqttPublishMessage) {
                ((MqttPublishMessage) msg).retain();
            }
            inbox.add(msg);
        });
        connection.channel().writeAndFlush(MqttMessageBuilder.connectMessage(
                clientId, "", "", "", "", false, false, false, 0, 60));
        MqttConnAckMessage ack = (MqttConnAckMessage) awaitPublish(inbox,
                msg -> msg instanceof MqttConnAckMessage, 5);
        assertNotNull(ack, "device connect ack timeout for " + clientId);
        return new MqttDevice(connection, inbox, clientId);
    }

    /**
     * 验证设备收到指定主题和内容的发布消息
     */
    private static void assertReceived(MqttDevice device, String expectedTopic, String expectedPayload) {
        MqttMessage received = awaitPublish(device.inbox,
                msg -> msg instanceof MqttPublishMessage
                        && expectedTopic.equals(((MqttPublishMessage) msg).variableHeader().topicName()),
                5);
        assertNotNull(received, "device [" + device.clientId + "] did not receive publish on " + expectedTopic);
        MqttPublishMessage rp = (MqttPublishMessage) received;
        assertEquals(expectedTopic, rp.variableHeader().topicName());
        byte[] rpPayload = new byte[rp.payload().readableBytes()];
        rp.payload().readBytes(rpPayload);
        assertEquals(expectedPayload, new String(rpPayload, StandardCharsets.UTF_8));
    }

    /**
     * 验证设备在指定时间内未收到消息
     */
    private static void assertNotReceived(MqttDevice device, String topic, long timeoutSecond) {
        MqttMessage notReceived = awaitPublish(device.inbox,
                msg -> msg instanceof MqttPublishMessage
                        && topic.equals(((MqttPublishMessage) msg).variableHeader().topicName()),
                timeoutSecond);
        assertNull(notReceived, "device [" + device.clientId + "] should NOT have received publish on " + topic);
    }

    // ================================================================
    //  集群压测相关
    // ================================================================

    /**
     * 集群压测：两个节点组成集群，模拟设备平均连接两个节点进行消息上报压力测试
     * <p>
     * 启动 node-1 和 node-2 组成集群，设备均分连接至两节点，循环发布消息。
     * 统计 published / acked / dispatchReceived 指标，验证集群下的消息吞吐能力。
     *
     * @throws Exception 测试异常
     */
    @Test
    void testClusterStress() throws Exception {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.getLogger("root").setLevel(Level.INFO);
        loggerContext.getLogger("plus.jmqx.broker").setLevel(Level.INFO);
        loggerContext.getLogger("plus.jmqx.broker.cluster").setLevel(Level.INFO);

        StressConfig stress = loadStressConfig();
        String namespace = "jmqx-cluster-stress-" + UUID.randomUUID().toString().split("-")[0];
        String clusterUrls = "127.0.0.1:7771,127.0.0.1:7772";

        AtomicLong dispatchReceived = new AtomicLong();

        // Node-1: MQTT = stress.port, cluster port = 7771
        Bootstrap bootstrap1 = startClusterNode(namespace, "node-1", clusterUrls, 7771,
                stress.port, stressDispatcher(dispatchReceived));
        log.info("cluster node-1 started on MQTT port {}", stress.port);

        // Node-2: MQTT = stress.port + 1000, cluster port = 7772
        Bootstrap bootstrap2 = startClusterNode(namespace, "node-2", clusterUrls, 7772,
                stress.port + 1000, stressDispatcher(dispatchReceived));
        log.info("cluster node-2 started on MQTT port {}", stress.port + 1000);

        // 等待集群形成
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
                    () -> logClusterProgress(stress, published, acked, dispatchReceived, lastAcked, lastReportAt, start),
                    stress.reportIntervalSeconds,
                    stress.reportIntervalSeconds,
                    TimeUnit.SECONDS);

            // 预检发布
            if (!preflightPublishCluster(stress, namespace, acked, dispatchReceived)) {
                reporter.shutdownNow();
                executor.shutdownNow();
                StressResult failed = new StressResult(acked.get(), start, System.nanoTime(), false);
                logClusterStressResult(stress, failed, dispatchReceived.get());
                return;
            }

            // 启动连接 node-1 的设备
            for (int i = 0; i < devicesPerNode; i++) {
                int idx = i;
                executor.submit(() -> runStressClient("stress-n1-" + idx, stress.port,
                        stress, acked, published, allClients, latch));
            }
            // 启动连接 node-2 的设备
            for (int i = 0; i < devicesPerNode; i++) {
                int idx = i;
                executor.submit(() -> runStressClient("stress-n2-" + idx, stress.port + 1000,
                        stress, acked, published, allClients, latch));
            }

            boolean ok = latch.await(stress.timeoutSeconds, TimeUnit.SECONDS);
            long end = System.nanoTime();
            executor.shutdownNow();
            reporter.shutdownNow();

            // 关闭所有压测客户端
            for (MqttStressClient client : allClients) {
                try {
                    client.close();
                } catch (Exception e) {
                    // ignore
                }
            }

            StressResult result = new StressResult(acked.get(), start, end, ok);
            logClusterStressResult(stress, result, dispatchReceived.get());
        } finally {
            bootstrap2.shutdown();
            bootstrap1.shutdown();
        }
    }

    /**
     * 启动单个集群节点（无 SSL，适用于压测）
     *
     * @param namespace  命名空间
     * @param node       节点名称
     * @param urls       集群节点地址
     * @param port       集群节点端口
     * @param mqttPort   MQTT 端口
     * @param dispatcher 消息分发器
     * @return Bootstrap 实例
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
            Bootstrap bootstrap = new Bootstrap(config, dispatcher);
            bootstrap.start().block();
            return bootstrap;
        } catch (Exception e) {
            throw new RuntimeException("cluster node [" + node + "] start failed", e);
        }
    }

    /**
     * 运行单个压测设备
     *
     * @param clientId    客户端 ID
     * @param port        MQTT 端口
     * @param config      压测配置
     * @param acked       Ack 计数
     * @param published   发布计数
     * @param allClients  客户端列表
     * @param latch       完成计数器
     */
    private void runStressClient(String clientId, int port, StressConfig config,
                                 AtomicLong acked, AtomicLong published,
                                 List<MqttStressClient> allClients, CountDownLatch latch) {
        MqttStressClient client = new MqttStressClient(clientId, port, acked);
        try {
            client.connect();
            synchronized (allClients) {
                allClients.add(client);
            }
            long sentCount = client.publishLoop(config.topic, config.payloadBytes,
                    config.durationSeconds, config.flushEvery, config.inFlightLimit, published);
            client.awaitAcks(acked, sentCount, config.timeoutSeconds);
        } catch (Exception e) {
            log.error("stress client [{}] error", clientId, e);
        } finally {
            latch.countDown();
        }
    }

    /**
     * 加载压测参数
     *
     * @return 压测配置
     */
    private StressConfig loadStressConfig() {
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

    /**
     * 构造压测用分发器，仅计数接收到的消息
     *
     * @param dispatchReceived 分发计数
     * @return 分发器
     */
    private PlatformDispatcher stressDispatcher(AtomicLong dispatchReceived) {
        return new PlatformDispatcher() {
            @Override
            public Mono<Void> onConnect(ConnectMessage message) {
                return Mono.fromRunnable(() -> {});
            }

            @Override
            public Mono<Void> onDisconnect(DisconnectMessage message) {
                return Mono.fromRunnable(() -> {});
            }

            @Override
            public Mono<Void> onConnectionLost(ConnectionLostMessage message) {
                return Mono.fromRunnable(() -> {});
            }

            @Override
            public Mono<Void> onPublish(PublishMessage message) {
                return Mono.fromRunnable(dispatchReceived::incrementAndGet);
            }
        };
    }

    /**
     * 执行压测预检发布
     *
     * @param config           压测配置
     * @param namespace        命名空间
     * @param acked            Ack 计数
     * @param dispatchReceived 分发计数
     * @return 是否预检成功
     */
    private boolean preflightPublishCluster(StressConfig config, String namespace,
                                            AtomicLong acked, AtomicLong dispatchReceived) {
        MqttStressClient client = new MqttStressClient("stress-preflight", config.port, acked);
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
            return ok;
        } catch (Exception e) {
            log.error("preflight publish failed", e);
            return false;
        } finally {
            client.close();
        }
    }

    /**
     * 等待分发计数增长
     *
     * @param dispatchReceived 分发计数
     * @param beforeDispatch   初始计数
     * @param timeoutSeconds   超时秒数
     * @return 是否成功
     */
    private boolean waitForDispatch(AtomicLong dispatchReceived, long beforeDispatch, int timeoutSeconds) {
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

    /**
     * 输出压测实时进度
     *
     * @param config           压测配置
     * @param published        发布计数
     * @param acked            Ack 计数
     * @param dispatchReceived 分发计数
     * @param lastAcked        上次 Ack 计数
     * @param lastReportAt     上次报告时间
     * @param startNanos       起始时间
     */
    private void logClusterProgress(StressConfig config,
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
        String msg = String.format("cluster stress progress: threads=%d, published=%d, acked=%d, dispatchReceived=%d, intervalThroughput=%.0f msg/s, elapsed=%.1fs",
                config.threads,
                totalPublished,
                totalAcked,
                totalDispatch,
                intervalThroughput,
                elapsedSeconds);
        log.info(msg);
    }

    /**
     * 输出压测汇总结果
     *
     * @param config           压测配置
     * @param result           压测结果
     * @param dispatchReceived 分发计数
     */
    private void logClusterStressResult(StressConfig config, StressResult result, long dispatchReceived) {
        double seconds = (result.endNanos - result.startNanos) / 1_000_000_000.0;
        double throughput = result.sent / Math.max(seconds, 0.001);
        log.info("cluster stress result: threads={}, durationSeconds={}, payloadBytes={}, acked={}, dispatchReceived={}, time={}s, throughput={} msg/s, completed={}",
                config.threads,
                config.durationSeconds,
                config.payloadBytes,
                result.sent,
                dispatchReceived,
                String.format("%.3f", seconds),
                String.format("%.0f", throughput),
                result.completed);
    }

    /**
     * 压测客户端：高吞吐 MQTT 发布，带飞行窗口控制和 PUBACK 跟踪
     */
    private static final class MqttStressClient {
        private final String clientId;
        private final int port;
        private final AtomicLong ackedCounter;
        private Connection connection;
        private final ConcurrentLinkedQueue<MqttMessage> inbox = new ConcurrentLinkedQueue<>();
        private int packetId = 1;
        private final AtomicLong localAcked = new AtomicLong();

        private MqttStressClient(String clientId, int port, AtomicLong ackedCounter) {
            this.clientId = clientId;
            this.port = port;
            this.ackedCounter = ackedCounter;
        }

        void connect() {
            this.connection = TcpClient.create()
                    .resolver(NoopAddressResolverGroup.INSTANCE)
                    .remoteAddress(() -> new InetSocketAddress("127.0.0.1", port))
                    .connectNow(Duration.ofSeconds(5));
            ensureMqttPipeline();

            MqttMessage connect = MqttMessageBuilder.connectMessage(
                    clientId, "", "", "", "", false, false, false, 0, 60);
            writeAndFlush(connect);
            connection.inbound()
                    .receiveObject()
                    .ofType(MqttMessage.class)
                    .subscribe(this::onInbound);
            MqttConnAckMessage ack = (MqttConnAckMessage) awaitMessage(
                    msg -> msg instanceof MqttConnAckMessage,
                    Duration.ofSeconds(5));
            if (ack == null) {
                throw new IllegalStateException("connect ack timeout for " + clientId);
            }
        }

        long publishLoop(String topic, int payloadBytes, int durationSeconds,
                         int flushEvery, int inFlightLimit, AtomicLong published) {
            byte[] payload = new byte[payloadBytes];
            long endAt = System.nanoTime() + TimeUnit.SECONDS.toNanos(durationSeconds);
            int i = 0;
            long localSent = 0;
            while (System.nanoTime() < endAt) {
                if (!connection.channel().isActive()) {
                    break;
                }
                while ((localSent - localAcked.get()) >= inFlightLimit) {
                    if (!safeFlush()) break;
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
                while (!connection.channel().isWritable()) {
                    if (!safeFlush()) break;
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
                if (!connection.channel().isActive()) {
                    break;
                }
                MqttFixedHeader fixedHeader = new MqttFixedHeader(
                        MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false, 0);
                int messageId = nextPacketId();
                MqttPublishVariableHeader header = new MqttPublishVariableHeader(topic, messageId);
                MqttPublishMessage message = new MqttPublishMessage(fixedHeader, header, Unpooled.wrappedBuffer(payload));
                if (!safeWrite(message)) break;
                if (flushEvery > 0 && (i + 1) % flushEvery == 0) {
                    if (!safeFlush()) break;
                }
                published.incrementAndGet();
                i++;
                localSent++;
            }
            safeFlush();
            return localSent;
        }

        void awaitAcks(AtomicLong acked, long expected, int timeoutSeconds) {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
            while (System.nanoTime() < deadline) {
                if (localAcked.get() >= expected) {
                    return;
                }
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }

        boolean publishOnceWaitAck(String topic, int payloadBytes, int timeoutSeconds) {
            byte[] payload = new byte[payloadBytes];
            MqttFixedHeader fixedHeader = new MqttFixedHeader(
                    MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false, 0);
            int messageId = nextPacketId();
            long beforeAck = localAcked.get();
            MqttPublishVariableHeader header = new MqttPublishVariableHeader(topic, messageId);
            MqttPublishMessage message = new MqttPublishMessage(fixedHeader, header, Unpooled.wrappedBuffer(payload));
            connection.channel().writeAndFlush(message).syncUninterruptibly();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
            while (System.nanoTime() < deadline) {
                if (localAcked.get() > beforeAck) {
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

        void close() {
            if (connection != null && !connection.isDisposed()) {
                connection.disposeNow();
            }
        }

        private boolean safeFlush() {
            try {
                if (connection != null && connection.channel().isActive()) {
                    submitFlush();
                    return true;
                }
            } catch (Exception e) {
                // channel closed
            }
            return false;
        }

        private boolean safeWrite(MqttMessage message) {
            try {
                if (connection != null && connection.channel().isActive()) {
                    submitWrite(message);
                    return true;
                }
            } catch (Exception e) {
                // channel closed
            }
            return false;
        }

        private void writeAndFlush(MqttMessage message) {
            if (connection.channel().eventLoop().inEventLoop()) {
                connection.channel().writeAndFlush(message).syncUninterruptibly();
                return;
            }
            connection.channel().eventLoop().submit(() -> connection.channel().writeAndFlush(message)).syncUninterruptibly();
        }

        private void submitWrite(MqttMessage message) {
            if (connection.channel().eventLoop().inEventLoop()) {
                connection.channel().write(message);
                return;
            }
            connection.channel().eventLoop().execute(() -> connection.channel().write(message));
        }

        private void submitFlush() {
            if (connection.channel().eventLoop().inEventLoop()) {
                connection.channel().flush();
                return;
            }
            connection.channel().eventLoop().execute(() -> connection.channel().flush());
        }

        private void ensureMqttPipeline() {
            String bridgeName = "reactor.right.reactiveBridge";
            if (connection.channel().pipeline().get(bridgeName) != null) {
                if (connection.channel().pipeline().get(MqttEncoder.class) == null) {
                    connection.channel().pipeline().addBefore(bridgeName, "mqttEncoder", MqttEncoder.INSTANCE);
                }
                if (connection.channel().pipeline().get(MqttDecoder.class) == null) {
                    connection.channel().pipeline().addBefore(bridgeName, "mqttDecoder", new MqttDecoder(1024 * 1024));
                }
                return;
            }
            if (connection.channel().pipeline().get(MqttEncoder.class) == null) {
                connection.channel().pipeline().addFirst("mqttEncoder", MqttEncoder.INSTANCE);
            }
            if (connection.channel().pipeline().get(MqttDecoder.class) == null) {
                connection.channel().pipeline().addLast("mqttDecoder", new MqttDecoder(1024 * 1024));
            }
        }

        private int nextPacketId() {
            int id = packetId++;
            if (packetId > 0xFFFF) {
                packetId = 1;
            }
            return id;
        }

        private void onInbound(MqttMessage message) {
            if (message.fixedHeader().messageType() == MqttMessageType.PUBACK) {
                ackedCounter.incrementAndGet();
                localAcked.incrementAndGet();
                return;
            }
            inbox.add(message);
        }

        private MqttMessage awaitMessage(Predicate<MqttMessage> predicate, Duration timeout) {
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

    private static final class StressConfig {
        private int port;
        private int threads;
        private int durationSeconds;
        private int payloadBytes;
        private int flushEvery;
        private int inFlightLimit;
        private int timeoutSeconds;
        private int reportIntervalSeconds;
        private String topic;
    }

    private static final class StressResult {
        private final long sent;
        private final long startNanos;
        private final long endNanos;
        private final boolean completed;

        private StressResult(long sent, long startNanos, long endNanos, boolean completed) {
            this.sent = sent;
            this.startNanos = startNanos;
            this.endNanos = endNanos;
            this.completed = completed;
        }
    }

}
