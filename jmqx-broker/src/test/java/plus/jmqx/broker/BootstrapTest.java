package plus.jmqx.broker;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.resolver.NoopAddressResolverGroup;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.LoggerFactory;
import plus.jmqx.broker.mqtt.MqttConfiguration;
import plus.jmqx.broker.mqtt.context.NamespaceContextHolder;
import plus.jmqx.broker.mqtt.message.MessageDispatcher;
import plus.jmqx.broker.mqtt.message.MqttMessageBuilder;
import plus.jmqx.broker.mqtt.message.dispatch.*;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpClient;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MQTT Broker 测试用例
 *
 * @author maxid
 * @since 2025/4/22 10:46
 */
@Slf4j
@EnabledIfSystemProperty(named = "jmqx.integration.tests", matches = "true")
class BootstrapTest {

    /**
     * 验证多个 Broker 实例启动与关闭
     *
     * @throws Exception 测试异常
     */
    @Test
    void testBroker() throws Exception {
        setLogContext();

        MqttConfiguration config1 = config("n1", 1883, 1884, 8883, 8884);
        Bootstrap bootstrap1 = new Bootstrap(config1, dispatcher());
        bootstrap1.start().block();

        MqttConfiguration config2 = config("n2", 2883, 2884, 9883, 9884);
        Bootstrap bootstrap2 = new Bootstrap(config2, dispatcher());
        bootstrap2.start().block();

        Thread.sleep(intProp("jmqx.test.await.seconds", 5) * TimeUnit.SECONDS.toMillis(1));

        bootstrap1.shutdown();
        bootstrap2.shutdown();
    }

    /**
     * 定向投递：向指定 clientId 设备发送消息（单节点）
     *
     * @throws Exception 测试异常
     */
    @Test
    void testPublishToClient() throws Exception {
        String ns = "jmqx-publish-target-" + UUID.randomUUID();
        MqttConfiguration config = config(ns, 4883);
        Bootstrap bootstrap = new Bootstrap(config);
        try {
            bootstrap.start().block(Duration.ofSeconds(5));
            MqttDevice device = connect("publish-target-device", 4883);

            MessageDispatcher dispatcher = NamespaceContextHolder.get(ns, "").getContext().getMessageDispatcher();
            MqttPublishMessage pubMsg = MqttMessageBuilder.publishMessage(
                    false, MqttQoS.AT_LEAST_ONCE, 0, "test/target",
                    Unpooled.wrappedBuffer("hello-target".getBytes(StandardCharsets.UTF_8)));
            dispatcher.publish(device.clientId, pubMsg);
            log.info("published to [{}] topic [test/target]", device.clientId);

            assertReceived(device, "test/target", "hello-target");
        } finally {
            bootstrap.shutdown();
        }
    }

    /**
     * 设置测试日志级别
     */
    private void setLogContext() {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.getLogger("root").setLevel(Level.INFO);
        loggerContext.getLogger("reactor.netty").setLevel(Level.INFO);
        loggerContext.getLogger("plus.jmqx.broker").setLevel(Level.INFO);
        loggerContext.getLogger("plus.jmqx.broker.mqtt.message.impl").setLevel(Level.DEBUG);
    }

    /**
     * 创建基础配置
     *
     * @param namespace 命名空间
     * @param mqttPort  MQTT 端口
     * @param mqttsPort MQTTS 端口
     * @param wsPort    WS 端口
     * @param wssPort   WSS 端口
     * @return MQTT 配置
     */
    private MqttConfiguration config(String namespace, int mqttPort, int mqttsPort, int wsPort, int wssPort) {
        MqttConfiguration config = new MqttConfiguration();
        config.setBusinessQueueSize(Integer.MAX_VALUE);
        config.setSslEnable(true);
        config.setPort(mqttPort);
        config.setSecurePort(mqttsPort);
        config.setWebsocketPort(wsPort);
        config.setWebsocketSecurePort(wssPort);
        config.setSslCa(Objects.requireNonNull(BootstrapTest.class.getResource("/ca.crt")).getPath());
        config.setSslCrt(Objects.requireNonNull(BootstrapTest.class.getResource("/server.crt")).getPath());
        config.setSslKey(Objects.requireNonNull(BootstrapTest.class.getResource("/server.key")).getPath());
        config.getClusterConfig().setNamespace(namespace);
        return config;
    }

    /**
     * 创建基础配置（无 SSL，单端口）
     *
     * @param namespace 命名空间
     * @param mqttPort  MQTT 端口
     * @return MQTT 配置
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
     *
     * @param namespace 命名空间
     * @param node      节点名称
     * @param mqttPort  MQTT 端口
     * @return MQTT 配置
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
        final Connection connection;
        final ConcurrentLinkedQueue<MqttMessage> inbox;
        final String clientId;

        MqttDevice(Connection connection, ConcurrentLinkedQueue<MqttMessage> inbox, String clientId) {
            this.connection = connection;
            this.inbox = inbox;
            this.clientId = clientId;
        }

        /**
         * 订阅主题
         *
         * @param topic 主题
         * @param qos   QoS 等级
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
     *
     * @param clientId 客户端 ID
     * @param port     MQTT 端口
     * @return MQTT 设备
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
     *
     * @param device          设备
     * @param expectedTopic   期望主题
     * @param expectedPayload 期望负载
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
     *
     * @param device        设备
     * @param topic         主题
     * @param timeoutSecond 超时秒数
     */
    private static void assertNotReceived(MqttDevice device, String topic, long timeoutSecond) {
        MqttMessage notReceived = awaitPublish(device.inbox,
                msg -> msg instanceof MqttPublishMessage
                        && topic.equals(((MqttPublishMessage) msg).variableHeader().topicName()),
                timeoutSecond);
        assertNull(notReceived, "device [" + device.clientId + "] should NOT have received publish on " + topic);
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

    /**
     * 为 Netty 连接添加 MQTT 编解码器
     *
     * @param conn Netty 连接
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
     * @param inbox     消息队列
     * @param predicate 匹配条件
     * @param seconds   超时秒数
     * @param <T>       消息类型
     * @return 匹配的消息，超时返回 null
     */
    private static <T extends MqttMessage> T awaitPublish(ConcurrentLinkedQueue<MqttMessage> inbox,
                                                          Predicate<MqttMessage> predicate, long seconds) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        while (System.nanoTime() < deadline) {
            for (MqttMessage msg : inbox) {
                if (predicate.test(msg)) {
                    inbox.remove(msg);
                    @SuppressWarnings("unchecked")
                    T t = (T) msg;
                    return t;
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
     * 构造测试用消息分发器
     *
     * @return 分发器
     */
    private PlatformDispatcher dispatcher() {
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
                    log.info("PublishMessage(clientId={}, username={}, topic={}, payload={})",
                            message.getClientId(),
                            message.getUsername(),
                            message.getTopic(),
                            new String(message.getPayload(), StandardCharsets.UTF_8));
                });
            }
        };
    }

}
