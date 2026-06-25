package plus.jmqx.broker;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.*;
import io.netty.resolver.NoopAddressResolverGroup;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.LoggerFactory;
import plus.jmqx.broker.mqtt.MqttConfiguration;
import plus.jmqx.broker.mqtt.context.NamespaceContextHolder;
import plus.jmqx.broker.mqtt.message.MessageDispatcher;
import plus.jmqx.broker.mqtt.message.MqttMessageBuilder;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpClient;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
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
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.getLogger("root").setLevel(Level.INFO);
        loggerContext.getLogger("plus.jmqx.broker").setLevel(Level.INFO);
        loggerContext.getLogger("plus.jmqx.broker.cluster").setLevel(Level.DEBUG);
        loggerContext.getLogger("reactor.netty").setLevel(Level.INFO);
        MqttConfiguration config = new MqttConfiguration();
        config.getClusterConfig().setEnabled(true);
        config.getClusterConfig().setUrl("127.0.0.1:7771,127.0.0.1:7772");
        config.getClusterConfig().setPort(7771);
        config.getClusterConfig().setNode("node-1");
        config.getClusterConfig().setNamespace("jmqx-cluster");
        Bootstrap bootstrap = new Bootstrap(config);
        bootstrap.start().block();
        Thread.sleep(intProp("jmqx.test.await.seconds", 5) * TimeUnit.SECONDS.toMillis(1));
        bootstrap.shutdown();
    }

    /**
     * 启动集群节点 2 测试
     *
     * @throws Exception 测试异常
     */
    @Test
    void cluster02() throws Exception {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.getLogger("root").setLevel(Level.INFO);
        loggerContext.getLogger("plus.jmqx.broker").setLevel(Level.INFO);
        loggerContext.getLogger("plus.jmqx.broker.cluster").setLevel(Level.DEBUG);
        loggerContext.getLogger("reactor.netty").setLevel(Level.INFO);
        MqttConfiguration config = new MqttConfiguration();
        config.setPort(2883);
        config.setSecurePort(2884);
        config.setWebsocketPort(9883);
        config.setWebsocketSecurePort(9884);
        config.getClusterConfig().setEnabled(true);
        config.getClusterConfig().setUrl("127.0.0.1:7771,127.0.0.1:7772");
        config.getClusterConfig().setPort(7772);
        config.getClusterConfig().setNode("node-2");
        config.getClusterConfig().setNamespace("jmqx-cluster");
        Bootstrap bootstrap = new Bootstrap(config);
        bootstrap.start().block();
        Thread.sleep(intProp("jmqx.test.await.seconds", 5) * TimeUnit.SECONDS.toMillis(1));
        bootstrap.shutdown();
    }

    /**
     * 单节点模式测试
     *
     * @throws Exception 测试异常
     */
    @Test
    void clusterSingle() throws Exception {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.getLogger("root").setLevel(Level.INFO);
        loggerContext.getLogger("plus.jmqx.broker").setLevel(Level.INFO);
        loggerContext.getLogger("plus.jmqx.broker.cluster").setLevel(Level.DEBUG);
        loggerContext.getLogger("reactor.netty").setLevel(Level.INFO);
        MqttConfiguration config = new MqttConfiguration();
        config.setPort(3883);
        config.setSecurePort(3884);
        config.setWebsocketPort(10883);
        config.setWebsocketSecurePort(10884);
        config.getClusterConfig().setEnabled(false);
        config.getClusterConfig().setUrl("127.0.0.1:7771,127.0.0.1:7772");
        config.getClusterConfig().setPort(7772);
        config.getClusterConfig().setNode("node-3");
        config.getClusterConfig().setNamespace("jmqx-cluster");
        Bootstrap bootstrap = new Bootstrap(config);
        bootstrap.start().block();
        Thread.sleep(intProp("jmqx.test.await.seconds", 5) * TimeUnit.SECONDS.toMillis(1));
        bootstrap.shutdown();
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

        // Node-1: MQTT port 5883, cluster port 7773
        MqttConfiguration config1 = config(namespace, "node-1", 5883);
        config1.getClusterConfig().setUrl("127.0.0.1:7773,127.0.0.1:7774");
        config1.getClusterConfig().setPort(7773);
        Bootstrap bootstrap1 = new Bootstrap(config1);
        bootstrap1.start().block(Duration.ofSeconds(10));

        // Node-2: MQTT port 6883, cluster port 7774
        MqttConfiguration config2 = config(namespace, "node-2", 6883);
        config2.getClusterConfig().setUrl("127.0.0.1:7773,127.0.0.1:7774");
        config2.getClusterConfig().setPort(7774);
        Bootstrap bootstrap2 = new Bootstrap(config2);
        bootstrap2.start().block(Duration.ofSeconds(10));

        // 等待集群形成
        Thread.sleep(3000);

        try {
            MqttDevice deviceA = connect("device-a", 5883);
            deviceA.subscribe(topic, MqttQoS.AT_LEAST_ONCE);

            MqttDevice deviceB = connect("device-b", 5883);
            deviceB.subscribe(topic, MqttQoS.AT_LEAST_ONCE);

            // 从 node-2 向 deviceA 定向投递
            MessageDispatcher dispatcher = NamespaceContextHolder.get(namespace, "node-2")
                    .getContext().getMessageDispatcher();
            MqttPublishMessage pubMsg = MqttMessageBuilder.publishMessage(
                    false, MqttQoS.AT_LEAST_ONCE, 0, topic,
                    Unpooled.wrappedBuffer("hello-cluster".getBytes(StandardCharsets.UTF_8)));
            dispatcher.publish(deviceA.clientId, pubMsg);
            log.info("published to [{}] topic [{}] from node-2", deviceA.clientId, topic);

            assertReceived(deviceA, topic, "hello-cluster");
            assertNotReceived(deviceB, topic, 2);
        } finally {
            bootstrap2.shutdown();
            bootstrap1.shutdown();
        }
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

}
