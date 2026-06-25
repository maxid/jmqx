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

        String namespace = "jmqx-publish-target-cluster-" + UUID.randomUUID();
        String topic = "cluster/target";
        byte[] payload = "hello-cluster".getBytes(StandardCharsets.UTF_8);
        String deviceA = "device-a";
        String deviceB = "device-b";

        // Node-1: MQTT port 5883, cluster port 7773
        MqttConfiguration config1 = new MqttConfiguration();
        config1.setBusinessQueueSize(Integer.MAX_VALUE);
        config1.setSslEnable(false);
        config1.setPort(5883);
        config1.setSecurePort(-1);
        config1.setWebsocketPort(-1);
        config1.setWebsocketSecurePort(-1);
        config1.getClusterConfig().setEnabled(true);
        config1.getClusterConfig().setUrl("127.0.0.1:7773,127.0.0.1:7774");
        config1.getClusterConfig().setPort(7773);
        config1.getClusterConfig().setNode("node-1");
        config1.getClusterConfig().setNamespace(namespace);
        Bootstrap bootstrap1 = new Bootstrap(config1);
        bootstrap1.start().block(Duration.ofSeconds(10));

        // Node-2: MQTT port 6883, cluster port 7774
        MqttConfiguration config2 = new MqttConfiguration();
        config2.setBusinessQueueSize(Integer.MAX_VALUE);
        config2.setSslEnable(false);
        config2.setPort(6883);
        config2.setSecurePort(-1);
        config2.setWebsocketPort(-1);
        config2.setWebsocketSecurePort(-1);
        config2.getClusterConfig().setEnabled(true);
        config2.getClusterConfig().setUrl("127.0.0.1:7773,127.0.0.1:7774");
        config2.getClusterConfig().setPort(7774);
        config2.getClusterConfig().setNode("node-2");
        config2.getClusterConfig().setNamespace(namespace);
        Bootstrap bootstrap2 = new Bootstrap(config2);
        bootstrap2.start().block(Duration.ofSeconds(10));

        // 等待集群形成
        Thread.sleep(3000);

        try {
            // ---- Device A 连接到 node-1，订阅 topic ----
            Connection devA = TcpClient.create()
                    .resolver(NoopAddressResolverGroup.INSTANCE)
                    .remoteAddress(() -> new InetSocketAddress("127.0.0.1", 5883))
                    .connectNow(Duration.ofSeconds(5));
            addMqttCodec(devA);
            ConcurrentLinkedQueue<MqttMessage> inboxA = new ConcurrentLinkedQueue<>();
            devA.inbound().receiveObject().ofType(MqttMessage.class).subscribe(msg -> {
                if (msg instanceof MqttPublishMessage) {
                    ((MqttPublishMessage) msg).retain();
                }
                inboxA.add(msg);
            });
            devA.channel().writeAndFlush(MqttMessageBuilder.connectMessage(
                    deviceA, "", "", "", "", false, false, false, 0, 60));
            assertNotNull(awaitPublish(inboxA, msg -> msg instanceof MqttConnAckMessage, 5),
                    "deviceA connect ack timeout");
            log.info("deviceA [{}] connected to node-1", deviceA);
            // Device A subscribe
            devA.channel().writeAndFlush(MqttMessageBuilder.subMessage(1,
                    java.util.Collections.singletonList(new MqttTopicSubscription(topic, MqttQoS.AT_LEAST_ONCE))));
            assertNotNull(awaitPublish(inboxA, msg -> msg instanceof MqttSubAckMessage, 5),
                    "deviceA subscribe ack timeout");
            log.info("deviceA subscribed to [{}]", topic);

            // ---- Device B 连接到 node-1，订阅同一 topic ----
            Connection devB = TcpClient.create()
                    .resolver(NoopAddressResolverGroup.INSTANCE)
                    .remoteAddress(() -> new InetSocketAddress("127.0.0.1", 5883))
                    .connectNow(Duration.ofSeconds(5));
            addMqttCodec(devB);
            ConcurrentLinkedQueue<MqttMessage> inboxB = new ConcurrentLinkedQueue<>();
            devB.inbound().receiveObject().ofType(MqttMessage.class).subscribe(msg -> {
                if (msg instanceof MqttPublishMessage) {
                    ((MqttPublishMessage) msg).retain();
                }
                inboxB.add(msg);
            });
            devB.channel().writeAndFlush(MqttMessageBuilder.connectMessage(
                    deviceB, "", "", "", "", false, false, false, 0, 60));
            assertNotNull(awaitPublish(inboxB, msg -> msg instanceof MqttConnAckMessage, 5),
                    "deviceB connect ack timeout");
            log.info("deviceB [{}] connected to node-1", deviceB);
            // Device B subscribe
            devB.channel().writeAndFlush(MqttMessageBuilder.subMessage(1,
                    java.util.Collections.singletonList(new MqttTopicSubscription(topic, MqttQoS.AT_LEAST_ONCE))));
            assertNotNull(awaitPublish(inboxB, msg -> msg instanceof MqttSubAckMessage, 5),
                    "deviceB subscribe ack timeout");
            log.info("deviceB subscribed to [{}]", topic);

            // ---- 从 node-2 向 deviceA 定向投递 ----
            MessageDispatcher dispatcher = NamespaceContextHolder.get(namespace, "node-2")
                    .getContext().getMessageDispatcher();
            MqttPublishMessage pubMsg = MqttMessageBuilder.publishMessage(
                    false, MqttQoS.AT_LEAST_ONCE, 0, topic, Unpooled.wrappedBuffer(payload));
            dispatcher.publish(deviceA, pubMsg);
            log.info("published to [{}] topic [{}] from node-2", deviceA, topic);

            // Device A 应收到消息
            MqttMessage received = awaitPublish(inboxA,
                    msg -> msg instanceof MqttPublishMessage
                            && topic.equals(((MqttPublishMessage) msg).variableHeader().topicName()),
                    5);
            assertNotNull(received, "target deviceA should receive targeted publish in cluster mode");
            MqttPublishMessage rp = (MqttPublishMessage) received;
            assertEquals(topic, rp.variableHeader().topicName());
            byte[] rpPayload = new byte[rp.payload().readableBytes()];
            rp.payload().readBytes(rpPayload);
            assertEquals("hello-cluster", new String(rpPayload, StandardCharsets.UTF_8));
            log.info("deviceA received targeted publish verified");

            // Device B 不应收到消息（定向投递不经过主题路由）
            MqttMessage notReceived = awaitPublish(inboxB,
                    msg -> msg instanceof MqttPublishMessage
                            && topic.equals(((MqttPublishMessage) msg).variableHeader().topicName()),
                    2);
            assertNull(notReceived, "non-target deviceB should NOT receive targeted publish");
            log.info("deviceB correctly did not receive the targeted publish");
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

}
