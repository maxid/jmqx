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
     *
     * @throws Exception 测试异常
     */
    @Test
    void testPublishToClientCluster() throws Exception {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.getLogger("root").setLevel(Level.INFO);
        loggerContext.getLogger("plus.jmqx.broker").setLevel(Level.INFO);

        MqttConfiguration config = new MqttConfiguration();
        config.setBusinessQueueSize(Integer.MAX_VALUE);
        config.setSslEnable(false);
        config.setPort(5883);
        config.setSecurePort(-1);
        config.setWebsocketPort(-1);
        config.setWebsocketSecurePort(-1);
        config.getClusterConfig().setEnabled(true);
        config.getClusterConfig().setUrl("127.0.0.1:7773");
        config.getClusterConfig().setPort(7773);
        config.getClusterConfig().setNode("node-publish-target");
        config.getClusterConfig().setNamespace("jmqx-publish-target-cluster");

        Bootstrap bootstrap = new Bootstrap(config);
        try {
            bootstrap.start().block(Duration.ofSeconds(10));
            String targetClientId = "cluster-target-device";
            String topic = "cluster/target";
            byte[] payload = "hello-cluster".getBytes(StandardCharsets.UTF_8);

            // 设备连接
            Connection dev = TcpClient.create()
                    .resolver(NoopAddressResolverGroup.INSTANCE)
                    .remoteAddress(() -> new InetSocketAddress("127.0.0.1", 5883))
                    .connectNow(Duration.ofSeconds(5));
            addMqttCodec(dev);
            ConcurrentLinkedQueue<MqttMessage> inbox = new ConcurrentLinkedQueue<>();
            dev.inbound().receiveObject().ofType(MqttMessage.class).subscribe(msg -> {
                if (msg instanceof MqttPublishMessage) {
                    ((MqttPublishMessage) msg).retain();
                }
                inbox.add(msg);
            });
            dev.channel().writeAndFlush(MqttMessageBuilder.connectMessage(
                    targetClientId, "", "", "", "", false, false, false, 0, 60));
            MqttConnAckMessage ack = (MqttConnAckMessage) awaitPublish(inbox,
                    msg -> msg instanceof MqttConnAckMessage, 5);
            assertNotNull(ack, "device connect ack timeout");
            log.info("device [{}] connected to cluster broker", targetClientId);

            // 通过 dispatcher 定向投递（集群模式走 PUBLISH_TARGET + TailIntercept）
            MessageDispatcher dispatcher = NamespaceContextHolder.get(
                    config.getClusterConfig().getNamespace()).getContext().getMessageDispatcher();
            MqttPublishMessage pubMsg = MqttMessageBuilder.publishMessage(
                    false, MqttQoS.AT_LEAST_ONCE, 0, topic, Unpooled.wrappedBuffer(payload));
            dispatcher.publish(targetClientId, pubMsg);
            log.info("published to [{}] topic [{}] (cluster mode)", targetClientId, topic);

            // 设备验证收到消息
            MqttMessage received = awaitPublish(inbox,
                    msg -> msg instanceof MqttPublishMessage
                            && topic.equals(((MqttPublishMessage) msg).variableHeader().topicName()),
                    5);
            assertNotNull(received, "device did not receive targeted publish in cluster mode");
            MqttPublishMessage rp = (MqttPublishMessage) received;
            assertEquals(topic, rp.variableHeader().topicName());
            byte[] rpPayload = new byte[rp.payload().readableBytes()];
            rp.payload().readBytes(rpPayload);
            assertEquals("hello-cluster", new String(rpPayload, StandardCharsets.UTF_8));
            log.info("device received targeted publish verified (cluster mode)");
        } finally {
            bootstrap.shutdown();
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
