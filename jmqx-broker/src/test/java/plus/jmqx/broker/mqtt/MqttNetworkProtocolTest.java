package plus.jmqx.broker.mqtt;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.netty.resolver.NoopAddressResolverGroup;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCounted;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import plus.jmqx.broker.Bootstrap;
import plus.jmqx.broker.mqtt.message.MqttMessageBuilder;
import plus.jmqx.broker.mqtt.message.dispatch.ConnectMessage;
import plus.jmqx.broker.mqtt.message.dispatch.ConnectionLostMessage;
import plus.jmqx.broker.mqtt.message.dispatch.DisconnectMessage;
import plus.jmqx.broker.mqtt.message.dispatch.PlatformDispatcher;
import plus.jmqx.broker.mqtt.message.dispatch.PublishMessage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpClient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Predicate;
import java.util.UUID;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * MQTT 3.x/5 网络连通性单元测试。
 * 运行需显式启用：-Djmqx.network.tests=true
 */
@EnabledIfSystemProperty(named = "jmqx.network.tests", matches = "true")
class MqttNetworkProtocolTest {

    private static final String TOPIC = "network/test";

    /**
     * MQTT 3.1 连接/订阅/发布链路测试。
     *
     * @throws Exception 测试异常
     */
    @Test
    void mqtt31ConnectSubscribePublish() throws Exception {
        runConnectSubscribePublish(MqttVersion.MQTT_3_1);
    }

    /**
     * MQTT 3.1.1 连接/订阅/发布链路测试。
     *
     * @throws Exception 测试异常
     */
    @Test
    void mqtt311ConnectSubscribePublish() throws Exception {
        runConnectSubscribePublish(MqttVersion.MQTT_3_1_1);
    }

    /**
     * MQTT 5 连接/订阅/发布链路测试。
     *
     * @throws Exception 测试异常
     */
    @Test
    void mqtt5ConnectSubscribePublish() throws Exception {
        runConnectSubscribePublish(MqttVersion.MQTT_5);
    }

    /**
     * QoS1 发布/ACK 流程测试。
     *
     * @throws Exception 测试异常
     */
    @Test
    void qos1PublishAckFlow() throws Exception {
        runPublishWithQos(MqttQoS.AT_LEAST_ONCE);
    }

    /**
     * QoS2 发布/REC/REL/COMP 流程测试。
     *
     * @throws Exception 测试异常
     */
    @Test
    void qos2PublishRecRelCompFlow() throws Exception {
        runPublishWithQos(MqttQoS.EXACTLY_ONCE);
    }

    /**
     * 断线重连后会话保持测试。
     *
     * @throws Exception 测试异常
     */
    @Test
    void sessionPersistenceWithDisconnect() throws Exception {
        int port = randomPort();
        MqttConfiguration config = createConfig(port);

        Bootstrap bootstrap = new Bootstrap(config, noopDispatcher());
        bootstrap.start().block();

        String clientId = "persist-client";
        MqttClient subscriber = new MqttClient(clientId, port);
        subscriber.connect(MqttVersion.MQTT_3_1_1, false, 60);
        subscriber.subscribe(TOPIC, MqttQoS.AT_LEAST_ONCE);
        subscriber.disconnect();

        MqttClient publisher = new MqttClient("publisher", port);
        publisher.connect(MqttVersion.MQTT_3_1_1, true, 60);
        publisher.publish(TOPIC, "offline-message", MqttQoS.AT_LEAST_ONCE);
        publisher.waitPubAck();

        MqttClient reconnect = new MqttClient(clientId, port);
        reconnect.connect(MqttVersion.MQTT_3_1_1, false, 60);
        MqttPublishMessage received = reconnect.awaitPublish(Duration.ofSeconds(5));
        assertNotNull(received);
        ByteBuf payload = received.payload();
        byte[] bytes = new byte[payload.readableBytes()];
        payload.readBytes(bytes);
        reconnect.ackPublish(received);
        ReferenceCountUtil.release(received);
        assertEquals("offline-message", new String(bytes));

        reconnect.close();
        publisher.close();
        bootstrap.shutdown();
    }

    /**
     * 保留消息对后订阅者投递测试。
     *
     * @throws Exception 测试异常
     */
    @Test
    void retainMessageDeliveredToLateSubscriber() throws Exception {
        int port = randomPort();
        MqttConfiguration config = createConfig(port);

        Bootstrap bootstrap = new Bootstrap(config, noopDispatcher());
        bootstrap.start().block();

        MqttClient publisher = new MqttClient("retain-pub", port);
        publisher.connect(MqttVersion.MQTT_3_1_1, true, 60);
        publisher.publishWithRetain(TOPIC, "retain-message", MqttQoS.AT_LEAST_ONCE);
        publisher.waitPubAck();

        MqttClient subscriber = new MqttClient("retain-sub", port);
        subscriber.connect(MqttVersion.MQTT_3_1_1, true, 60);
        subscriber.subscribe(TOPIC, MqttQoS.AT_MOST_ONCE);

        MqttPublishMessage received = subscriber.awaitPublish(Duration.ofSeconds(5));
        assertNotNull(received);
        ByteBuf payload = received.payload();
        byte[] bytes = new byte[payload.readableBytes()];
        payload.readBytes(bytes);
        subscriber.ackPublish(received);
        ReferenceCountUtil.release(received);
        assertEquals("retain-message", new String(bytes));

        subscriber.close();
        publisher.close();
        bootstrap.shutdown();
    }

    /**
     * KeepAlive 触发断开测试。
     *
     * @throws Exception 测试异常
     */
    @Test
    void keepAliveTriggersDisconnect() throws Exception {
        int port = randomPort();
        MqttConfiguration config = createConfig(port);

        Bootstrap bootstrap = new Bootstrap(config, noopDispatcher());
        bootstrap.start().block();

        MqttClient client = new MqttClient("keepalive-client", port);
        client.connect(MqttVersion.MQTT_3_1_1, true, 1);
        boolean disconnected = client.waitForDisconnect(Duration.ofSeconds(4));
        assertEquals(true, disconnected);

        bootstrap.shutdown();
    }

    /**
     * QoS2 重复 PUBREL 不应重复投递测试。
     *
     * @throws Exception 测试异常
     */
    @Test
    void qos2DuplicatePubRelDoesNotDuplicateDelivery() throws Exception {
        int port = randomPort();
        MqttConfiguration config = createConfig(port);

        Bootstrap bootstrap = new Bootstrap(config, noopDispatcher());
        bootstrap.start().block();

        MqttClient subscriber = new MqttClient("sub-dup", port);
        subscriber.connect(MqttVersion.MQTT_3_1_1, true, 60);
        subscriber.subscribe(TOPIC, MqttQoS.EXACTLY_ONCE);

        MqttClient publisher = new MqttClient("pub-dup", port);
        publisher.connect(MqttVersion.MQTT_3_1_1, true, 60);
        int messageId = publisher.publish(TOPIC, "dup-qos2", MqttQoS.EXACTLY_ONCE);
        publisher.waitPublishAckFlow(MqttQoS.EXACTLY_ONCE);
        publisher.sendPubRel(messageId);

        MqttPublishMessage first = subscriber.awaitPublish(Duration.ofSeconds(5));
        assertNotNull(first);
        subscriber.ackPublish(first);
        ReferenceCountUtil.release(first);

        MqttPublishMessage second = subscriber.awaitPublish(Duration.ofSeconds(1));
        if (second != null) {
            ReferenceCountUtil.release(second);
        }
        assertEquals(null, second);

        publisher.close();
        subscriber.close();
        bootstrap.shutdown();
    }

    /**
     * 连接异常断开时遗嘱消息投递测试。
     *
     * @throws Exception 测试异常
     */
    @Test
    void willMessagePublishedOnUnexpectedDisconnect() throws Exception {
        int port = randomPort();
        MqttConfiguration config = createConfig(port);

        Bootstrap bootstrap = new Bootstrap(config, noopDispatcher());
        bootstrap.start().block();

        MqttClient subscriber = new MqttClient("will-sub", port);
        subscriber.connect(MqttVersion.MQTT_3_1_1, true, 60);
        subscriber.subscribe("will/topic", MqttQoS.AT_MOST_ONCE);

        MqttClient willClient = new MqttClient("will-pub", port);
        willClient.connectWithWill(MqttVersion.MQTT_3_1_1, true, 60, "will/topic", "will-payload", MqttQoS.AT_MOST_ONCE, false);
        willClient.closeAbrupt();

        MqttPublishMessage received = subscriber.awaitPublish(Duration.ofSeconds(5));
        assertNotNull(received);
        ByteBuf payload = received.payload();
        byte[] bytes = new byte[payload.readableBytes()];
        payload.readBytes(bytes);
        ReferenceCountUtil.release(received);
        assertEquals("will-payload", new String(bytes));

        subscriber.close();
        bootstrap.shutdown();
    }

    /**
     * 执行连接/订阅/发布链路。
     *
     * @param version MQTT 协议版本
     * @throws Exception 测试异常
     */
    private void runConnectSubscribePublish(MqttVersion version) throws Exception {
        int port = randomPort();
        MqttConfiguration config = createConfig(port);

        Bootstrap bootstrap = new Bootstrap(config, noopDispatcher());
        bootstrap.start().block();

        MqttClient subscriber = new MqttClient("sub-" + version.protocolLevel(), port);
        subscriber.connect(version, true, 60);
        subscriber.subscribe(TOPIC, MqttQoS.AT_MOST_ONCE);

        MqttClient publisher = new MqttClient("pub-" + version.protocolLevel(), port);
        publisher.connect(version, true, 60);
        publisher.publish(TOPIC, "hello-" + version.protocolLevel(), MqttQoS.AT_MOST_ONCE);

        MqttPublishMessage received = subscriber.awaitPublish(Duration.ofSeconds(5));
        assertNotNull(received);
        ByteBuf payload = received.payload();
        byte[] bytes = new byte[payload.readableBytes()];
        payload.readBytes(bytes);
        subscriber.ackPublish(received);
        ReferenceCountUtil.release(received);
        assertEquals("hello-" + version.protocolLevel(), new String(bytes));

        publisher.close();
        subscriber.close();
        bootstrap.shutdown();
    }

    /**
     * 执行指定 QoS 发布流程。
     *
     * @param qos QoS 等级
     * @throws Exception 测试异常
     */
    private void runPublishWithQos(MqttQoS qos) throws Exception {
        int port = randomPort();
        MqttConfiguration config = createConfig(port);

        Bootstrap bootstrap = new Bootstrap(config, noopDispatcher());
        bootstrap.start().block();

        MqttClient subscriber = new MqttClient("sub-qos-" + qos.value(), port);
        subscriber.connect(MqttVersion.MQTT_3_1_1, true, 60);
        subscriber.subscribe(TOPIC, qos);

        MqttClient publisher = new MqttClient("pub-qos-" + qos.value(), port);
        publisher.connect(MqttVersion.MQTT_3_1_1, true, 60);
        publisher.publish(TOPIC, "qos-" + qos.value(), qos);
        publisher.waitPublishAckFlow(qos);

        MqttPublishMessage received = subscriber.awaitPublish(Duration.ofSeconds(5));
        assertNotNull(received);
        ByteBuf payload = received.payload();
        byte[] bytes = new byte[payload.readableBytes()];
        payload.readBytes(bytes);
        subscriber.ackPublish(received);
        ReferenceCountUtil.release(received);
        assertEquals("qos-" + qos.value(), new String(bytes));

        publisher.close();
        subscriber.close();
        bootstrap.shutdown();
    }

    /**
     * 获取可用随机端口。
     *
     * @return 端口
     * @throws IOException IO 异常
     */
    private static int randomPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    /**
     * 创建测试配置。
     *
     * @param port 端口
     * @return MQTT 配置
     */
    private static MqttConfiguration createConfig(int port) {
        MqttConfiguration config = new MqttConfiguration();
        config.setSslEnable(false);
        config.setWiretap(false);
        config.setPort(port);
        config.setSecurePort(-1);
        config.setWebsocketPort(-1);
        config.setWebsocketSecurePort(-1);
        config.getClusterConfig().setNamespace("jmqx-test-" + UUID.randomUUID());
        return config;
    }

    /**
     * 构造空实现分发器。
     *
     * @return 分发器
     */
    private static PlatformDispatcher noopDispatcher() {
        return new PlatformDispatcher() {
            /**
             * 处理连接消息。
             *
             * @param message 连接消息
             * @return 处理结果
             */
            @Override
            public Mono<Void> onConnect(ConnectMessage message) {
                return Mono.empty();
            }

            /**
             * 处理断开消息。
             *
             * @param message 断开消息
             * @return 处理结果
             */
            @Override
            public Mono<Void> onDisconnect(DisconnectMessage message) {
                return Mono.empty();
            }

            /**
             * 处理连接丢失消息。
             *
             * @param message 连接丢失消息
             * @return 处理结果
             */
            @Override
            public Mono<Void> onConnectionLost(ConnectionLostMessage message) {
                return Mono.empty();
            }

            /**
             * 处理发布消息。
             *
             * @param message 发布消息
             * @return 处理结果
             */
            @Override
            public Mono<Void> onPublish(PublishMessage message) {
                return Mono.empty();
            }
        };
    }

    private static final class MqttClient {
        private final String clientId;
        private final int port;
        private Connection connection;
        private Flux<MqttMessage> inbound;
        private final ConcurrentLinkedQueue<MqttMessage> inbox = new ConcurrentLinkedQueue<>();
        private int packetId = 1;

        /**
         * 构造测试客户端。
         *
         * @param clientId 客户端 ID
         * @param port     端口
         */
        private MqttClient(String clientId, int port) {
            this.clientId = clientId;
            this.port = port;
        }

        /**
         * 连接并完成握手。
         *
         * @param version          MQTT 版本
         * @param cleanSession     清理会话
         * @param keepAliveSeconds KeepAlive 秒数
         */
        void connect(MqttVersion version, boolean cleanSession, int keepAliveSeconds) {
            this.connection = TcpClient.create()
                    .resolver(NoopAddressResolverGroup.INSTANCE)
                    .remoteAddress(() -> new InetSocketAddress("127.0.0.1", port))
                    .connectNow(Duration.ofSeconds(5));
            ensureMqttPipeline();

            this.inbound = connection.inbound()
                    .receiveObject()
                    .ofType(MqttMessage.class)
                    .publish()
                    .autoConnect();
            this.inbound.subscribe(this::onInbound);

            MqttMessage connect = connectMessage(version, clientId, cleanSession, keepAliveSeconds);
            writeAndFlush(connect);
            MqttConnAckMessage ack = (MqttConnAckMessage) awaitMessage(
                    msg -> msg instanceof MqttConnAckMessage,
                    Duration.ofSeconds(5)
            );
            assertNotNull(ack);
            assertEquals(MqttConnectReturnCode.CONNECTION_ACCEPTED, ack.variableHeader().connectReturnCode());
        }

        /**
         * 订阅主题。
         *
         * @param topic 主题
         * @param qos   QoS 等级
         */
        void subscribe(String topic, MqttQoS qos) {
            MqttMessage subscribe = MqttMessageBuilder.subMessage(
                    nextPacketId(),
                    Arrays.asList(new MqttTopicSubscription(topic, qos)));
            writeAndFlush(subscribe);
            MqttSubAckMessage ack = (MqttSubAckMessage) awaitMessage(
                    msg -> msg instanceof MqttSubAckMessage,
                    Duration.ofSeconds(5)
            );
            assertNotNull(ack);
        }

        /**
         * 发布消息。
         *
         * @param topic   主题
         * @param payload 负载
         * @param qos     QoS 等级
         * @return 消息 ID
         */
        int publish(String topic, String payload, MqttQoS qos) {
            byte[] bytes = payload.getBytes();
            MqttFixedHeader fixedHeader = new MqttFixedHeader(
                    MqttMessageType.PUBLISH,
                    false,
                    qos,
                    false,
                    0
            );
            int messageId = qos == MqttQoS.AT_MOST_ONCE ? 0 : nextPacketId();
            MqttPublishVariableHeader header = new MqttPublishVariableHeader(topic, messageId);
            MqttPublishMessage message = new MqttPublishMessage(fixedHeader, header, Unpooled.wrappedBuffer(bytes));
            writeAndFlush(message);
            return messageId;
        }

        /**
         * 发布保留消息。
         *
         * @param topic   主题
         * @param payload 负载
         * @param qos     QoS 等级
         */
        void publishWithRetain(String topic, String payload, MqttQoS qos) {
            byte[] bytes = payload.getBytes();
            MqttFixedHeader fixedHeader = new MqttFixedHeader(
                    MqttMessageType.PUBLISH,
                    false,
                    qos,
                    true,
                    0
            );
            int messageId = qos == MqttQoS.AT_MOST_ONCE ? 0 : nextPacketId();
            MqttPublishVariableHeader header = new MqttPublishVariableHeader(topic, messageId);
            MqttPublishMessage message = new MqttPublishMessage(fixedHeader, header, Unpooled.wrappedBuffer(bytes));
            writeAndFlush(message);
        }

        /**
         * 等待发布消息。
         *
         * @param timeout 超时时间
         * @return 发布消息
         */
        MqttPublishMessage awaitPublish(Duration timeout) {
            MqttMessage message = awaitMessage(msg -> msg instanceof MqttPublishMessage, timeout);
            return (MqttPublishMessage) message;
        }

        /**
         * 发送发布确认流程。
         *
         * @param message 发布消息
         */
        void ackPublish(MqttPublishMessage message) {
            MqttQoS qos = message.fixedHeader().qosLevel();
            if (qos == MqttQoS.AT_LEAST_ONCE) {
                int packetId = message.variableHeader().packetId();
                writeAndFlush(MqttMessageBuilder.publishAckMessage(packetId));
            } else if (qos == MqttQoS.EXACTLY_ONCE) {
                int packetId = message.variableHeader().packetId();
                writeAndFlush(MqttMessageBuilder.publishRecMessage(packetId));
                MqttMessage pubRel = awaitMessage(
                        msg -> msg.fixedHeader().messageType() == MqttMessageType.PUBREL,
                        Duration.ofSeconds(5)
                );
                if (pubRel != null) {
                    int relId = ((MqttMessageIdVariableHeader) pubRel.variableHeader()).messageId();
                    writeAndFlush(MqttMessageBuilder.publishCompMessage(relId));
                }
            }
        }

        /**
         * 等待发布 ACK 流程完成。
         *
         * @param qos QoS 等级
         */
        void waitPublishAckFlow(MqttQoS qos) {
            if (qos == MqttQoS.AT_LEAST_ONCE) {
                MqttMessage pubAck = awaitMessage(
                        msg -> msg.fixedHeader().messageType() == MqttMessageType.PUBACK,
                        Duration.ofSeconds(5)
                );
                assertNotNull(pubAck);
            } else if (qos == MqttQoS.EXACTLY_ONCE) {
                MqttMessage pubRec = awaitMessage(
                        msg -> msg.fixedHeader().messageType() == MqttMessageType.PUBREC,
                        Duration.ofSeconds(5)
                );
                assertNotNull(pubRec);
                int messageId = ((MqttMessageIdVariableHeader) pubRec.variableHeader()).messageId();
                writeAndFlush(MqttMessageBuilder.publishRelMessage(messageId));
                MqttMessage pubComp = awaitMessage(
                        msg -> msg.fixedHeader().messageType() == MqttMessageType.PUBCOMP,
                        Duration.ofSeconds(5)
                );
                assertNotNull(pubComp);
            }
        }

        /**
         * 等待 PUBACK。
         */
        void waitPubAck() {
            MqttMessage pubAck = awaitMessage(
                    msg -> msg.fixedHeader().messageType() == MqttMessageType.PUBACK,
                    Duration.ofSeconds(5)
            );
            assertNotNull(pubAck);
        }

        /**
         * 主动断开连接。
         */
        void disconnect() {
            MqttMessage disconnect = new MqttMessage(new MqttFixedHeader(MqttMessageType.DISCONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0));
            writeAndFlush(disconnect);
            close();
        }

        /**
         * 非正常断开连接。
         */
        void closeAbrupt() {
            if (connection != null && !connection.isDisposed()) {
                connection.disposeNow();
            }
        }

        /**
         * 关闭连接。
         */
        void close() {
            if (connection != null && !connection.isDisposed()) {
                connection.disposeNow();
            }
        }

        /**
         * 生成消息 ID。
         *
         * @return 消息 ID
         */
        private int nextPacketId() {
            return packetId++;
        }

        /**
         * 等待连接断开。
         *
         * @param timeout 超时时间
         * @return 是否断开
         */
        boolean waitForDisconnect(Duration timeout) {
            connection.onDispose().block(timeout);
            return connection.isDisposed();
        }

        /**
         * 发送 PUBREL 并等待 PUBCOMP。
         *
         * @param messageId 消息 ID
         */
        void sendPubRel(int messageId) {
            writeAndFlush(MqttMessageBuilder.publishRelMessage(messageId));
            MqttMessage pubComp = awaitMessage(
                    msg -> msg.fixedHeader().messageType() == MqttMessageType.PUBCOMP,
                    Duration.ofSeconds(5)
            );
            assertNotNull(pubComp);
        }

        /**
         * 建立带遗嘱的连接。
         *
         * @param version          MQTT 版本
         * @param cleanSession     清理会话
         * @param keepAliveSeconds KeepAlive 秒数
         * @param willTopic        遗嘱主题
         * @param willPayload      遗嘱负载
         * @param willQos          遗嘱 QoS
         * @param willRetain       遗嘱保留
         */
        void connectWithWill(MqttVersion version, boolean cleanSession, int keepAliveSeconds, String willTopic, String willPayload, MqttQoS willQos, boolean willRetain) {
            this.connection = TcpClient.create()
                    .resolver(NoopAddressResolverGroup.INSTANCE)
                    .remoteAddress(() -> new InetSocketAddress("127.0.0.1", port))
                    .connectNow(Duration.ofSeconds(5));
            ensureMqttPipeline();

            this.inbound = connection.inbound()
                    .receiveObject()
                    .ofType(MqttMessage.class)
                    .publish()
                    .autoConnect();
            this.inbound.subscribe(this::onInbound);

            MqttMessage connect = connectMessageWithWill(version, clientId, cleanSession, keepAliveSeconds, willTopic, willPayload, willQos, willRetain);
            writeAndFlush(connect);
            MqttConnAckMessage ack = (MqttConnAckMessage) awaitMessage(
                    msg -> msg instanceof MqttConnAckMessage,
                    Duration.ofSeconds(5)
            );
            assertNotNull(ack);
            assertEquals(MqttConnectReturnCode.CONNECTION_ACCEPTED, ack.variableHeader().connectReturnCode());
        }

        /**
         * 写入并刷新消息。
         *
         * @param message 消息
         */
        private void writeAndFlush(MqttMessage message) {
            connection.channel().writeAndFlush(message).syncUninterruptibly();
        }

        /**
         * 确保 MQTT 编解码器存在。
         */
        private void ensureMqttPipeline() {
            String bridgeName = "reactor.right.reactiveBridge";
            if (connection.channel().pipeline().get(bridgeName) != null) {
                if (connection.channel().pipeline().get(MqttEncoder.class) == null) {
                    connection.channel().pipeline().addBefore(bridgeName, "mqttEncoder", MqttEncoder.INSTANCE);
                }
                if (connection.channel().pipeline().get(MqttDecoder.class) == null) {
                    connection.channel().pipeline().addBefore(bridgeName, "mqttDecoder", new MqttDecoder(1024 * 1024));
                }
                if (connection.channel().pipeline().get("mqttRetainInbound") == null) {
                    connection.channel().pipeline().addAfter("mqttDecoder", "mqttRetainInbound", new ChannelInboundHandlerAdapter() {
                        /**
                         * 处理入站消息并保留引用。
                         *
                         * @param ctx 上下文
                         * @param msg 消息
                         * @throws Exception 处理异常
                         */
                        @Override
                        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                            if (msg instanceof ReferenceCounted) {
                                ((ReferenceCounted) msg).retain();
                            }
                            ctx.fireChannelRead(msg);
                        }
                    });
                }
                return;
            }
            if (connection.channel().pipeline().get(MqttEncoder.class) == null) {
                connection.channel().pipeline().addFirst("mqttEncoder", MqttEncoder.INSTANCE);
            }
            if (connection.channel().pipeline().get(MqttDecoder.class) == null) {
                connection.channel().pipeline().addLast("mqttDecoder", new MqttDecoder(1024 * 1024));
            }
            if (connection.channel().pipeline().get("mqttRetainInbound") == null) {
                connection.channel().pipeline().addAfter("mqttDecoder", "mqttRetainInbound", new ChannelInboundHandlerAdapter() {
                    /**
                     * 处理入站消息并保留引用。
                     *
                     * @param ctx 上下文
                     * @param msg 消息
                     * @throws Exception 处理异常
                     */
                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                        if (msg instanceof ReferenceCounted) {
                            ((ReferenceCounted) msg).retain();
                        }
                        ctx.fireChannelRead(msg);
                    }
                });
            }
        }

        /**
         * 复制发布消息并释放原消息。
         *
         * @param message 原消息
         * @return 复制消息
         */
        private MqttPublishMessage copyPublishMessage(MqttPublishMessage message) {
            try {
                ByteBuf payload = message.payload();
                ByteBuf copy = payload.copy();
                return new MqttPublishMessage(message.fixedHeader(), message.variableHeader(), copy);
            } finally {
                if (ReferenceCountUtil.refCnt(message) > 0) {
                    ReferenceCountUtil.release(message);
                }
            }
        }

        /**
         * 处理入站消息并入队。
         *
         * @param message 入站消息
         */
        private void onInbound(MqttMessage message) {
            if (message instanceof MqttPublishMessage) {
                inbox.add(copyPublishMessage((MqttPublishMessage) message));
            } else {
                inbox.add(message);
            }
        }

        /**
         * 等待匹配消息。
         *
         * @param predicate 过滤条件
         * @param timeout   超时时间
         * @return 消息
         */
        private MqttMessage awaitMessage(Predicate<MqttMessage> predicate, Duration timeout) {
            long deadline = System.nanoTime() + timeout.toNanos();
            while (System.nanoTime() < deadline) {
                for (Iterator<MqttMessage> it = inbox.iterator(); it.hasNext(); ) {
                    MqttMessage message = it.next();
                    if (predicate.test(message)) {
                        it.remove();
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
         * 构造 CONNECT 消息。
         *
         * @param version          MQTT 版本
         * @param clientId         客户端 ID
         * @param cleanSession     清理会话
         * @param keepAliveSeconds KeepAlive 秒数
         * @return CONNECT 消息
         */
        private static MqttConnectMessage connectMessage(MqttVersion version, String clientId, boolean cleanSession, int keepAliveSeconds) {
            MqttConnectVariableHeader header = new MqttConnectVariableHeader(
                    version.protocolName(),
                    version.protocolLevel(),
                    false,
                    false,
                    false,
                    0,
                    false,
                    cleanSession,
                    keepAliveSeconds
            );
            MqttConnectPayload payload = new MqttConnectPayload(
                    clientId,
                    (String) null,
                    (byte[]) null,
                    (String) null,
                    (byte[]) null
            );
            MqttFixedHeader fixedHeader = new MqttFixedHeader(
                    MqttMessageType.CONNECT,
                    false,
                    MqttQoS.AT_MOST_ONCE,
                    false,
                    10
            );
            return new MqttConnectMessage(fixedHeader, header, payload);
        }

        /**
         * 构造带遗嘱的 CONNECT 消息。
         *
         * @param version          MQTT 版本
         * @param clientId         客户端 ID
         * @param cleanSession     清理会话
         * @param keepAliveSeconds KeepAlive 秒数
         * @param willTopic        遗嘱主题
         * @param willPayload      遗嘱负载
         * @param willQos          遗嘱 QoS
         * @param willRetain       遗嘱保留
         * @return CONNECT 消息
         */
        private static MqttConnectMessage connectMessageWithWill(MqttVersion version,
                                                                String clientId,
                                                                boolean cleanSession,
                                                                int keepAliveSeconds,
                                                                String willTopic,
                                                                String willPayload,
                                                                MqttQoS willQos,
                                                                boolean willRetain) {
            MqttConnectVariableHeader header = new MqttConnectVariableHeader(
                    version.protocolName(),
                    version.protocolLevel(),
                    false,
                    false,
                    willRetain,
                    willQos.value(),
                    true,
                    cleanSession,
                    keepAliveSeconds
            );
            MqttConnectPayload payload = new MqttConnectPayload(
                    clientId,
                    willTopic,
                    willPayload == null ? null : willPayload.getBytes(),
                    (String) null,
                    (byte[]) null
            );
            MqttFixedHeader fixedHeader = new MqttFixedHeader(
                    MqttMessageType.CONNECT,
                    false,
                    MqttQoS.AT_MOST_ONCE,
                    false,
                    10
            );
            return new MqttConnectMessage(fixedHeader, header, payload);
        }
    }
}
