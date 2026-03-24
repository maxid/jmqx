package plus.jmqx.broker.mqtt.benchmark;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.resolver.NoopAddressResolverGroup;
import io.netty.util.ReferenceCounted;
import io.netty.util.ReferenceCountUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import plus.jmqx.broker.Bootstrap;
import plus.jmqx.broker.mqtt.MqttConfiguration;
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

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * 真实连接压测：基于 TCP+MQTT 协议进行发布/订阅性能对比。
 */
@EnabledIfSystemProperty(named = "jmqx.benchmark.tests", matches = "true")
class MqttLoadBenchmarkTest {

    private static final String TOPIC = "bench/topic";

    @Test
    void benchmarkPublishToSubscribers() throws Exception {
        int subscribers = intProp("bench.subscribers", 10);
        int publishers = intProp("bench.publishers", 1);
        int messages = intProp("bench.messages", 5000);
        int payloadBytes = intProp("bench.payloadBytes", 32);
        int port = intProp("bench.port", 18830);

        MqttConfiguration config = createConfig(port);

        Bootstrap bootstrap = new Bootstrap(config, noopDispatcher());
        bootstrap.start().block();

        int actualPort = config.getPort();
        List<MqttTestClient> subClients = new ArrayList<>();
        for (int i = 0; i < subscribers; i++) {
            MqttTestClient client = new MqttTestClient("sub-" + i, actualPort);
            client.connect();
            client.subscribe(TOPIC);
            subClients.add(client);
        }

        CountDownLatch receiveLatch = new CountDownLatch(subscribers * messages);
        subClients.forEach(client -> client.onPublish(receiveLatch::countDown));

        List<MqttTestClient> pubClients = new ArrayList<>();
        for (int i = 0; i < publishers; i++) {
            MqttTestClient client = new MqttTestClient("pub-" + i, actualPort);
            client.connect();
            pubClients.add(client);
        }

        long start = System.nanoTime();
        for (int i = 0; i < messages; i++) {
            MqttTestClient client = pubClients.get(i % publishers);
            client.publish(TOPIC, payloadBytes);
        }

        boolean ok = receiveLatch.await(60, TimeUnit.SECONDS);
        long end = System.nanoTime();

        pubClients.forEach(MqttTestClient::close);
        subClients.forEach(MqttTestClient::close);
        bootstrap.shutdown();

        if (!ok) {
            throw new IllegalStateException("benchmark did not finish in time");
        }

        double seconds = (end - start) / 1_000_000_000.0;
        double throughput = (double) messages / seconds;
        System.out.printf("MQTT load benchmark: subs=%d, pubs=%d, messages=%d, payload=%dB, time=%.3fs, throughput=%.0f msg/s%n",
                subscribers, publishers, messages, payloadBytes, seconds, throughput);
    }

    private static int intProp(String key, int def) {
        String value = System.getProperty(key);
        if (value == null || value.isEmpty()) {
            return def;
        }
        return Integer.parseInt(value);
    }

    private static PlatformDispatcher noopDispatcher() {
        return new PlatformDispatcher() {
            @Override
            public Mono<Void> onConnect(ConnectMessage message) {
                return Mono.empty();
            }

            @Override
            public Mono<Void> onDisconnect(DisconnectMessage message) {
                return Mono.empty();
            }

            @Override
            public Mono<Void> onConnectionLost(ConnectionLostMessage message) {
                return Mono.empty();
            }

            @Override
            public Mono<Void> onPublish(PublishMessage message) {
                return Mono.empty();
            }
        };
    }

    private static MqttConfiguration createConfig(int port) {
        MqttConfiguration config = new MqttConfiguration();
        config.setSslEnable(false);
        config.setWiretap(false);
        config.setPort(port);
        config.setSecurePort(-1);
        config.setWebsocketPort(-1);
        config.setWebsocketSecurePort(-1);
        config.getClusterConfig().setNamespace("jmqx-bench-" + UUID.randomUUID());
        return config;
    }

    private static final class MqttTestClient {
        private final String clientId;
        private final int port;
        private Connection connection;
        private Flux<MqttMessage> inbound;
        private final ConcurrentLinkedQueue<MqttMessage> inbox = new ConcurrentLinkedQueue<>();
        private int packetId = 1;
        private volatile Runnable publishHook;

        private MqttTestClient(String clientId, int port) {
            this.clientId = clientId;
            this.port = port;
        }

        void connect() {
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
            MqttConnAckMessage ack = (MqttConnAckMessage) awaitMessage(
                    msg -> msg instanceof MqttConnAckMessage,
                    Duration.ofSeconds(5)
            );
            if (ack == null) {
                throw new IllegalStateException("connect ack timeout");
            }
        }

        void subscribe(String topic) {
            MqttSubscribeMessage subscribe = MqttMessageBuilder.subMessage(
                    nextPacketId(),
                    Arrays.asList(new MqttTopicSubscription(topic, MqttQoS.AT_MOST_ONCE)));
            writeAndFlush(subscribe);
            MqttSubAckMessage ack = (MqttSubAckMessage) awaitMessage(
                    msg -> msg instanceof MqttSubAckMessage,
                    Duration.ofSeconds(5)
            );
            if (ack == null) {
                throw new IllegalStateException("subscribe ack timeout");
            }
        }

        void publish(String topic, int payloadBytes) {
            byte[] payload = new byte[payloadBytes];
            MqttFixedHeader fixedHeader = new MqttFixedHeader(
                    MqttMessageType.PUBLISH,
                    false,
                    MqttQoS.AT_MOST_ONCE,
                    false,
                    0
            );
            MqttPublishVariableHeader header = new MqttPublishVariableHeader(topic, 0);
            MqttPublishMessage message = new MqttPublishMessage(fixedHeader, header, Unpooled.wrappedBuffer(payload));
            writeAndFlush(message);
        }

        void onPublish(Runnable action) {
            this.publishHook = action;
        }

        void close() {
            if (connection != null && !connection.isDisposed()) {
                connection.disposeNow();
            }
        }

        private int nextPacketId() {
            return packetId++;
        }

        private void writeAndFlush(MqttMessage message) {
            connection.channel().writeAndFlush(message).syncUninterruptibly();
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
                if (connection.channel().pipeline().get("mqttRetainInbound") == null) {
                    connection.channel().pipeline().addAfter("mqttDecoder", "mqttRetainInbound", new ChannelInboundHandlerAdapter() {
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

        private void onInbound(MqttMessage message) {
            try {
                if (message instanceof MqttPublishMessage) {
                    Runnable hook = publishHook;
                    if (hook != null) {
                        hook.run();
                    }
                    return;
                }
                inbox.add(message);
            } finally {
                if (ReferenceCountUtil.refCnt(message) > 0) {
                    ReferenceCountUtil.release(message);
                }
            }
        }

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
    }
}
