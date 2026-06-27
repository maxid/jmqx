package plus.jmqx.broker.support;

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
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.netty.resolver.NoopAddressResolverGroup;
import plus.jmqx.broker.mqtt.message.MqttMessageBuilder;
import reactor.netty.Connection;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.tcp.TcpClient;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

/**
 * 高吞吐 MQTT 压测客户端：共享连接池、cleanSession、keepalive 保活、QoS1 发布循环。
 */
public class MqttStressClient {

    private static final int DEFAULT_KEEP_ALIVE_SECONDS = 60;
    private static final ConnectionProvider CLIENT_POOL = ConnectionProvider.builder("jmqx-stress-client")
            .maxConnections(20_000)
            .pendingAcquireMaxCount(-1)
            .pendingAcquireTimeout(Duration.ofMinutes(3))
            .build();

    private final String clientId;
    private final int port;
    private final AtomicLong ackedCounter;
    private final int keepAliveSeconds;
    private Connection connection;
    private volatile ScheduledFuture<?> pingFuture;
    private final ConcurrentLinkedQueue<MqttMessage> inbox = new ConcurrentLinkedQueue<>();
    private int packetId = 1;
    private final AtomicLong localAcked = new AtomicLong();

    public MqttStressClient(String clientId, int port, AtomicLong ackedCounter) {
        this(clientId, port, ackedCounter, DEFAULT_KEEP_ALIVE_SECONDS);
    }

    public MqttStressClient(String clientId, int port, AtomicLong ackedCounter, int keepAliveSeconds) {
        this.clientId = clientId;
        this.port = port;
        this.ackedCounter = ackedCounter;
        this.keepAliveSeconds = keepAliveSeconds;
    }

    public void connect() {
        connect(5);
    }

    public void connect(int timeoutSeconds) {
        this.connection = TcpClient.create(CLIENT_POOL)
                .resolver(NoopAddressResolverGroup.INSTANCE)
                .remoteAddress(() -> new InetSocketAddress("127.0.0.1", port))
                .connectNow(Duration.ofSeconds(timeoutSeconds));
        ensureMqttPipeline();

        writeAndFlush(cleanSessionConnect(clientId, keepAliveSeconds));
        connection.inbound()
                .receiveObject()
                .ofType(MqttMessage.class)
                .subscribe(this::onInbound);
        MqttConnAckMessage ack = (MqttConnAckMessage) awaitMessage(
                msg -> msg instanceof MqttConnAckMessage,
                Duration.ofSeconds(timeoutSeconds));
        if (ack == null) {
            throw new IllegalStateException("connect ack timeout for " + clientId);
        }
        if (ack.variableHeader().connectReturnCode() != MqttConnectReturnCode.CONNECTION_ACCEPTED) {
            throw new IllegalStateException("connect rejected for " + clientId + ": "
                    + ack.variableHeader().connectReturnCode());
        }
        startKeepalive();
    }

    public long publishLoop(String topic, int payloadBytes, int durationSeconds,
                            int flushEvery, int inFlightLimit, AtomicLong published) {
        byte[] payload = new byte[payloadBytes];
        long endAt = System.nanoTime() + TimeUnit.SECONDS.toNanos(durationSeconds);
        int i = 0;
        long localSent = 0;
        while (System.nanoTime() < endAt) {
            if (connection == null || !connection.channel().isActive()) {
                break;
            }
            while ((localSent - localAcked.get()) >= inFlightLimit) {
                if (!safeFlush()) {
                    break;
                }
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            while (!connection.channel().isWritable()) {
                if (!safeFlush()) {
                    break;
                }
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
            MqttPublishMessage message = new MqttPublishMessage(
                    fixedHeader, header, Unpooled.wrappedBuffer(payload));
            if (!safeWrite(message)) {
                break;
            }
            if (flushEvery > 0 && (i + 1) % flushEvery == 0) {
                if (!safeFlush()) {
                    break;
                }
            }
            published.incrementAndGet();
            i++;
            localSent++;
        }
        safeFlush();
        return localSent;
    }

    public void awaitAcks(long expected, int timeoutSeconds) {
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

    public boolean publishOnceWaitAck(String topic, int payloadBytes, int timeoutSeconds) {
        byte[] payload = new byte[payloadBytes];
        MqttFixedHeader fixedHeader = new MqttFixedHeader(
                MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false, 0);
        int messageId = nextPacketId();
        long beforeAck = localAcked.get();
        MqttPublishVariableHeader header = new MqttPublishVariableHeader(topic, messageId);
        MqttPublishMessage message = new MqttPublishMessage(
                fixedHeader, header, Unpooled.wrappedBuffer(payload));
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

    public void close() {
        stopKeepalive();
        if (connection == null || connection.isDisposed()) {
            return;
        }
        try {
            if (connection.channel().isActive()) {
                writeAndFlush(new MqttMessage(new MqttFixedHeader(
                        MqttMessageType.DISCONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0)));
            }
        } catch (Exception ignored) {
        } finally {
            if (!connection.isDisposed()) {
                connection.disposeNow();
            }
        }
    }

    private void startKeepalive() {
        long intervalMs = Math.max(1000L, keepAliveSeconds * 1000L * 3 / 4);
        pingFuture = connection.channel().eventLoop().scheduleAtFixedRate(() -> {
            try {
                if (connection != null && connection.channel().isActive()) {
                    writeAndFlush(MqttMessageBuilder.pingMessage());
                }
            } catch (Exception ignored) {
                stopKeepalive();
            }
        }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    private void stopKeepalive() {
        ScheduledFuture<?> future = pingFuture;
        if (future != null) {
            future.cancel(false);
            pingFuture = null;
        }
    }

    private boolean safeFlush() {
        try {
            if (connection != null && connection.channel().isActive()) {
                submitFlush();
                return true;
            }
        } catch (Exception ignored) {
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
        } catch (Exception ignored) {
            // channel closed
        }
        return false;
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
                connection.channel().pipeline()
                        .addBefore(bridgeName, "mqttEncoder", MqttEncoder.INSTANCE);
            }
            if (connection.channel().pipeline().get(MqttDecoder.class) == null) {
                connection.channel().pipeline()
                        .addBefore(bridgeName, "mqttDecoder", new MqttDecoder(1024 * 1024));
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
        switch (message.fixedHeader().messageType()) {
            case PUBACK:
                ackedCounter.incrementAndGet();
                localAcked.incrementAndGet();
                return;
            case PINGRESP:
                return;
            case PINGREQ:
                writeAndFlush(MqttMessageBuilder.pongMessage());
                return;
            default:
                inbox.add(message);
        }
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

    private static MqttConnectMessage cleanSessionConnect(String clientId, int keepAliveSeconds) {
        MqttConnectVariableHeader variableHeader = new MqttConnectVariableHeader(
                MqttVersion.MQTT_3_1_1.protocolName(),
                MqttVersion.MQTT_3_1_1.protocolLevel(),
                false, false, false, 0, false, true, keepAliveSeconds);
        MqttConnectPayload payload = new MqttConnectPayload(
                clientId, null, (byte[]) null, null, (byte[]) null);
        MqttFixedHeader fixedHeader = new MqttFixedHeader(
                MqttMessageType.CONNECT, false, MqttQoS.AT_MOST_ONCE, false, 10);
        return new MqttConnectMessage(fixedHeader, variableHeader, payload);
    }
}
