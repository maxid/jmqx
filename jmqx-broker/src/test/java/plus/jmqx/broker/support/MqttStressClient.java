package plus.jmqx.broker.support;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.resolver.NoopAddressResolverGroup;
import plus.jmqx.broker.mqtt.message.MqttMessageBuilder;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpClient;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

/**
 * 高吞吐 MQTT 压测客户端：连接、QoS1 发布循环、飞行窗口与 PUBACK 跟踪。
 */
public class MqttStressClient {

    private final String clientId;
    private final int port;
    private final AtomicLong ackedCounter;
    private Connection connection;
    private final ConcurrentLinkedQueue<MqttMessage> inbox = new ConcurrentLinkedQueue<>();
    private int packetId = 1;
    private final AtomicLong localAcked = new AtomicLong();

    public MqttStressClient(String clientId, int port, AtomicLong ackedCounter) {
        this.clientId = clientId;
        this.port = port;
        this.ackedCounter = ackedCounter;
    }

    public void connect() {
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
