package plus.jmqx.broker.support;

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
import java.util.function.Predicate;

/**
 * 海量连接压测用 MQTT 客户端：仅保持在线，不发布/订阅，但遵循正常 MQTT 行为
 * （clean session、keepalive PINGREQ/PINGRESP、优雅 DISCONNECT）。
 */
public class MqttKeepaliveClient {

    /**
     * 海量压测共享连接池（默认池 maxConnections 过小会导致 PoolAcquireTimeoutException）
     */
    private static final ConnectionProvider CLIENT_POOL = ConnectionProvider.builder("jmqx-massive-client")
            .maxConnections(20_000)
            .pendingAcquireMaxCount(-1)
            .pendingAcquireTimeout(Duration.ofMinutes(3))
            .build();

    private final    String                             clientId;
    private final    int                                port;
    private final    int                                keepAliveSeconds;
    private          Connection                         connection;
    private volatile ScheduledFuture<?>                 pingFuture;
    private final    ConcurrentLinkedQueue<MqttMessage> inbox = new ConcurrentLinkedQueue<>();

    public MqttKeepaliveClient(String clientId, int port) {
        this(clientId, port, 60);
    }

    public MqttKeepaliveClient(String clientId, int port, int keepAliveSeconds) {
        this.clientId = clientId;
        this.port = port;
        this.keepAliveSeconds = keepAliveSeconds;
    }

    public String getClientId() {
        return clientId;
    }

    public int getPort() {
        return port;
    }

    public boolean isActive() {
        return connection != null && !connection.isDisposed() && connection.channel().isActive();
    }

    /**
     * 建立 TCP 连接并完成 MQTT 握手，随后启动 keepalive 定时 PING。
     */
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
            throw new IllegalStateException("connect ack timeout for [" + clientId + "]");
        }
        MqttConnectReturnCode returnCode = ack.variableHeader().connectReturnCode();
        if (returnCode != MqttConnectReturnCode.CONNECTION_ACCEPTED) {
            throw new ConnectionRejectedException(
                    "connection [" + clientId + "] rejected: " + returnCode);
        }
        startKeepalive();
    }

    /**
     * 发送 DISCONNECT 后关闭 TCP 连接。
     */
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
            // 连接可能已断开
        } finally {
            if (!connection.isDisposed()) {
                connection.disposeNow();
            }
        }
    }

    private void startKeepalive() {
        // MQTT 规范：keepalive 周期内无其它报文则发 PINGREQ，取 3/4 周期留余量
        long intervalMs = Math.max(1000L, keepAliveSeconds * 1000L * 3 / 4);
        pingFuture = connection.channel().eventLoop().scheduleAtFixedRate(() -> {
            try {
                if (isActive()) {
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

    private void onInbound(MqttMessage message) {
        switch (message.fixedHeader().messageType()) {
            case PINGRESP:
            case PUBACK:
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

    public static final class ConnectionRejectedException extends RuntimeException {
        public ConnectionRejectedException(String message) {
            super(message);
        }
    }
}
