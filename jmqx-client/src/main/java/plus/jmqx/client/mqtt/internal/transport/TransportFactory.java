package plus.jmqx.client.mqtt.internal.transport;

import io.netty.channel.ChannelOption;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.extern.slf4j.Slf4j;
import plus.jmqx.client.mqtt.MqttClientConfig;
import plus.jmqx.client.mqtt.internal.handler.MqttClientHandler;
import plus.jmqx.client.mqtt.internal.transport.ws.ByteBufToWebSocketFrameEncoder;
import plus.jmqx.client.mqtt.internal.transport.ws.WebSocketFrameToByteBufDecoder;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.http.client.HttpClient;
import reactor.netty.tcp.TcpClient;

import java.util.concurrent.TimeUnit;

/**
 * 选择 reactor-netty 传输（TCP/TLS/WS/WSS）。
 *
 * <p>{@code DefaultMqttClient} 与传输无关 —— 它只看到一个 {@link Connection}。
 * MQTT 编解码 pipeline 通过 {@link #installPipeline} 在连接建立后显式挂载，
 * 使用 reactor-netty {@code Connection.addHandlerFirst/addHandlerLast} API
 * （与 jmqx-broker 的 {@code MqttReceiver} 同构），确保编码器在 reactor-netty
 * 的 reactiveBridge 之前对出站消息生效。
 *
 * @author maxid
 */
@Slf4j
public final class TransportFactory {

    /**
     * 根据配置创建连接。
     *
     * @param config 客户端配置（决定 TCP/TLS/WS/WSS）
     * @return 连接 Mono
     */
    @SuppressWarnings("unchecked")
    public Mono<Connection> connect(MqttClientConfig config) {
        Mono<? extends Connection> mono = switch (config.getTransportType()) {
            case TCP -> tcpClient(config).connect();
            case TLS -> applyTlsTcp(tcpClient(config), config).connect();
            case WS -> httpClient(config).websocket().uri(wsUri(config)).connect();
            case WSS -> applyTlsHttp(httpClient(config), config).websocket().uri(wsUri(config)).connect();
        };
        return (Mono<Connection>) mono;
    }

    private TcpClient tcpClient(MqttClientConfig c) {
        TcpClient client = TcpClient.newConnection()
                .host(c.getServerHost())
                .port(c.getServerPort())
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, c.getSocketConnectTimeoutMs())
                .option(ChannelOption.TCP_NODELAY, true);
        if (c.getLoopResources() != null) {
            client = client.runOn(c.getLoopResources());
        }
        return client;
    }

    private HttpClient httpClient(MqttClientConfig c) {
        HttpClient client = HttpClient.newConnection()
                .host(c.getServerHost())
                .port(c.getServerPort());
        if (c.getLoopResources() != null) {
            client = client.runOn(c.getLoopResources());
        }
        return client;
    }

    private TcpClient applyTlsTcp(TcpClient client, MqttClientConfig config) {
        SslContext sslContext = buildClientSslContext(config.getSslConfig());
        return client.secure(spec -> spec.sslContext(sslContext));
    }

    private HttpClient applyTlsHttp(HttpClient client, MqttClientConfig config) {
        SslContext sslContext = buildClientSslContext(config.getSslConfig());
        return client.secure(spec -> spec.sslContext(sslContext));
    }

    private SslContext buildClientSslContext(MqttSslConfig ssl) {
        try {
            SslContextBuilder builder = SslContextBuilder.forClient();
            if (ssl != null && ssl.isInsecureTrustAll()) {
                builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
            }
            return builder.build();
        } catch (Exception e) {
            throw new IllegalStateException("failed to build client SSL context", e);
        }
    }

    /**
     * 在已建立的 reactor-netty {@link Connection} 上安装 MQTT pipeline。
     *
     * <p>与 jmqx-broker 测试客户端同构：{@code mqttClient → idle → mqttDecoder → mqttEncoder → reactiveBridge}。
     * 入站经 reactiveBridge 解码后由 {@link MqttClientHandler#handleInbound} 分发；
     * 出站经 {@link plus.jmqx.client.mqtt.internal.NettyUtil#writeAndFlush} 写出 {@code MqttMessage}。
     *
     * @param conn    reactor-netty 连接
     * @param handler MQTT 业务处理器
     * @param config  客户端配置
     */
    public void installPipeline(Connection conn, MqttClientHandler handler, MqttClientConfig config) {
        int keepAlive = config.getKeepAliveSeconds();
        var pipeline = conn.channel().pipeline();
        String bridge = pipeline.get("reactor.right.reactiveBridge") != null
                ? "reactor.right.reactiveBridge"
                : "reactor.left.reactiveBridge";
        if (pipeline.get(bridge) != null) {
            if (pipeline.get(MqttEncoder.class) == null) {
                pipeline.addBefore(bridge, "mqttEncoder", MqttEncoder.INSTANCE);
            }
            if (pipeline.get(MqttDecoder.class) == null) {
                pipeline.addBefore(bridge, "mqttDecoder", new MqttDecoder(8 * 1024 * 1024));
            }
            if (pipeline.get("idle") == null) {
                pipeline.addBefore(bridge, "idle",
                        new IdleStateHandler((long) (keepAlive * 1.5), keepAlive, 0, TimeUnit.SECONDS));
            }
            if (pipeline.get("mqttClient") == null) {
                pipeline.addBefore(bridge, "mqttClient", handler);
            }
        } else {
            pipeline.addFirst("mqttDecoder", new MqttDecoder(8 * 1024 * 1024))
                    .addAfter("mqttDecoder", "mqttEncoder", MqttEncoder.INSTANCE)
                    .addAfter("mqttEncoder", "idle",
                            new IdleStateHandler((long) (keepAlive * 1.5), keepAlive, 0, TimeUnit.SECONDS))
                    .addAfter("idle", "mqttClient", handler);
        }
        if (isWebSocket(config)) {
            installWebSocketFraming(pipeline);
        }
        log.debug("MQTT pipeline installed on {}", conn.channel());
    }

    private String wsUri(MqttClientConfig c) {
        MqttWebSocketConfig ws = c.getWebSocketConfig();
        return ws != null && ws.getPath() != null ? ws.getPath() : "/mqtt";
    }

    private static boolean isWebSocket(MqttClientConfig config) {
        MqttClientConfig.TransportType type = config.getTransportType();
        return type == MqttClientConfig.TransportType.WS || type == MqttClientConfig.TransportType.WSS;
    }

    private static void installWebSocketFraming(io.netty.channel.ChannelPipeline pipeline) {
        if (pipeline.get("ws-decoder") != null && pipeline.get("mqttWsFrameDecoder") == null) {
            pipeline.addAfter("ws-decoder", "mqttWsFrameDecoder", new WebSocketFrameToByteBufDecoder());
        }
        if (pipeline.get("mqttEncoder") != null && pipeline.get("mqttWsFrameEncoder") == null) {
            pipeline.addBefore("mqttEncoder", "mqttWsFrameEncoder", new ByteBufToWebSocketFrameEncoder());
        }
    }

}
