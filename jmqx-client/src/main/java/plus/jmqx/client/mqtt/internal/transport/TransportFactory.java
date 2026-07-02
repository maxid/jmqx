package plus.jmqx.client.mqtt.internal.transport;

import io.netty.channel.ChannelOption;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.extern.slf4j.Slf4j;
import plus.jmqx.client.mqtt.MqttClientConfig;
import plus.jmqx.client.mqtt.internal.handler.MqttClientHandler;
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

    @SuppressWarnings("unchecked")
    public Mono<Connection> connect(MqttClientConfig config) {
        Mono<? extends Connection> mono = switch (config.getTransportType()) {
            case TCP -> tcpClient(config).connect();
            case TLS -> tcpClient(config).secure().connect();
            case WS -> httpClient(config).websocket().uri(wsUri(config)).connect();
            case WSS -> httpClient(config).secure().websocket().uri(wsUri(config)).connect();
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

    /**
     * 在已建立的 reactor-netty {@link Connection} 上安装 MQTT pipeline。
     *
     * <p>顺序（head→tail）：{@code idle → mqttDecoder → mqttEncoder → mqttClient → reactor handlers}。
     * 出站（tail→head）：reactiveBridge → mqttClient（业务可写出 MqttMessage）
     * → mqttEncoder（编码为 ByteBuf）→ mqttDecoder（透传）→ idle → head。
     *
     * @param conn    reactor-netty 连接
     * @param handler MQTT 业务处理器
     * @param config  客户端配置
     */
    public void installPipeline(Connection conn, MqttClientHandler handler, MqttClientConfig config) {
        int keepAlive = config.getKeepAliveSeconds();
        // 使用 Connection API 挂载（reactor-netty 会插入到 reactiveBridge 之前），与 broker 同构
        conn.addHandlerFirst("mqttEncoder", MqttEncoder.INSTANCE)
                .addHandlerFirst("mqttDecoder", new MqttDecoder(8 * 1024 * 1024))
                .addHandlerFirst("idle",
                        new IdleStateHandler((long) (keepAlive * 1.5), keepAlive, 0, TimeUnit.SECONDS))
                .addHandlerFirst("mqttClient", handler);
        log.debug("MQTT pipeline installed on {}", conn.channel());
    }

    private String wsUri(MqttClientConfig c) {
        MqttWebSocketConfig ws = c.getWebSocketConfig();
        return ws != null && ws.getPath() != null ? ws.getPath() : "/mqtt";
    }

}
