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
import java.util.function.Function;

/**
 * 选择 reactor-netty 传输（TCP/TLS/WS/WSS）并安装 MQTT pipeline。
 *
 * <p>{@code DefaultMqttClient} 与传输无关 —— 它只看到一个 {@link Connection}。
 *
 * @author maxid
 */
@Slf4j
public final class TransportFactory {

    @SuppressWarnings("unchecked")
    public Mono<Connection> connect(MqttClientConfig config,
                                     Function<MqttClientConfig, MqttClientHandler> handlerFactory) {
        Mono<? extends Connection> mono = switch (config.getTransportType()) {
            case TCP -> tcpClient(config, handlerFactory).connect();
            case TLS -> tcpClient(config, handlerFactory).secure().connect();
            case WS -> httpClient(config, handlerFactory).websocket().uri(wsUri(config)).connect();
            case WSS -> httpClient(config, handlerFactory).secure().websocket().uri(wsUri(config)).connect();
        };
        return (Mono<Connection>) mono;
    }

    private TcpClient tcpClient(MqttClientConfig c, Function<MqttClientConfig, MqttClientHandler> hf) {
        TcpClient client = TcpClient.newConnection()
                .host(c.getServerHost())
                .port(c.getServerPort())
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, c.getSocketConnectTimeoutMs())
                .option(ChannelOption.TCP_NODELAY, true);
        if (c.getLoopResources() != null) {
            client = client.runOn(c.getLoopResources());
        }
        return client.doOnConnected(conn -> installPipeline(conn, hf.apply(c), c));
    }

    private HttpClient httpClient(MqttClientConfig c, Function<MqttClientConfig, MqttClientHandler> hf) {
        HttpClient client = HttpClient.newConnection()
                .host(c.getServerHost())
                .port(c.getServerPort());
        if (c.getLoopResources() != null) {
            client = client.runOn(c.getLoopResources());
        }
        return client.doOnConnected(conn -> installPipeline(conn, hf.apply(c), c));
    }

    private void installPipeline(Connection conn, MqttClientHandler handler, MqttClientConfig c) {
        int keepAlive = c.getKeepAliveSeconds();
        conn.channel().pipeline()
                .addFirst("mqttDecoder", new MqttDecoder(8 * 1024 * 1024))
                .addAfter("mqttDecoder", "mqttEncoder", MqttEncoder.INSTANCE)
                .addAfter("mqttEncoder", "idle",
                        new IdleStateHandler((long) (keepAlive * 1.5), keepAlive, 0, TimeUnit.SECONDS))
                .addAfter("idle", "mqttClient", handler);
        log.debug("MQTT pipeline installed on {}", conn.channel());
    }

    private String wsUri(MqttClientConfig c) {
        MqttWebSocketConfig ws = c.getWebSocketConfig();
        return ws != null && ws.getPath() != null ? ws.getPath() : "/mqtt";
    }
}
