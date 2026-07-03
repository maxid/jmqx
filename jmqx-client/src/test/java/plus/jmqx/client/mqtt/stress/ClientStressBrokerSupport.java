package plus.jmqx.client.mqtt.stress;

import org.junit.jupiter.api.BeforeAll;
import plus.jmqx.client.mqtt.MqttClient;
import plus.jmqx.client.mqtt.MqttClientConfig;
import plus.jmqx.client.mqtt.internal.transport.MqttSslConfig;
import plus.jmqx.client.mqtt.internal.transport.MqttWebSocketConfig;
import plus.jmqx.client.mqtt.v3.Mqtt3AsyncClient;
import plus.jmqx.client.mqtt.v3.Mqtt3ClientBuilder;
import plus.jmqx.client.mqtt.v3.Mqtt3RxClient;
import plus.jmqx.client.mqtt.v5.Mqtt5AsyncClient;
import plus.jmqx.client.mqtt.v5.Mqtt5ClientBuilder;
import plus.jmqx.client.mqtt.v5.Mqtt5RxClient;

import java.time.Duration;

/**
 * 压力测试公共基类：连接用户自备的外部 MQTT broker，不启动内嵌 jmqx-broker。
 *
 * <p>运行前须自行启动 broker（如 jmqx-broker、EMQX 等）。传输与端口默认与 jmqx-broker 一致：
 * TCP {@code 1883}、MQTTS {@code 8883}、WS {@code 1884}、WSS {@code 8884}。
 *
 * <p>通过 {@code -Djmqx.client.stress.transport=tcp|mqtts|ws|wss} 选择传输层；
 * 主机与端口可用 {@code jmqx.client.stress.broker.*} 覆盖（亦兼容 {@code jmqx.it.broker.*}）。
 * 认证可用 {@code jmqx.client.stress.broker.username} / {@code broker.password}。
 */
public abstract class ClientStressBrokerSupport {

    protected static final Duration TIMEOUT = Duration.ofSeconds(10);

    @BeforeAll
    static void verifyExternalBroker() {
        ClientStressTransport transport = ClientStressSupport.transport();
        String host = brokerHost();
        int port = brokerPort();
        Mqtt3RxClient probe = v3Rx("stress-probe-" + System.nanoTime());
        try {
            var ack = probe.connect().block(TIMEOUT);
            if (ack == null || !ack.getReturnCode().isAccepted()) {
                throw new IllegalStateException("broker at " + host + ":" + port + " rejected connection");
            }
            probe.disconnect().block(TIMEOUT);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "MQTT broker not reachable at " + transport.label() + "://" + host + ":" + port
                            + ". Start a broker with the matching listener before stress tests. "
                            + "Use -Djmqx.client.stress.transport=tcp|mqtts|ws|wss and "
                            + "-Djmqx.client.stress.broker.host=... / broker.port|securePort|websocketPort|websocketSecurePort=... "
                            + "(optional broker.username / broker.password for auth)",
                    e);
        }
    }

    protected static ClientStressTransport transport() {
        return ClientStressSupport.transport();
    }

    protected static String brokerHost() {
        return ClientStressSupport.brokerHost();
    }

    protected static int brokerPort() {
        return ClientStressSupport.brokerPort();
    }

    protected static MqttSslConfig stressSslConfig() {
        return MqttSslConfig.builder().insecureTrustAll(true).build();
    }

    protected static MqttWebSocketConfig stressWebSocketConfig() {
        return MqttWebSocketConfig.builder().path("/mqtt").subprotocol("mqtt").build();
    }

    protected static Mqtt3ClientBuilder v3Builder(String clientId) {
        ClientStressTransport t = transport();
        Mqtt3ClientBuilder builder = MqttClient.builder().useMqttVersion3()
                .serverHost(brokerHost())
                .serverPort(brokerPort())
                .transportType(t.transportType())
                .identifier(clientId);
        applyTransport(builder, t);
        applyCredentials(builder);
        return builder;
    }

    protected static Mqtt5ClientBuilder v5Builder(String clientId) {
        ClientStressTransport t = transport();
        Mqtt5ClientBuilder builder = MqttClient.builder().useMqttVersion5()
                .serverHost(brokerHost())
                .serverPort(brokerPort())
                .transportType(t.transportType())
                .identifier(clientId)
                .cleanStart(true);
        applyTransport(builder, t);
        applyCredentials(builder);
        return builder;
    }

    protected static Mqtt3RxClient v3Rx(String clientId) {
        return v3Builder(clientId).buildRx();
    }

    protected static Mqtt3AsyncClient v3Async(String clientId) {
        return v3Builder(clientId).buildAsync();
    }

    protected static Mqtt5RxClient v5Rx(String clientId) {
        return v5Builder(clientId).buildRx();
    }

    protected static Mqtt5AsyncClient v5Async(String clientId) {
        return v5Builder(clientId).buildAsync();
    }

    private static void applyTransport(Mqtt3ClientBuilder builder, ClientStressTransport transport) {
        MqttClientConfig.TransportType type = transport.transportType();
        if (type == MqttClientConfig.TransportType.TLS || type == MqttClientConfig.TransportType.WSS) {
            builder.sslConfig(stressSslConfig());
        }
        if (type == MqttClientConfig.TransportType.WS || type == MqttClientConfig.TransportType.WSS) {
            builder.webSocketConfig(stressWebSocketConfig());
        }
    }

    private static void applyTransport(Mqtt5ClientBuilder builder, ClientStressTransport transport) {
        MqttClientConfig.TransportType type = transport.transportType();
        if (type == MqttClientConfig.TransportType.TLS || type == MqttClientConfig.TransportType.WSS) {
            builder.sslConfig(stressSslConfig());
        }
        if (type == MqttClientConfig.TransportType.WS || type == MqttClientConfig.TransportType.WSS) {
            builder.webSocketConfig(stressWebSocketConfig());
        }
    }

    private static void applyCredentials(Mqtt3ClientBuilder builder) {
        String username = ClientStressSupport.username();
        if (username != null) {
            builder.username(username);
        }
        byte[] password = ClientStressSupport.password();
        if (password != null) {
            builder.password(password);
        }
    }

    private static void applyCredentials(Mqtt5ClientBuilder builder) {
        String username = ClientStressSupport.username();
        if (username != null) {
            builder.username(username);
        }
        byte[] password = ClientStressSupport.password();
        if (password != null) {
            builder.password(password);
        }
    }

    protected static void disconnectQuietly(Mqtt3RxClient client) {
        if (client != null) {
            try {
                client.disconnect().block(TIMEOUT);
            } catch (Exception ignored) {
                // cleanup
            }
        }
    }

    protected static void disconnectQuietly(Mqtt5RxClient client) {
        if (client != null) {
            try {
                client.disconnect().block(TIMEOUT);
            } catch (Exception ignored) {
                // cleanup
            }
        }
    }

}
