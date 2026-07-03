package plus.jmqx.client.mqtt.stress;

import org.junit.jupiter.api.BeforeAll;
import plus.jmqx.client.mqtt.MqttClient;
import plus.jmqx.client.mqtt.v3.Mqtt3RxClient;
import plus.jmqx.client.mqtt.v5.Mqtt5RxClient;

import java.time.Duration;

/**
 * 压力测试公共基类：连接用户自备的外部 MQTT broker，不启动内嵌 jmqx-broker。
 *
 * <p>运行前须自行启动 broker（如 jmqx-broker、EMQX 等），默认连接 {@code localhost:1883}。
 * 可通过 {@code -Djmqx.client.stress.broker.host=...} 与
 * {@code -Djmqx.client.stress.broker.port=...} 覆盖（亦兼容 {@code jmqx.it.broker.*}）。
 */
public abstract class ClientStressBrokerSupport {

    protected static final Duration TIMEOUT = Duration.ofSeconds(10);

    @BeforeAll
    static void verifyExternalBroker() {
        String host = brokerHost();
        int port = brokerPort();
        Mqtt3RxClient probe = MqttClient.builder().useMqttVersion3()
                .serverHost(host)
                .serverPort(port)
                .identifier("stress-probe-" + System.nanoTime())
                .buildRx();
        try {
            var ack = probe.connect().block(TIMEOUT);
            if (ack == null || !ack.getReturnCode().isAccepted()) {
                throw new IllegalStateException("broker at " + host + ":" + port + " rejected connection");
            }
            probe.disconnect().block(TIMEOUT);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "MQTT broker not reachable at " + host + ":" + port
                            + ". Start a broker before stress tests (e.g. jmqx-broker on port 1883). "
                            + "Override with -Djmqx.client.stress.broker.host=... "
                            + "-Djmqx.client.stress.broker.port=...",
                    e);
        }
    }

    protected static String brokerHost() {
        return ClientStressSupport.brokerHost();
    }

    protected static int brokerPort() {
        return ClientStressSupport.brokerPort();
    }

    protected static Mqtt3RxClient v3Rx(String clientId) {
        return MqttClient.builder().useMqttVersion3()
                .serverHost(brokerHost())
                .serverPort(brokerPort())
                .identifier(clientId)
                .buildRx();
    }

    protected static Mqtt5RxClient v5Rx(String clientId) {
        return MqttClient.builder().useMqttVersion5()
                .serverHost(brokerHost())
                .serverPort(brokerPort())
                .identifier(clientId)
                .cleanStart(true)
                .buildRx();
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
