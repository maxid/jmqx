package plus.jmqx.client.mqtt.v5;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import plus.jmqx.client.mqtt.MqttClient;
import plus.jmqx.client.mqtt.MqttClientConfig;
import plus.jmqx.client.mqtt.internal.transport.MqttWebSocketConfig;
import plus.jmqx.client.mqtt.message.QoS;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Publish;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Subscribe;
import plus.jmqx.client.mqtt.v5.message.Mqtt5TopicFilter;
import reactor.core.Disposable;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 第三方 MQTT Broker 集成测试（MQTT 5.0，不依赖内嵌 jmqx-broker）。
 *
 * <p>通过系统属性配置目标平台地址，方便对接 EMQX / Mosquitto / VerneMQ 等：
 *
 * <pre>{@code
 * # 默认值
 * -Dtest3rd.host=localhost
 * -Dtest3rd.tcp.port=1883
 * -Dtest3rd.ws.port=8083
 * -Dtest3rd.ws.path=/mqtt
 * }</pre>
 *
 * <p>显式运行（TCP + WebSocket）：
 * <pre>{@code
 * mvn -pl jmqx-client test -Dtest=Mqtt5ThirdPartyIT
 * }</pre>
 *
 * <p>只测 TCP：
 * <pre>{@code
 * mvn -pl jmqx-client test -Dtest=Mqtt5ThirdPartyIT#tcpConnectAndPublishSubscribe
 * }</pre>
 *
 * <p>连接本地 EMQX 时跳过 TLS 校验（如需要）：
 * <pre>{@code
 * mvn -pl jmqx-client test -Dtest=Mqtt5ThirdPartyIT -Dtest3rd.ws.port=8083
 * }</pre>
 */
@Slf4j
@Disabled("需手动启用并指定目标 broker 地址（-Dtest3rd.host=...）")
class Mqtt5ThirdPartyIT {

    /**
     * 连接超时。
     */
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    // ---- 系统属性配置 ----
    private static final String HOST     = System.getProperty("test3rd.host", "localhost");
    private static final int    TCP_PORT = Integer.getInteger("test3rd.tcp.port", 1883);
    private static final int    WS_PORT  = Integer.getInteger("test3rd.ws.port", 8083);
    private static final String WS_PATH  = System.getProperty("test3rd.ws.path", "/mqtt");

    private Mqtt5RxClient client;

    @AfterEach
    void cleanup() {
        if (client != null) {
            try {
                client.disconnect().block(TIMEOUT);
            } catch (Exception ignored) {
                // cleanup
            }
            client = null;
        }
    }

    private static MqttWebSocketConfig wsConfig() {
        return MqttWebSocketConfig.builder()
                .path(WS_PATH)
                .subprotocol("mqtt")
                .build();
    }

    private static String uniqueId(String prefix) {
        return prefix + "-" + System.nanoTime();
    }

    private static String uniqueTopic(String prefix) {
        return prefix + "/" + System.nanoTime();
    }

    // ---- 测试方法 ----

    @Test
    void tcpConnectAndPublishSubscribe() throws Exception {
        log.info("Connecting to {}:{} (TCP, v5)", HOST, TCP_PORT);
        client = MqttClient.builder().useMqttVersion5()
                .serverHost(HOST)
                .serverPort(TCP_PORT)
                .identifier(uniqueId("it-5th-tcp"))
                .cleanStart(true)
                .buildRx();

        assertConnAccepted();
        publishSubscribeSmoke();
    }

    @Test
    void wsConnectAndPublishSubscribe() throws Exception {
        log.info("Connecting to ws://{}:{}{} (WS, v5)", HOST, WS_PORT, WS_PATH);
        client = MqttClient.builder().useMqttVersion5()
                .serverHost(HOST)
                .serverPort(WS_PORT)
                .transportType(MqttClientConfig.TransportType.WS)
                .webSocketConfig(wsConfig())
                .identifier(uniqueId("it-5th-ws"))
                .cleanStart(true)
                .buildRx();

        assertConnAccepted();
        publishSubscribeSmoke();
    }

    // ---- 内部辅助 ----

    private void assertConnAccepted() {
        assertTrue(Objects.requireNonNull(client.connect().block(TIMEOUT)).isAccepted());
    }

    private void publishSubscribeSmoke() throws InterruptedException {
        String topic = uniqueTopic("test/5thparty");
        log.debug("Smoke test with topic={}", topic);

        AtomicReference<Mqtt5Publish> received = new AtomicReference<>();
        Disposable stream = client.subscribePublishes(v5Sub(topic, QoS.AT_LEAST_ONCE))
                .doOnNext(Mqtt5Publish::ack)
                .subscribe(received::set);

        client.subscribe(v5Sub(topic, QoS.AT_LEAST_ONCE)).block(TIMEOUT);
        client.publish(v5Pub(topic, "5thparty-ok", QoS.AT_LEAST_ONCE)).block(TIMEOUT);

        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (received.get() == null && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        stream.dispose();

        assertNotNull(received.get(), "expected publish on " + topic);
        assertEquals(topic, received.get().getTopic());
        assertEquals("5thparty-ok", new String(received.get().getPayloadAsBytes(), StandardCharsets.UTF_8));

        log.info("Smoke test passed for topic={}", topic);
    }

    private static Mqtt5Subscribe v5Sub(String filter, QoS qos) {
        return Mqtt5Subscribe.builder()
                .topicFilters(List.of(Mqtt5TopicFilter.builder().topicFilter(filter).qos(qos).build()))
                .build();
    }

    private static Mqtt5Publish v5Pub(String topic, String payload, QoS qos) {
        return Mqtt5Publish.builder()
                .topic(topic)
                .payload(payload.getBytes(StandardCharsets.UTF_8))
                .qos(qos)
                .build();
    }

}
