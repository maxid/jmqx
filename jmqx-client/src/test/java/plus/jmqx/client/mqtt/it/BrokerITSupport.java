package plus.jmqx.client.mqtt.it;

import org.junit.jupiter.api.BeforeAll;
import plus.jmqx.client.mqtt.MqttClient;
import plus.jmqx.client.mqtt.MqttClientConfig;
import plus.jmqx.client.mqtt.internal.transport.MqttSslConfig;
import plus.jmqx.client.mqtt.internal.transport.MqttWebSocketConfig;
import plus.jmqx.client.mqtt.message.QoS;
import plus.jmqx.client.mqtt.v3.Mqtt3RxClient;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Publish;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Subscribe;
import plus.jmqx.client.mqtt.v3.message.Mqtt3TopicFilter;
import plus.jmqx.client.mqtt.v5.Mqtt5RxClient;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Publish;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Subscribe;
import plus.jmqx.client.mqtt.v5.message.Mqtt5TopicFilter;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import reactor.core.Disposable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * jmqx-client ↔ jmqx-broker 集成测试公共基类。
 */
public abstract class BrokerITSupport {

    protected static final Duration TIMEOUT = Duration.ofSeconds(10);

    @BeforeAll
    static void ensureBrokerReady() {
        EmbeddedBrokerHolder.ensureStarted();
    }

    protected static int brokerPort() {
        return EmbeddedBrokerHolder.port();
    }

    protected static int brokerSecurePort() {
        return EmbeddedBrokerHolder.securePort();
    }

    protected static int brokerWebsocketPort() {
        return EmbeddedBrokerHolder.websocketPort();
    }

    protected static int brokerWebsocketSecurePort() {
        return EmbeddedBrokerHolder.websocketSecurePort();
    }

    protected static String brokerHost() {
        return EmbeddedBrokerHolder.host();
    }

    protected static MqttSslConfig testSslConfig() {
        return MqttSslConfig.builder().insecureTrustAll(true).build();
    }

    protected static MqttWebSocketConfig testWebSocketConfig() {
        return MqttWebSocketConfig.builder().path("/mqtt").subprotocol("mqtt").build();
    }

    protected static String uniqueId(String prefix) {
        return prefix + "-" + System.nanoTime();
    }

    protected static String uniqueTopic(String prefix) {
        return prefix + "/" + System.nanoTime();
    }

    protected static Mqtt3RxClient v3Rx(String clientId) {
        return MqttClient.builder().useMqttVersion3()
                .serverHost(brokerHost()).serverPort(brokerPort())
                .identifier(clientId)
                .buildRx();
    }

    protected static Mqtt3RxClient v3Rx(String clientId, boolean cleanSession) {
        return MqttClient.builder().useMqttVersion3()
                .serverHost(brokerHost()).serverPort(brokerPort())
                .identifier(clientId)
                .cleanSession(cleanSession)
                .buildRx();
    }

    protected static Mqtt5RxClient v5Rx(String clientId) {
        return MqttClient.builder().useMqttVersion5()
                .serverHost(brokerHost()).serverPort(brokerPort())
                .identifier(clientId)
                .cleanStart(true)
                .buildRx();
    }

    protected static Mqtt3RxClient v3RxTls(String clientId) {
        return MqttClient.builder().useMqttVersion3()
                .serverHost(brokerHost()).serverPort(brokerSecurePort())
                .transportType(MqttClientConfig.TransportType.TLS)
                .sslConfig(testSslConfig())
                .identifier(clientId)
                .buildRx();
    }

    protected static Mqtt3RxClient v3RxWs(String clientId) {
        return MqttClient.builder().useMqttVersion3()
                .serverHost(brokerHost()).serverPort(brokerWebsocketPort())
                .transportType(MqttClientConfig.TransportType.WS)
                .webSocketConfig(testWebSocketConfig())
                .identifier(clientId)
                .buildRx();
    }

    protected static Mqtt3RxClient v3RxWss(String clientId) {
        return MqttClient.builder().useMqttVersion3()
                .serverHost(brokerHost()).serverPort(brokerWebsocketSecurePort())
                .transportType(MqttClientConfig.TransportType.WSS)
                .sslConfig(testSslConfig())
                .webSocketConfig(testWebSocketConfig())
                .identifier(clientId)
                .buildRx();
    }

    protected static Mqtt5RxClient v5RxTls(String clientId) {
        return MqttClient.builder().useMqttVersion5()
                .serverHost(brokerHost()).serverPort(brokerSecurePort())
                .transportType(MqttClientConfig.TransportType.TLS)
                .sslConfig(testSslConfig())
                .identifier(clientId)
                .cleanStart(true)
                .buildRx();
    }

    protected static Mqtt5RxClient v5RxWs(String clientId) {
        return MqttClient.builder().useMqttVersion5()
                .serverHost(brokerHost()).serverPort(brokerWebsocketPort())
                .transportType(MqttClientConfig.TransportType.WS)
                .webSocketConfig(testWebSocketConfig())
                .identifier(clientId)
                .cleanStart(true)
                .buildRx();
    }

    protected static Mqtt5RxClient v5RxWss(String clientId) {
        return MqttClient.builder().useMqttVersion5()
                .serverHost(brokerHost()).serverPort(brokerWebsocketSecurePort())
                .transportType(MqttClientConfig.TransportType.WSS)
                .sslConfig(testSslConfig())
                .webSocketConfig(testWebSocketConfig())
                .identifier(clientId)
                .cleanStart(true)
                .buildRx();
    }

    protected static Mqtt3Subscribe v3Sub(String filter, QoS qos) {
        return Mqtt3Subscribe.builder()
                .topicFilters(List.of(Mqtt3TopicFilter.builder().topicFilter(filter).qos(qos).build()))
                .build();
    }

    protected static Mqtt3Subscribe v3Sub(List<String> filters, QoS qos) {
        return Mqtt3Subscribe.builder()
                .topicFilters(filters.stream()
                        .map(f -> Mqtt3TopicFilter.builder().topicFilter(f).qos(qos).build())
                        .toList())
                .build();
    }

    protected static Mqtt3Publish v3Pub(String topic, String payload, QoS qos) {
        return Mqtt3Publish.builder()
                .topic(topic)
                .payload(payload.getBytes(StandardCharsets.UTF_8))
                .qos(qos)
                .build();
    }

    protected static Mqtt3Publish v3PubRetain(String topic, String payload, QoS qos) {
        return Mqtt3Publish.builder()
                .topic(topic)
                .payload(payload.getBytes(StandardCharsets.UTF_8))
                .qos(qos)
                .retain(true)
                .build();
    }

    protected static Mqtt5Subscribe v5Sub(String filter, QoS qos) {
        return Mqtt5Subscribe.builder()
                .topicFilters(List.of(Mqtt5TopicFilter.builder().topicFilter(filter).qos(qos).build()))
                .build();
    }

    protected static Mqtt5Publish v5Pub(String topic, String payload, QoS qos) {
        return Mqtt5Publish.builder()
                .topic(topic)
                .payload(payload.getBytes(StandardCharsets.UTF_8))
                .qos(qos)
                .build();
    }

    protected static void awaitV3(AtomicReference<Mqtt3Publish> ref) throws InterruptedException {
        awaitV3(ref, TIMEOUT);
    }

    protected static void awaitV3(AtomicReference<Mqtt3Publish> ref, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (ref.get() == null && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
    }

    protected static void awaitV5(AtomicReference<Mqtt5Publish> ref) throws InterruptedException {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (ref.get() == null && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
    }

    protected static void assertV3Payload(Mqtt3Publish publish, String topic, String payload) {
        assertNotNull(publish, "expected publish on " + topic);
        assertEquals(topic, publish.getTopic());
        assertEquals(payload, new String(publish.getPayloadAsBytes(), StandardCharsets.UTF_8));
    }

    protected static void assertV5Payload(Mqtt5Publish publish, String topic, String payload) {
        assertNotNull(publish, "expected publish on " + topic);
        assertEquals(topic, publish.getTopic());
        assertEquals(payload, new String(publish.getPayloadAsBytes(), StandardCharsets.UTF_8));
    }

    protected static void assertNoV3Within(AtomicReference<Mqtt3Publish> ref, Duration wait) throws InterruptedException {
        Mqtt3Publish before = ref.get();
        Thread.sleep(wait.toMillis());
        Mqtt3Publish after = ref.get();
        if (before == null) {
            assertNull(after, "unexpected publish received");
        } else {
            assertEquals(before, after, "unexpected additional publish received");
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

    protected static void assertConnAcceptedV3(Mqtt3RxClient client) {
        assertTrue(client.connect().block(TIMEOUT).getReturnCode().isAccepted());
    }

    protected static void assertConnAcceptedV5(Mqtt5RxClient client) {
        assertTrue(client.connect().block(TIMEOUT).isAccepted());
    }

    protected void v3PublishSubscribeSmoke(Mqtt3RxClient client) throws Exception {
        String topic = uniqueTopic("test/transport");
        assertConnAcceptedV3(client);
        AtomicReference<Mqtt3Publish> received = new AtomicReference<>();
        Disposable stream = client.subscribePublishes(v3Sub(topic, QoS.AT_LEAST_ONCE))
                .doOnNext(Mqtt3Publish::ack)
                .subscribe(received::set);
        client.subscribe(v3Sub(topic, QoS.AT_LEAST_ONCE)).block(TIMEOUT);
        client.publish(v3Pub(topic, "transport-ok", QoS.AT_LEAST_ONCE)).block(TIMEOUT);
        awaitV3(received);
        stream.dispose();
        assertV3Payload(received.get(), topic, "transport-ok");
    }

    protected void v5PublishSubscribeSmoke(Mqtt5RxClient client) throws Exception {
        String topic = uniqueTopic("test/transport");
        assertConnAcceptedV5(client);
        AtomicReference<Mqtt5Publish> received = new AtomicReference<>();
        Disposable stream = client.subscribePublishes(v5Sub(topic, QoS.AT_LEAST_ONCE))
                .doOnNext(Mqtt5Publish::ack)
                .subscribe(received::set);
        client.subscribe(v5Sub(topic, QoS.AT_LEAST_ONCE)).block(TIMEOUT);
        client.publish(v5Pub(topic, "transport-ok", QoS.AT_LEAST_ONCE)).block(TIMEOUT);
        awaitV5(received);
        stream.dispose();
        assertV5Payload(received.get(), topic, "transport-ok");
    }

}
