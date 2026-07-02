package plus.jmqx.client.mqtt.v3;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import plus.jmqx.client.mqtt.MqttClient;
import plus.jmqx.client.mqtt.message.QoS;
import plus.jmqx.client.mqtt.v3.message.Mqtt3ConnAck;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Publish;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Subscribe;
import plus.jmqx.client.mqtt.v3.message.Mqtt3TopicFilter;
import reactor.core.Disposable;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * v3 客户端对接 jmqx-broker 的端到端集成测试。
 *
 * <p>需要 jmqx-broker 在 {@code localhost:1883} 运行。集成测试以 IT 后缀命名，
 * 被 surefire 排除在默认 {@code mvn test} 之外；用 {@code -Dtest=Mqtt3ClientIT} 显式运行。
 *
 * @author maxid
 */
class Mqtt3ClientIT {

    private static final Logger   log     = LoggerFactory.getLogger(Mqtt3ClientIT.class);
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private Mqtt3RxClient client;

    @AfterEach
    void cleanup() {
        if (client != null) {
            try {
                client.disconnect().block(TIMEOUT);
            } catch (Exception e) {
                log.warn("cleanup disconnect failed: {}", e.toString());
            }
        }
    }

    @Test
    void connectSubscribePublishReceiveDisconnect() throws Exception {
        String topic = "test/it/" + System.nanoTime();
        client = MqttClient.builder().useMqttVersion3()
                .serverHost("localhost").serverPort(1883)
                .identifier("it-v3-" + System.nanoTime())
                .buildRx();

        Mqtt3ConnAck ack = client.connect().block(TIMEOUT);
        assertNotNull(ack, "CONNACK not received");
        assertTrue(ack.getReturnCode().isAccepted(), "connect refused: " + ack.getReturnCode());
        log.info("connected, sessionPresent={}", ack.isSessionPresent());

        Mqtt3Subscribe sub = Mqtt3Subscribe.builder()
                .topicFilters(java.util.List.of(
                        Mqtt3TopicFilter.builder().topicFilter(topic).qos(QoS.AT_LEAST_ONCE).build()))
                .build();

        AtomicReference<Mqtt3Publish> received = new AtomicReference<>();
        Disposable stream = client.subscribePublishes(sub)
                .doOnNext(p -> {
                    log.info("received: {}", p.getTopic());
                    p.ack();
                })
                .subscribe(received::set);

        client.subscribe(sub).block(TIMEOUT);
        log.info("subscribed to {}", topic);

        client.publish(Mqtt3Publish.builder()
                        .topic(topic)
                        .payload("world".getBytes())
                        .qos(QoS.AT_LEAST_ONCE)
                        .build())
                .block(TIMEOUT);
        log.info("published to {}", topic);

        long deadline = System.nanoTime() + 5_000_000_000L;
        while (received.get() == null && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        stream.dispose();
        assertNotNull(received.get(), "did not receive published message");
        assertEquals(topic, received.get().getTopic());
        assertEquals("world", new String(received.get().getPayloadAsBytes()));
    }

    @Test
    void qos0PublishDelivered() throws Exception {
        String topic = "test/it0/" + System.nanoTime();
        client = MqttClient.builder().useMqttVersion3()
                .serverHost("localhost").serverPort(1883)
                .identifier("it-v3q0-" + System.nanoTime())
                .buildRx();
        assertTrue(client.connect().block(TIMEOUT).getReturnCode().isAccepted());

        Mqtt3Subscribe sub = Mqtt3Subscribe.builder()
                .topicFilters(java.util.List.of(
                        Mqtt3TopicFilter.builder().topicFilter(topic).qos(QoS.AT_MOST_ONCE).build()))
                .build();

        AtomicReference<Mqtt3Publish> received = new AtomicReference<>();
        Disposable stream = client.subscribePublishes(sub).subscribe(received::set);
        client.subscribe(sub).block(TIMEOUT);

        client.publish(Mqtt3Publish.builder()
                        .topic(topic)
                        .payload("fire".getBytes())
                        .qos(QoS.AT_MOST_ONCE)
                        .build())
                .block(TIMEOUT);

        long deadline = System.nanoTime() + 5_000_000_000L;
        while (received.get() == null && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        stream.dispose();
        assertNotNull(received.get(), "did not receive QoS0 message");
        assertEquals("fire", new String(received.get().getPayloadAsBytes()));
    }
}
