package plus.jmqx.client.mqtt.v5;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import plus.jmqx.client.mqtt.MqttClient;
import plus.jmqx.client.mqtt.message.QoS;
import plus.jmqx.client.mqtt.v5.message.Mqtt5ConnAck;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Publish;
import plus.jmqx.client.mqtt.v5.message.Mqtt5PublishProperties;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Subscribe;
import plus.jmqx.client.mqtt.v5.message.Mqtt5TopicFilter;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MQTT 5 客户端集成测试 —— 需要 jmqx-broker 在 localhost:1883 运行。
 */
class Mqtt5ClientIT {

    private static final Logger log = LoggerFactory.getLogger(Mqtt5ClientIT.class);

    @Test
    void connectPublishSubscribeDisconnectV5() throws Exception {
        Mqtt5RxClient client = MqttClient.builder().useMqttVersion5()
                .serverHost("localhost").serverPort(1883)
                .identifier("it-v5-" + System.nanoTime())
                .buildRx();

        Mqtt5ConnAck ack = client.connect().block(Duration.ofSeconds(5));
        assertNotNull(ack);
        assertTrue(ack.isAccepted());
        log.info("v5 connected, receiveMax={}", ack.getProperties().getReceiveMaximum());

        AtomicReference<Mqtt5Publish> received = new AtomicReference<>();
        Mqtt5Subscribe sub = Mqtt5Subscribe.builder()
                .topicFilters(List.of(Mqtt5TopicFilter.builder()
                        .topicFilter("v5/#").qos(QoS.AT_LEAST_ONCE).build()))
                .build();
        client.subscribePublishes(sub).doOnNext(received::set).subscribe();
        client.subscribe(sub).block(Duration.ofSeconds(5));

        client.publish(Mqtt5Publish.builder()
                        .topic("v5/hello")
                        .payload("world5".getBytes())
                        .qos(QoS.AT_LEAST_ONCE)
                        .properties(Mqtt5PublishProperties.builder().contentType("text/plain").build())
                        .build())
                .block(Duration.ofSeconds(5));

        long deadline = System.nanoTime() + 5_000_000_000L;
        while (received.get() == null && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        assertNotNull(received.get(), "did not receive v5 publish");
        assertEquals("v5/hello", received.get().getTopic());
        assertEquals("world5", new String(received.get().getPayloadAsBytes()));

        client.disconnect().block(Duration.ofSeconds(5));
    }
}
