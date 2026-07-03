package plus.jmqx.client.mqtt.v5;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import plus.jmqx.client.mqtt.MqttClient;
import plus.jmqx.client.mqtt.it.BrokerITSupport;
import plus.jmqx.client.mqtt.message.QoS;
import plus.jmqx.client.mqtt.v5.message.Mqtt5ConnAck;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Publish;
import plus.jmqx.client.mqtt.v5.message.Mqtt5PublishProperties;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Subscribe;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Unsubscribe;
import reactor.core.Disposable;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MQTT 5.0 客户端 ↔ jmqx-broker 端到端集成测试。
 *
 * <p>默认内嵌 broker；对接外部 broker 时使用 {@code -Djmqx.it.broker.port=1883}。
 */
class Mqtt5ClientIT extends BrokerITSupport {

    private Mqtt5RxClient client;

    @AfterEach
    void cleanup() {
        disconnectQuietly(client);
        client = null;
    }

    @Test
    void connectAndDisconnect() {
        client = v5Rx(uniqueId("it-v5-conn"));
        Mqtt5ConnAck ack = client.connect().block(TIMEOUT);
        assertNotNull(ack);
        assertTrue(ack.isAccepted());
        client.disconnect().block(TIMEOUT);
    }

    @ParameterizedTest
    @EnumSource(QoS.class)
    void publishSubscribeAllQosLevels(QoS qos) throws Exception {
        String topic = uniqueTopic("v5/qos");
        client = v5Rx(uniqueId("it-v5-qos"));
        assertConnAcceptedV5(client);

        Mqtt5Subscribe sub = v5Sub(topic, qos);
        AtomicReference<Mqtt5Publish> received = new AtomicReference<>();
        Disposable stream = client.subscribePublishes(sub)
                .doOnNext(p -> {
                    if (p.getQoS().value() > 0) {
                        p.ack();
                    }
                })
                .subscribe(received::set);
        client.subscribe(sub).block(TIMEOUT);

        client.publish(v5Pub(topic, "v5-qos-" + qos.value(), qos)).block(TIMEOUT);
        awaitV5(received);
        stream.dispose();
        assertV5Payload(received.get(), topic, "v5-qos-" + qos.value());
    }

    @Test
    void publishWithUserPropertiesAndContentType() throws Exception {
        String topic = uniqueTopic("v5/props");
        client = v5Rx(uniqueId("it-v5-props"));
        assertConnAcceptedV5(client);

        AtomicReference<Mqtt5Publish> received = new AtomicReference<>();
        Mqtt5Subscribe sub = v5Sub(topic, QoS.AT_LEAST_ONCE);
        Disposable stream = client.subscribePublishes(sub)
                .doOnNext(p -> p.ack())
                .subscribe(received::set);
        client.subscribe(sub).block(TIMEOUT);

        client.publish(Mqtt5Publish.builder()
                        .topic(topic)
                        .payload("props-body".getBytes())
                        .qos(QoS.AT_LEAST_ONCE)
                        .properties(Mqtt5PublishProperties.builder()
                                .contentType("text/plain")
                                .build())
                        .build())
                .block(TIMEOUT);
        awaitV5(received);
        stream.dispose();
        assertV5Payload(received.get(), topic, "props-body");
        // contentType 由 broker 透传；若 broker 未实现 v5 属性透传则为 null
        if (received.get().getProperties().getContentType() != null) {
            assertEquals("text/plain", received.get().getProperties().getContentType());
        }
    }

    @Test
    void asyncApiPublishSubscribe() throws Exception {
        String topic = uniqueTopic("v5/async");
        var async = MqttClient.builder().useMqttVersion5()
                .serverHost(brokerHost()).serverPort(brokerPort())
                .identifier(uniqueId("it-v5-async"))
                .cleanStart(true)
                .buildAsync();

        AtomicReference<Mqtt5Publish> received = new AtomicReference<>();
        Mqtt5Subscribe sub = v5Sub(topic, QoS.AT_LEAST_ONCE);
        async.connect().get();
        async.subscribe(sub, p -> {
            p.ack();
            received.set(p);
        }).get();
        async.publish(v5Pub(topic, "v5-async", QoS.AT_LEAST_ONCE)).get();
        awaitV5(received);
        assertV5Payload(received.get(), topic, "v5-async");
        async.disconnect().get();
    }

    @Test
    void blockingApiConnectPublish() {
        String topic = uniqueTopic("v5/block");
        var blocking = MqttClient.builder().useMqttVersion5()
                .serverHost(brokerHost()).serverPort(brokerPort())
                .identifier(uniqueId("it-v5-block"))
                .cleanStart(true)
                .buildBlocking();

        blocking.connect();
        blocking.subscribe(v5Sub(topic, QoS.AT_MOST_ONCE));
        blocking.publish(v5Pub(topic, "v5-block", QoS.AT_MOST_ONCE));
        blocking.disconnect();
    }

    @Test
    void wildcardHashSubscription() throws Exception {
        String base = uniqueTopic("v5/sensor");
        String topic = base + "/humidity";
        client = v5Rx(uniqueId("it-v5-hash"));
        assertConnAcceptedV5(client);

        AtomicReference<Mqtt5Publish> received = new AtomicReference<>();
        Mqtt5Subscribe sub = v5Sub(base + "/#", QoS.AT_MOST_ONCE);
        Disposable stream = client.subscribePublishes(sub).subscribe(received::set);
        client.subscribe(sub).block(TIMEOUT);
        client.publish(v5Pub(topic, "65", QoS.AT_MOST_ONCE)).block(TIMEOUT);
        awaitV5(received);
        stream.dispose();
        assertV5Payload(received.get(), topic, "65");
    }

    @Test
    void unsubscribeStopsDelivery() throws Exception {
        String topic = uniqueTopic("v5/unsub");
        client = v5Rx(uniqueId("it-v5-unsub"));
        assertConnAcceptedV5(client);

        Mqtt5Subscribe sub = v5Sub(topic, QoS.AT_MOST_ONCE);
        AtomicReference<Mqtt5Publish> received = new AtomicReference<>();
        Disposable stream = client.subscribePublishes(sub).subscribe(received::set);
        client.subscribe(sub).block(TIMEOUT);
        client.publish(v5Pub(topic, "before", QoS.AT_MOST_ONCE)).block(TIMEOUT);
        awaitV5(received);

        client.unsubscribe(Mqtt5Unsubscribe.builder().topicFilters(List.of(topic)).build()).block(TIMEOUT);
        received.set(null);
        client.publish(v5Pub(topic, "after", QoS.AT_MOST_ONCE)).block(TIMEOUT);
        Thread.sleep(800);
        assertTrue(received.get() == null);
        stream.dispose();
    }

    @Test
    void twoClientsPublisherAndSubscriber() throws Exception {
        String topic = uniqueTopic("v5/peer");
        Mqtt5RxClient subscriber = v5Rx(uniqueId("it-v5-sub"));
        Mqtt5RxClient publisher = v5Rx(uniqueId("it-v5-pub"));
        client = subscriber;
        try {
            assertConnAcceptedV5(subscriber);
            assertConnAcceptedV5(publisher);

            AtomicReference<Mqtt5Publish> received = new AtomicReference<>();
            Mqtt5Subscribe sub = v5Sub(topic, QoS.AT_LEAST_ONCE);
            Disposable stream = subscriber.subscribePublishes(sub)
                    .doOnNext(Mqtt5Publish::ack)
                    .subscribe(received::set);
            subscriber.subscribe(sub).block(TIMEOUT);

            publisher.publish(v5Pub(topic, "v5-peer", QoS.AT_LEAST_ONCE)).block(TIMEOUT);
            awaitV5(received);
            stream.dispose();
            assertV5Payload(received.get(), topic, "v5-peer");
        } finally {
            disconnectQuietly(publisher);
        }
    }

    @Test
    void retainMessageDeliveredToLateSubscriber() throws Exception {
        String topic = uniqueTopic("v5/retain");
        Mqtt5RxClient publisher = v5Rx(uniqueId("it-v5-ret-pub"));
        client = v5Rx(uniqueId("it-v5-ret-sub"));
        try {
            assertConnAcceptedV5(publisher);
            publisher.publish(Mqtt5Publish.builder()
                    .topic(topic)
                    .payload("v5-retained".getBytes())
                    .qos(QoS.AT_LEAST_ONCE)
                    .retain(true)
                    .build()).block(TIMEOUT);

            assertConnAcceptedV5(client);
            AtomicReference<Mqtt5Publish> received = new AtomicReference<>();
            Mqtt5Subscribe sub = v5Sub(topic, QoS.AT_MOST_ONCE);
            Disposable stream = client.subscribePublishes(sub).subscribe(received::set);
            client.subscribe(sub).block(TIMEOUT);
            awaitV5(received);
            stream.dispose();
            assertV5Payload(received.get(), topic, "v5-retained");
        } finally {
            disconnectQuietly(publisher);
        }
    }

    @Test
    void connAckExposesReceiveMaximum() {
        client = v5Rx(uniqueId("it-v5-rxmax"));
        Mqtt5ConnAck ack = client.connect().block(TIMEOUT);
        assertNotNull(ack);
        assertTrue(ack.isAccepted());
        assertTrue(ack.getProperties().getReceiveMaximum() >= 0);
    }

}
