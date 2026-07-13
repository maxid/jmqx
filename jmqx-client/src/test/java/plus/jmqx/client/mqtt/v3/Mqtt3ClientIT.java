package plus.jmqx.client.mqtt.v3;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import plus.jmqx.client.mqtt.MqttClient;
import plus.jmqx.client.mqtt.it.BrokerITSupport;
import plus.jmqx.client.mqtt.message.QoS;
import plus.jmqx.client.mqtt.v3.message.Mqtt3ConnAck;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Publish;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Subscribe;
import plus.jmqx.client.mqtt.v3.message.Mqtt3Unsubscribe;
import reactor.core.Disposable;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MQTT 3.1.1 客户端 ↔ jmqx-broker 端到端集成测试。
 *
 * <p>默认内嵌 broker；对接外部 broker 时使用 {@code -Djmqx.it.broker.port=1883}。
 * 被 surefire 排除在默认 {@code mvn test} 之外，显式运行：{@code mvn -pl jmqx-client -Dtest=Mqtt3ClientIT test}
 */
class Mqtt3ClientIT extends BrokerITSupport {

    private Mqtt3RxClient client;

    @AfterEach
    void cleanup() {
        disconnectQuietly(client);
        client = null;
    }

    @Test
    void connectAndDisconnect() {
        client = v3Rx(uniqueId("it-v3-conn"));
        Mqtt3ConnAck ack = client.connect().block(TIMEOUT);
        assertNotNull(ack);
        assertTrue(ack.getReturnCode().isAccepted());
        client.disconnect().block(TIMEOUT);
    }

    @ParameterizedTest
    @EnumSource(QoS.class)
    void publishSubscribeAllQosLevels(QoS qos) throws Exception {
        String topic = uniqueTopic("test/qos");
        client = v3Rx(uniqueId("it-v3-qos"));
        assertConnAcceptedV3(client);

        Mqtt3Subscribe sub = v3Sub(topic, qos);
        AtomicReference<Mqtt3Publish> received = new AtomicReference<>();
        Disposable stream = client.subscribePublishes(sub)
                .doOnNext(p -> {
                    if (p.getQoS().value() > 0) {
                        p.ack();
                    }
                })
                .subscribe(received::set);
        client.subscribe(sub).block(TIMEOUT);

        client.publish(v3Pub(topic, "qos-" + qos.value(), qos)).block(TIMEOUT);
        awaitV3(received);
        stream.dispose();
        assertV3Payload(received.get(), topic, "qos-" + qos.value());
    }

    @Test
    void asyncApiPublishSubscribe() throws Exception {
        String topic = uniqueTopic("test/async");
        var async = MqttClient.builder().useMqttVersion3()
                .serverHost(brokerHost()).serverPort(brokerPort())
                .identifier(uniqueId("it-v3-async"))
                .buildAsync();

        AtomicReference<Mqtt3Publish> received = new AtomicReference<>();
        Mqtt3Subscribe sub = v3Sub(topic, QoS.AT_LEAST_ONCE);
        async.connect().get();
        async.subscribe(sub, p -> {
            p.ack();
            received.set(p);
        }).get();
        async.publish(v3Pub(topic, "async-payload", QoS.AT_LEAST_ONCE)).get();
        awaitV3(received);
        assertV3Payload(received.get(), topic, "async-payload");
        async.disconnect().get();
    }

    @Test
    void blockingApiPublishSubscribe() throws Exception {
        String topic = uniqueTopic("test/block");
        var blocking = MqttClient.builder().useMqttVersion3()
                .serverHost(brokerHost()).serverPort(brokerPort())
                .identifier(uniqueId("it-v3-block"))
                .buildBlocking();

        blocking.connect();
        blocking.subscribe(v3Sub(topic, QoS.AT_MOST_ONCE));
        blocking.publish(v3Pub(topic, "block-payload", QoS.AT_MOST_ONCE));

        client = blocking.toRx();
        AtomicReference<Mqtt3Publish> received = new AtomicReference<>();
        Disposable stream = client.subscribePublishes(v3Sub(topic, QoS.AT_MOST_ONCE))
                .subscribe(received::set);
        blocking.publish(v3Pub(topic, "block-payload-2", QoS.AT_MOST_ONCE));
        awaitV3(received);
        stream.dispose();
        assertV3Payload(received.get(), topic, "block-payload-2");
        blocking.disconnect();
        client = null;
    }

    @Test
    void wildcardHashSubscription() throws Exception {
        String base = uniqueTopic("sensor");
        String topic = base + "/temp";
        client = v3Rx(uniqueId("it-v3-hash"));
        assertConnAcceptedV3(client);

        AtomicReference<Mqtt3Publish> received = new AtomicReference<>();
        Mqtt3Subscribe sub = v3Sub(base + "/#", QoS.AT_MOST_ONCE);
        Disposable stream = client.subscribePublishes(sub).subscribe(received::set);
        client.subscribe(sub).block(TIMEOUT);
        client.publish(v3Pub(topic, "22.5", QoS.AT_MOST_ONCE)).block(TIMEOUT);
        awaitV3(received);
        stream.dispose();
        assertV3Payload(received.get(), topic, "22.5");
    }

    @Test
    void wildcardPlusSubscription() throws Exception {
        String base = uniqueTopic("home");
        String room = base + "/room";
        String topic = room + "/temp";
        client = v3Rx(uniqueId("it-v3-plus"));
        assertConnAcceptedV3(client);

        AtomicReference<Mqtt3Publish> received = new AtomicReference<>();
        Mqtt3Subscribe sub = v3Sub(base + "/+/temp", QoS.AT_MOST_ONCE);
        Disposable stream = client.subscribePublishes(sub).subscribe(received::set);
        client.subscribe(sub).block(TIMEOUT);
        client.publish(v3Pub(topic, "19.8", QoS.AT_MOST_ONCE)).block(TIMEOUT);
        awaitV3(received);
        stream.dispose();
        assertV3Payload(received.get(), topic, "19.8");
    }

    @Test
    void unsubscribeStopsDelivery() throws Exception {
        String topic = uniqueTopic("test/unsub");
        client = v3Rx(uniqueId("it-v3-unsub"));
        assertConnAcceptedV3(client);

        Mqtt3Subscribe sub = v3Sub(topic, QoS.AT_MOST_ONCE);
        AtomicReference<Mqtt3Publish> received = new AtomicReference<>();
        Disposable stream = client.subscribePublishes(sub).subscribe(received::set);
        client.subscribe(sub).block(TIMEOUT);
        client.publish(v3Pub(topic, "before", QoS.AT_MOST_ONCE)).block(TIMEOUT);
        awaitV3(received);
        assertV3Payload(received.get(), topic, "before");

        client.unsubscribe(Mqtt3Unsubscribe.builder().topicFilters(List.of(topic)).build()).block(TIMEOUT);
        received.set(null);
        client.publish(v3Pub(topic, "after", QoS.AT_MOST_ONCE)).block(TIMEOUT);
        assertNoV3Within(received, Duration.ofMillis(800));
        stream.dispose();
    }

    @Test
    void twoClientsPublisherAndSubscriber() throws Exception {
        String topic = uniqueTopic("test/peer");
        Mqtt3RxClient subscriber = v3Rx(uniqueId("it-v3-sub"));
        Mqtt3RxClient publisher = v3Rx(uniqueId("it-v3-pub"));
        client = subscriber;
        try {
            assertConnAcceptedV3(subscriber);
            assertConnAcceptedV3(publisher);

            AtomicReference<Mqtt3Publish> received = new AtomicReference<>();
            Mqtt3Subscribe sub = v3Sub(topic, QoS.AT_LEAST_ONCE);
            Disposable stream = subscriber.subscribePublishes(sub)
                    .doOnNext(Mqtt3Publish::ack)
                    .subscribe(received::set);
            subscriber.subscribe(sub).block(TIMEOUT);

            publisher.publish(v3Pub(topic, "peer-msg", QoS.AT_LEAST_ONCE)).block(TIMEOUT);
            awaitV3(received);
            stream.dispose();
            assertV3Payload(received.get(), topic, "peer-msg");
        } finally {
            disconnectQuietly(publisher);
        }
    }

    @Test
    void retainMessageDeliveredToLateSubscriber() throws Exception {
        String topic = uniqueTopic("test/retain");
        Mqtt3RxClient publisher = v3Rx(uniqueId("it-v3-ret-pub"));
        client = v3Rx(uniqueId("it-v3-ret-sub"));
        try {
            assertConnAcceptedV3(publisher);
            publisher.publish(v3PubRetain(topic, "retained", QoS.AT_LEAST_ONCE)).block(TIMEOUT);

            assertConnAcceptedV3(client);
            AtomicReference<Mqtt3Publish> received = new AtomicReference<>();
            Mqtt3Subscribe sub = v3Sub(topic, QoS.AT_MOST_ONCE);
            Disposable stream = client.subscribePublishes(sub).subscribe(received::set);
            client.subscribe(sub).block(TIMEOUT);
            awaitV3(received);
            stream.dispose();
            assertV3Payload(received.get(), topic, "retained");
        } finally {
            disconnectQuietly(publisher);
        }
    }

    @Test
    void sessionPersistenceDeliversOfflineMessage() throws Exception {
        String topic = uniqueTopic("test/session");
        String clientId = uniqueId("it-v3-persist");
        Mqtt3RxClient subscriber = v3Rx(clientId, false);
        Mqtt3RxClient publisher = v3Rx(uniqueId("it-v3-offline-pub"));
        client = subscriber;
        try {
            assertConnAcceptedV3(subscriber);
            Mqtt3Subscribe sub = v3Sub(topic, QoS.AT_LEAST_ONCE);
            subscriber.subscribe(sub).block(TIMEOUT);
            subscriber.disconnect().block(TIMEOUT);

            assertConnAcceptedV3(publisher);
            publisher.publish(v3Pub(topic, "offline", QoS.AT_LEAST_ONCE)).block(TIMEOUT);

            subscriber = v3Rx(clientId, false);
            client = subscriber;
            AtomicReference<Mqtt3Publish> received = new AtomicReference<>();
            Disposable stream = subscriber.subscribePublishes(sub)
                    .doOnNext(Mqtt3Publish::ack)
                    .subscribe(received::set);
            Mqtt3ConnAck ack = subscriber.connect().block(TIMEOUT);
            assertTrue(ack.getReturnCode().isAccepted());
            awaitV3(received);
            stream.dispose();
            assertV3Payload(received.get(), topic, "offline");
        } finally {
            disconnectQuietly(publisher);
        }
    }

    @Test
    void multipleTopicFiltersInOneSubscribe() throws Exception {
        String topicA = uniqueTopic("test/multi/a");
        String topicB = uniqueTopic("test/multi/b");
        client = v3Rx(uniqueId("it-v3-multi"));
        assertConnAcceptedV3(client);

        Mqtt3Subscribe sub = v3Sub(List.of(topicA, topicB), QoS.AT_MOST_ONCE);
        AtomicReference<Mqtt3Publish> received = new AtomicReference<>();
        Disposable stream = client.subscribePublishes(sub).subscribe(received::set);
        client.subscribe(sub).block(TIMEOUT);

        client.publish(v3Pub(topicB, "multi-b", QoS.AT_MOST_ONCE)).block(TIMEOUT);
        awaitV3(received);
        stream.dispose();
        assertV3Payload(received.get(), topicB, "multi-b");
    }

    @Test
    void emptyPayloadPublish() throws Exception {
        String topic = uniqueTopic("test/empty");
        client = v3Rx(uniqueId("it-v3-empty"));
        assertConnAcceptedV3(client);

        AtomicReference<Mqtt3Publish> received = new AtomicReference<>();
        Mqtt3Subscribe sub = v3Sub(topic, QoS.AT_MOST_ONCE);
        Disposable stream = client.subscribePublishes(sub).subscribe(received::set);
        client.subscribe(sub).block(TIMEOUT);
        client.publish(v3Pub(topic, "", QoS.AT_MOST_ONCE)).block(TIMEOUT);
        awaitV3(received);
        stream.dispose();
        assertNotNull(received.get());
        assertTrue(received.get().getPayloadAsBytes().length == 0);
    }

    @Test
    void concurrentPublishesFromAsyncClient() throws Exception {
        String topic = uniqueTopic("test/concurrent");
        client = v3Rx(uniqueId("it-v3-conc-rx"));
        var async = client.toAsync();
        assertConnAcceptedV3(client);

        AtomicReference<Mqtt3Publish> received = new AtomicReference<>();
        Mqtt3Subscribe sub = v3Sub(topic, QoS.AT_MOST_ONCE);
        Disposable stream = client.subscribePublishes(sub).subscribe(received::set);
        client.subscribe(sub).block(TIMEOUT);

        CompletableFuture<?>[] futures = new CompletableFuture[5];
        for (int i = 0; i < futures.length; i++) {
            final int n = i;
            futures[i] = async.publish(v3Pub(topic, "msg-" + n, QoS.AT_MOST_ONCE));
        }
        CompletableFuture.allOf(futures).get();
        awaitV3(received);
        stream.dispose();
        assertNotNull(received.get());
    }

    /**
     * MQTT v3.1.1 规范 §3.1.4：新客户端使用相同 clientId 连接时，
     * broker 必须断开旧客户端并接受新连接。
     * <p>验证：旧客户端无法继续通信 → 新客户端连接成功 → 新客户端正常收发消息。
     */
    @Test
    void duplicateClientIdKicksOldConnection() throws Exception {
        String clientId = uniqueId("it-v3-dup");

        // 第一个客户端 — 干净会话，无自动重连
        Mqtt3RxClient clientA = MqttClient.builder().useMqttVersion3()
                .serverHost(brokerHost()).serverPort(brokerPort())
                .identifier(clientId)
                .cleanSession(true)
                .buildRx();

        try {
            assertConnAcceptedV3(clientA);

            // client B 使用相同 clientId 连接——应为成功
            Mqtt3RxClient clientB = v3Rx(clientId);
            try {
                Mqtt3ConnAck ackB = clientB.connect().block(TIMEOUT);
                assertNotNull(ackB);
                assertTrue(ackB.getReturnCode().isAccepted(),
                        "client B with same clientId should be accepted");

                // 验证 client A 无法继续通信（被 broker 踢出）
                // 尝试在 client A 上发送消息，应因连接断开而失败
                String failTopic = uniqueTopic("test/dup-v3-fail");
                boolean publishFailed = false;
                try {
                    clientA.publish(v3Pub(failTopic, "should-fail", QoS.AT_MOST_ONCE)).block(TIMEOUT);
                } catch (Exception e) {
                    publishFailed = true;
                }
                assertTrue(publishFailed,
                        "client A publish must fail after being kicked");

                // 验证 client B 可正常收发消息
                String topic = uniqueTopic("test/dup-v3");
                AtomicReference<Mqtt3Publish> received = new AtomicReference<>();
                Mqtt3Subscribe sub = v3Sub(topic, QoS.AT_LEAST_ONCE);
                Disposable stream = clientB.subscribePublishes(sub)
                        .doOnNext(Mqtt3Publish::ack)
                        .subscribe(received::set);
                clientB.subscribe(sub).block(TIMEOUT);
                clientB.publish(v3Pub(topic, "dup-hello", QoS.AT_LEAST_ONCE)).block(TIMEOUT);
                awaitV3(received);
                stream.dispose();
                assertV3Payload(received.get(), topic, "dup-hello");

                clientB.disconnect().block(TIMEOUT);
            } finally {
                disconnectQuietly(clientB);
            }
        } finally {
            disconnectQuietly(clientA);
        }
    }

}
