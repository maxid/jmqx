package plus.jmqx.client.mqtt.stress;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import plus.jmqx.client.mqtt.MqttClient;
import plus.jmqx.client.mqtt.v5.Mqtt5AsyncClient;
import plus.jmqx.client.mqtt.v5.Mqtt5RxClient;
import plus.jmqx.client.mqtt.v5.message.Mqtt5Subscribe;
import plus.jmqx.client.mqtt.v5.message.Mqtt5TopicFilter;
import reactor.core.Disposable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MQTT 5.0 jmqx-client 压力测试（三类独立场景）。
 *
 * <p>用法同 {@link Mqtt3ClientStressTest}，将类名替换为 {@code Mqtt5ClientStressTest} 即可。
 */
@EnabledIfSystemProperty(named = "jmqx.stress.tests", matches = "true")
class Mqtt5ClientStressTest extends ClientStressBrokerSupport {

    static {
        ClientStressLogSupport.configure();
    }

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(30);

    private static ClientStressConfig cfg;

    @BeforeAll
    static void init() {
        cfg = ClientStressSupport.loadConfig();
    }

    @Test
    @EnabledIf("plus.jmqx.client.mqtt.stress.StressScenarioFilter#connectEnabled")
    void connectStress() throws Exception {
        ClientStressConfig c = cfg;
        ConnectStressRunner.ConnectStats stats = ConnectStressRunner.run("v5", c, clientId -> {
            Mqtt5RxClient client = newClient(clientId);
            client.connect().block(CONNECT_TIMEOUT);
            return () -> disconnectQuietly(client);
        });
        assertTrue(stats.completedInTime(), "connect stress timed out");
        assertTrue(stats.established() >= c.connections * 0.95,
                "too many connection failures: established=" + stats.established() + "/" + c.connections);
        assertEquals(stats.established(), stats.completed(),
                "not all established connections were cleanly disconnected");
    }

    @Test
    @EnabledIf("plus.jmqx.client.mqtt.stress.StressScenarioFilter#publishEnabled")
    void publishStress() throws Exception {
        ClientStressConfig c = cfg;
        byte[] payload = ClientStressSupport.randomPayload(c.payloadBytes);
        String topic = c.topic + "/v5/publish/" + System.nanoTime();
        int workers = Math.max(c.threads, c.publishers);
        int base = c.messages / workers;
        int remainder = c.messages % workers;
        AckAwareStressPublisher.PublishProgress progress = new AckAwareStressPublisher.PublishProgress();
        CountDownLatch latch = new CountDownLatch(workers);
        ExecutorService pool = Executors.newFixedThreadPool(workers);

        long start = System.nanoTime();
        try (StressProgressReporter progressReporter = StressProgressReporter.start(
                "v5-publish", c.progressIntervalSeconds, start,
                () -> ClientStressSupport.formatPublishProgress(
                        c.messages, progress.sent.get(), progress.acked.get(), progress.failed.get(), start))) {
            for (int i = 0; i < workers; i++) {
                final int idx = i;
                final int messagesPerWorker = base + (idx < remainder ? 1 : 0);
                pool.submit(() -> {
                    Mqtt5AsyncClient client = null;
                    try {
                        client = MqttClient.builder().useMqttVersion5()
                                .serverHost(brokerHost()).serverPort(brokerPort())
                                .identifier("stress-v5-pub-" + idx + "-" + System.nanoTime())
                                .cleanStart(true)
                                .buildAsync();
                        client.connect().get(c.timeoutSeconds, TimeUnit.SECONDS);
                        AckAwareStressPublisher.publishV5(
                                client, topic, payload, c.qos, messagesPerWorker, c.inflight, c.timeoutSeconds, progress);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        if (client != null) {
                            try {
                                client.disconnect().get(10, TimeUnit.SECONDS);
                            } catch (Exception ignored) {
                                // cleanup
                            }
                        }
                        latch.countDown();
                    }
                });
            }
            latch.await(c.timeoutSeconds, TimeUnit.SECONDS);
        }
        pool.shutdownNow();
        long end = System.nanoTime();
        long acked = progress.acked.get();
        long failed = progress.failed.get();
        ClientStressSupport.logPublishStress("v5", c, acked, failed, start, end, latch.getCount() == 0);
        assertTrue(latch.getCount() == 0, "publish stress did not finish");
        assertEquals(c.messages, acked,
                "not all messages acked by broker (acked=" + acked + ", failed=" + failed + ")");
        assertEquals(0, failed, "publish ack failures detected");
        assertTrue(ClientStressSupport.throughput(acked, start, end) >= c.minThroughputMsgPerSec,
                "publish throughput below minimum " + c.minThroughputMsgPerSec + " msg/s");
    }

    @Test
    @EnabledIf("plus.jmqx.client.mqtt.stress.StressScenarioFilter#subscribeEnabled")
    void subscribeStress() throws Exception {
        ClientStressConfig c = cfg;
        byte[] payload = ClientStressSupport.randomPayload(c.payloadBytes);
        String topic = c.topic + "/v5/subscribe/" + System.nanoTime();
        long expected = (long) c.subscribers * c.messages;
        CountDownLatch latch = new CountDownLatch((int) Math.min(expected, Integer.MAX_VALUE));
        AtomicLong received = new AtomicLong();
        List<Mqtt5RxClient> subscribers = new ArrayList<>();
        List<Disposable> streams = new ArrayList<>();

        try {
            for (int i = 0; i < c.subscribers; i++) {
                Mqtt5RxClient sub = newClient("stress-v5-sub-" + i + "-" + System.nanoTime());
                subscribers.add(sub);
                sub.connect().block(CONNECT_TIMEOUT);
                Mqtt5Subscribe subscribe = Mqtt5Subscribe.builder()
                        .topicFilters(List.of(Mqtt5TopicFilter.builder().topicFilter(topic).qos(c.qos).build()))
                        .build();
                streams.add(sub.subscribePublishes(subscribe)
                        .doOnNext(p -> {
                            if (p.getQoS().value() > 0) {
                                p.ack();
                            }
                            received.incrementAndGet();
                            latch.countDown();
                        })
                        .subscribe());
                sub.subscribe(subscribe).block(CONNECT_TIMEOUT);
            }

            long start = System.nanoTime();
            boolean ok;
            try (StressProgressReporter progressReporter = StressProgressReporter.start(
                    "v5-subscribe", c.progressIntervalSeconds, start,
                    () -> ClientStressSupport.formatSubscribeProgress(expected, received.get(), start))) {
                publishAll(topic, payload, c);
                ok = latch.await(c.timeoutSeconds, TimeUnit.SECONDS);
            }
            long end = System.nanoTime();
            ClientStressSupport.logSubscribeStress("v5", c, received.get(), start, end, ok);
            assertTrue(ok, "subscribe stress did not finish within timeout");
            assertEquals(expected, received.get(), "not all messages received by subscribers");
            assertTrue(ClientStressSupport.throughput(received.get(), start, end) >= c.minThroughputMsgPerSec,
                    "subscribe throughput below minimum " + c.minThroughputMsgPerSec + " msg/s");
        } finally {
            streams.forEach(Disposable::dispose);
            subscribers.forEach(ClientStressBrokerSupport::disconnectQuietly);
        }
    }

    private static void publishAll(String topic, byte[] payload, ClientStressConfig c) throws Exception {
        List<Mqtt5AsyncClient> publishers = new ArrayList<>();
        try {
            for (int i = 0; i < c.publishers; i++) {
                Mqtt5AsyncClient pub = MqttClient.builder().useMqttVersion5()
                        .serverHost(brokerHost()).serverPort(brokerPort())
                        .identifier("stress-v5-feed-" + i + "-" + System.nanoTime())
                        .cleanStart(true)
                        .buildAsync();
                pub.connect().get(c.timeoutSeconds, TimeUnit.SECONDS);
                publishers.add(pub);
            }
            int base = c.messages / publishers.size();
            int remainder = c.messages % publishers.size();
            long acked = 0;
            long failed = 0;
            for (int i = 0; i < publishers.size(); i++) {
                int count = base + (i < remainder ? 1 : 0);
                AckAwareStressPublisher.PublishStats stats = AckAwareStressPublisher.publishV5(
                        publishers.get(i), topic, payload, c.qos, count, c.inflight, c.timeoutSeconds);
                acked += stats.acked();
                failed += stats.failed();
            }
            assertEquals(c.messages, acked, "feed publisher did not ack all messages");
            assertEquals(0, failed, "feed publisher ack failures");
        } finally {
            for (Mqtt5AsyncClient pub : publishers) {
                try {
                    pub.disconnect().get(10, TimeUnit.SECONDS);
                } catch (Exception ignored) {
                    // cleanup
                }
            }
        }
    }

    private static Mqtt5RxClient newClient(String clientId) {
        return v5Rx(clientId);
    }

}
